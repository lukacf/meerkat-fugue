use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::agent::{WorkerMessage, run_agent_worker};
use crate::config::{
    MaterializedMobTemplate, MobExecutionBackend, NativeMcpServerConfig, ServiceConfig,
    TrackerConfig, WorkflowConfigManager, is_native_linear_mcp_server,
};
use crate::deploy_gate::DeployStartRequest;
use crate::domain::{
    AgentUpdate, Issue, LiveSession, RecentEvent, TokenTotals, WorkerExit, WorkerOutcome,
    is_todo_state, normalize_state, sanitize_workspace_key,
};
use crate::error::{FugueError, Result};
#[cfg(test)]
use crate::final_pr_gate::{
    FinalPrGateCompletionInput, FinalPrGateCompletionPlan, FinalPrGateRuntimePlan,
    final_pr_gate_completion_plan,
};
use crate::final_pr_gate::{
    FinalPrGateProjectionPlan, FinalPrGateRuntimeTriggerInput, FinalPrGateStartRequest,
    final_pr_gate_start_request,
};
use crate::github_tools::GitHubToolHost;
use crate::independent_review::IndependentReviewStartRequest;
use crate::kapellmeister::{
    CursorPartition, DurableCursorKey, DurableCursorPosition, DurableCursorSource,
    KapellmeisterAuthorityMode, KapellmeisterDecisionContext, KapellmeisterDecisionResponse,
    KapellmeisterHostDecision, KapellmeisterHostObservation, KapellmeisterRepoScope,
    KapellmeisterRunningMobObservation, KapellmeisterShadowHost, KapellmeisterShadowInspection,
    KapellmeisterShadowSignal, KapellmeisterToolAuthority, KapellmeisterWorkspaceEvidence,
    PrHandoffMonitorDecision, RkatKapellmeisterShadowRuntime, ShadowDecisionId, ShadowSignalId,
    ShadowWorkerFailure, ShadowWorkerFailureReason, ShadowWorkerOutcome, inspect_shadow_state,
};
use crate::kapellmeister_host::{
    KapellmeisterHostToolExecutor, execute_guarded_kapellmeister_decision,
    independent_review_native_launch_template,
};
use crate::linear_adapter::{
    IssueAdapter, IssueLaneClassification, IssueLaneRuntimeObservation,
    active_projection_phase_labels_for_template, classify_issue_lane, evaluate_issue_projection,
    issue_lane_state_path_for_workflow, lane_state_path_env_name,
    projected_terminal_lane_for_template_status, projection_lane_for_template_node,
    reconcile_issue_lane_observation_state, record_internal_lane_for_workflow,
    terminal_states_env_name,
};
use crate::linear_tools::{LINEAR_ACTION_STATES_ENV, LINEAR_REVIEW_HANDOFF_STATES_ENV};
use crate::meta_runtime::{
    MetaRuntimeNativeMobExecutionHost, MetaRuntimeRegistry, meta_runtime_state_path,
};
use crate::mob_event_observer::{
    NativeMobEventObserver, merge_mob_event_snapshots, native_mob_workpad_input_with_existing,
    native_mob_workpad_should_upsert,
};
#[cfg(test)]
use crate::mob_tools::InMemoryNativeMobExecutionHost;
use crate::mob_tools::{
    NativeMobExecutionHost, NativeMobLaunchPlan, native_template_step_timeout_floor_ms,
};
use crate::native_mob_contract::{NativeMobE2eEvidence, inspect_native_mob_e2e_contract};
use crate::native_reviewer_gate::{ReviewerGateObservation, latest_reviewer_gate_observation};
use crate::tool_layer::{FugueToolLayer, clean_workspace_for_issue_identifier_best_effort};
use crate::tools::{
    BranchName, CancelExecutionInput, CheckCiOutput, CiStatus, ClaimIssueInput, CreatePrInput,
    Cursor, ExistingPrMatch, FugueToolError, FugueToolErrorCode, FugueToolOperation,
    FugueToolOutput, FugueToolResult, GitHubPullRequestNumber, GitHubPullRequestRef,
    GitHubPullRequestSnapshot, GitHubRepoRef, GitSha, IdempotencyKey, InspectRepoInput,
    LinearIssueIdentifier, LinearIssueRef, LinearStateRef, ListRunsInput, MergePrInput,
    MobEventSnapshot, MobRunId, MobRunSnapshot, MobRunStatus, MoveIssueInput, MoveIssueOutput,
    PullRequestState, RetryAdvice, SpawnOrResumeExecutionInput, ToolExecutionMetadata,
    UpsertWorkpadInput, WorkspaceKey,
};
use crate::workgraph_gate::{
    WORKGRAPH_BLOCKED_STATES_ENV, WORKGRAPH_STATE_PATH_ENV, workgraph_blocked_states_env_value,
};
use crate::workspace::WorkspaceManager;

pub type SharedState = Arc<RwLock<OrchestratorState>>;

const KAPELLMEISTER_SHADOW_RECONCILE_TIMEOUT_MS: u64 = 10_000;
const KAPELLMEISTER_REPO_SIGNAL_PENDING_LOG_MS: u64 = 15_000;
const NATIVE_STALL_TIMEOUT_STEP_GRACE_MS: i64 = 60_000;
const KAPELLMEISTER_SIGNAL_DEDUP_LIMIT: usize = 2048;

pub struct Orchestrator {
    config: WorkflowConfigManager,
    state: SharedState,
    refresh_rx: mpsc::Receiver<()>,
    internal_tx: mpsc::Sender<InternalMessage>,
    internal_rx: mpsc::Receiver<InternalMessage>,
    native_execution_host: Arc<dyn NativeMobExecutionHost + Send + Sync>,
    meta_runtime_registry: MetaRuntimeRegistry,
    native_event_observer: NativeMobEventObserver,
    native_worker_execution: bool,
    terminal_issue_refresh: bool,
    kapellmeister_signal_deduper: Arc<Mutex<RecentSignalDeduper>>,
    #[cfg(test)]
    captured_native_signal_messages: Option<Mutex<Vec<String>>>,
    linear_backoff_until: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub enum InternalMessage {
    Worker(Box<WorkerMessage>),
    RetryDue {
        issue_id: String,
    },
    KapellmeisterStartIssue {
        issue_identifier: String,
        response: oneshot::Sender<KapellmeisterStartIssueResult>,
    },
    KapellmeisterStartFinalPrGate {
        request: FinalPrGateStartRequest,
        response: oneshot::Sender<KapellmeisterStartIssueResult>,
    },
    KapellmeisterStartIndependentReview {
        request: IndependentReviewStartRequest,
        response: oneshot::Sender<KapellmeisterStartIssueResult>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct KapellmeisterStartIssueResult {
    pub issue_identifier: String,
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug)]
pub struct OrchestratorState {
    pub poll_interval_ms: u64,
    pub max_concurrent_agents: usize,
    running: HashMap<String, RunningEntry>,
    claimed: HashSet<String>,
    retry_attempts: HashMap<String, RetryEntry>,
    completed: HashSet<String>,
    pub meerkat_totals: TokenTotals,
    pub ended_runtime_seconds: f64,
    pub meerkat_rate_limits: Option<Value>,
    pub last_error: Option<String>,
    pub recent_events: HashMap<String, Vec<RecentEvent>>,
}

#[derive(Debug)]
struct RunningEntry {
    issue: Issue,
    identifier: String,
    policy_mode: IssuePolicyMode,
    workspace_path: String,
    session: LiveSession,
    started_at: DateTime<Utc>,
    retry_attempt: Option<u32>,
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
    termination: Option<TerminationPlan>,
    native_run: Option<NativeRunHandle>,
}

#[derive(Debug, Clone)]
struct NativeRunHandle {
    run_id: MobRunId,
    observed_events: Vec<MobEventSnapshot>,
    recovery: NativeStatusRecovery,
}

#[derive(Debug)]
struct RecentSignalDeduper {
    seen: HashSet<ShadowSignalId>,
    order: VecDeque<ShadowSignalId>,
    limit: usize,
}

impl RecentSignalDeduper {
    fn new(limit: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            limit,
        }
    }

    fn accept(&mut self, id: &ShadowSignalId) -> bool {
        if self.seen.contains(id) {
            return false;
        }
        self.seen.insert(id.clone());
        self.order.push_back(id.clone());
        while self.order.len() > self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        true
    }
}

#[derive(Debug, Clone, Default)]
struct NativeStatusRecovery {
    workspace: Option<RecoveredWorkspaceEvidence>,
    linear: Option<RecoveredLinearEvidence>,
    github: Option<RecoveredGitHubEvidence>,
    review: Option<NativeStatusReviewEvidence>,
}

#[derive(Debug, Clone)]
struct RecoveredWorkspaceEvidence {
    path: String,
    branch: Option<BranchName>,
    head_sha: Option<GitSha>,
    upstream_branch: Option<BranchName>,
    upstream_head_sha: Option<GitSha>,
    dirty: bool,
}

#[derive(Debug, Clone)]
struct RecoveredLinearEvidence {
    issue_state: String,
    branch_name: Option<BranchName>,
    workpad_native_run_id: Option<MobRunId>,
    workpad_mentions_pr: bool,
    workpad_pr_number: Option<GitHubPullRequestNumber>,
    unresolved_review_finding: Option<String>,
    reviewer_rework_requested: bool,
}

#[derive(Debug, Clone)]
struct RecoveredGitHubEvidence {
    pull_request: GitHubPullRequestSnapshot,
    rejected_pull_requests: Vec<RejectedPullRequestEvidence>,
}

#[derive(Debug, Clone)]
struct RejectedPullRequestEvidence {
    number: GitHubPullRequestNumber,
    reason: String,
}

#[derive(Debug, Clone)]
struct DetectedNativePrHandoff {
    issue: Issue,
    run_id: MobRunId,
    pull_request: GitHubPullRequestSnapshot,
    workpad_has_pr: bool,
}

#[derive(Debug, Default)]
struct NativePrHandoffReconcileResult {
    changed: bool,
    monitored_issue_ids: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
struct NativeDurableEvidence {
    run: Option<MobRunSnapshot>,
    linear: Option<RecoveredLinearEvidence>,
    github: Option<RecoveredGitHubEvidence>,
    workspace: Option<RecoveredWorkspaceEvidence>,
    events: Vec<MobEventSnapshot>,
}

#[derive(Debug, Clone)]
struct NativeStatusReviewEvidence {
    reason: String,
}

trait NativeStatusGitHubInspector: Send + Sync {
    fn inspect_pr(&self, input: crate::tools::InspectPrInput) -> FugueToolResult;
    fn check_pr_ci(&self, pull_request: &GitHubPullRequestSnapshot) -> FugueToolResult {
        GitHubToolHost::new().check_pr_ci(pull_request)
    }
}

impl NativeStatusGitHubInspector for GitHubToolHost {
    fn inspect_pr(&self, input: crate::tools::InspectPrInput) -> FugueToolResult {
        GitHubToolHost::inspect_pr(self, input)
    }

    fn check_pr_ci(&self, pull_request: &GitHubPullRequestSnapshot) -> FugueToolResult {
        GitHubToolHost::check_pr_ci(self, pull_request)
    }
}

#[derive(Debug)]
struct RetryEntry {
    identifier: String,
    policy_mode: IssuePolicyMode,
    workspace_path: String,
    attempt: u32,
    native_run_generation: Option<u32>,
    due_at: DateTime<Utc>,
    error: Option<String>,
    handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssuePolicyMode {
    Kapellmeister,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HumanLaneAction {
    Continue,
    StopNoRetry,
    StopAndCleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminationAuthority {
    HostRuntime,
    LinearTerminalLane,
    LinearNonterminalLane,
}

impl TerminationAuthority {
    fn permits_kapellmeister_termination(self) -> bool {
        matches!(
            self,
            TerminationAuthority::HostRuntime | TerminationAuthority::LinearTerminalLane
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchBackend {
    NativeMob,
}

#[derive(Debug, Clone)]
struct TerminationPlan {
    cleanup_workspace: bool,
    retry: bool,
    reason: String,
}

enum RunningTerminationTarget {
    NativeWorker {
        identifier: String,
        session_id: String,
        run_id: MobRunId,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    },
    Native {
        identifier: String,
        session_id: String,
        run_id: MobRunId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeRunTerminalAction {
    Complete,
    Retry,
    Replan,
    CreatePrHandoff,
    HumanReview,
    Park(NativeRunParkReason),
    KeepRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeRunParkReason {
    ResourceExhausted,
    HostMechanics,
    Stalled,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "retain the typed application contract without incidental boxing"
)]
enum NativeLaunchRefreshAction {
    Continue(Issue),
    Stop { reason: String },
}

struct OrchestratorFinalPrGateStartExecutor {
    native_execution_host: Arc<dyn NativeMobExecutionHost + Send + Sync>,
    config: ServiceConfig,
    issue: Issue,
}

#[async_trait]
impl KapellmeisterHostToolExecutor for OrchestratorFinalPrGateStartExecutor {
    async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::LinearClaimIssue,
            input.idempotency_key,
        )
    }

    async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult {
        unsupported_final_gate_operation(FugueToolOperation::LinearMoveIssue, input.idempotency_key)
    }

    async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::LinearUpsertWorkpad,
            input.idempotency_key,
        )
    }

    async fn spawn_or_resume_execution(
        &mut self,
        input: SpawnOrResumeExecutionInput,
    ) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::MobSpawnOrResumeExecution,
            input.idempotency_key,
        )
    }

    async fn spawn_or_resume_final_pr_gate(
        &mut self,
        request: FinalPrGateStartRequest,
    ) -> FugueToolResult {
        let mcp_tools = agent_visible_mcp_tool_names(&self.config.mob.mcp_servers);
        let template = crate::kapellmeister_host::final_pr_gate_native_launch_template_with_profile(
            &request,
            &mcp_tools,
            &self.config.independent_review.orchestrator_profile,
        );
        let repository = Some(request.pr.repo.clone());
        let plan = match NativeMobLaunchPlan::new(
            request.spawn_input,
            template.clone(),
            native_member_mcp_servers_for_issue(
                &self.config.mob.mcp_servers,
                &self.config.workflow_dir,
                &template,
                &self.issue,
                repository.as_ref(),
                &self.config.tracker.terminal_states,
                &native_issue_review_handoff_states(&self.config, &self.issue),
                &native_action_lanes(&self.config),
            ),
            native_member_direct_tool_servers(&self.config.tracker, &template),
            self.config.tracker.project_slug.clone(),
            repository,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                return FugueToolResult::Err {
                    error,
                    metadata: ToolExecutionMetadata::default(),
                };
            }
        };
        let native_execution_host = Arc::clone(&self.native_execution_host);
        match tokio::task::spawn_blocking(move || native_execution_host.spawn_or_resume_plan(plan))
            .await
        {
            Ok(result) => result,
            Err(error) => FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::MobSpawnOrResumeExecution,
                    code: FugueToolErrorCode::BackendUnavailable,
                    message: format!("final PR gate launch plan task failed: {error}"),
                    retry: RetryAdvice::SafeToRetry,
                    idempotency_key: None,
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }

    async fn spawn_or_resume_independent_review(
        &mut self,
        request: IndependentReviewStartRequest,
    ) -> FugueToolResult {
        let mcp_tools = agent_visible_mcp_tool_names(&self.config.mob.mcp_servers);
        let template = independent_review_native_launch_template(&request, &mcp_tools);
        let repository = Some(request.pull_request.repo.clone());
        let plan = match NativeMobLaunchPlan::new(
            request.spawn_input,
            template.clone(),
            native_member_mcp_servers_for_issue(
                &self.config.mob.mcp_servers,
                &self.config.workflow_dir,
                &template,
                &self.issue,
                repository.as_ref(),
                &self.config.tracker.terminal_states,
                &native_issue_review_handoff_states(&self.config, &self.issue),
                &native_action_lanes(&self.config),
            ),
            native_member_direct_tool_servers(&self.config.tracker, &template),
            self.config.tracker.project_slug.clone(),
            repository,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                return FugueToolResult::Err {
                    error,
                    metadata: ToolExecutionMetadata::default(),
                };
            }
        };
        let native_execution_host = Arc::clone(&self.native_execution_host);
        match tokio::task::spawn_blocking(move || native_execution_host.spawn_or_resume_plan(plan))
            .await
        {
            Ok(result) => result,
            Err(error) => FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::MobSpawnOrResumeExecution,
                    code: FugueToolErrorCode::BackendUnavailable,
                    message: format!("independent review launch plan task failed: {error}"),
                    retry: RetryAdvice::SafeToRetry,
                    idempotency_key: None,
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }

    async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult {
        unsupported_final_gate_operation(FugueToolOperation::GitHubMergePr, input.idempotency_key)
    }
}

fn unsupported_final_gate_operation(
    operation: FugueToolOperation,
    idempotency_key: IdempotencyKey,
) -> FugueToolResult {
    FugueToolResult::Err {
        error: FugueToolError {
            operation,
            code: FugueToolErrorCode::UnsupportedOperation,
            message: "orchestrator final gate executor only supports Kapellmeister-owned final PR gate start/resume".to_string(),
            retry: RetryAdvice::DoNotRetry,
            idempotency_key: Some(idempotency_key),
        },
        metadata: ToolExecutionMetadata::default(),
    }
}

pub struct FinalPrGateProjectionExecutor {
    config: ServiceConfig,
    issue: LinearIssueRef,
    registry: MetaRuntimeRegistry,
}

#[async_trait]
impl KapellmeisterHostToolExecutor for FinalPrGateProjectionExecutor {
    async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::LinearClaimIssue,
            input.idempotency_key,
        )
    }

    async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult {
        let idempotency_key = input.idempotency_key.clone();
        let gate = crate::workgraph_gate::WorkGraphTerminalGate::new(
            meta_runtime_state_path(&self.config.meerkat).join("workgraph.sqlite3"),
            self.config.tracker.terminal_states.clone(),
            native_workgraph_blocked_states(&self.config),
        )
        .for_mob(self.config.meerkat.mob_id().into_owned());
        if let Err(message) = gate.guard_terminal_move(&self.config.tracker, &input).await {
            return FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::LinearMoveIssue,
                    code: FugueToolErrorCode::InvalidInput,
                    message,
                    retry: RetryAdvice::DoNotRetry,
                    idempotency_key: Some(idempotency_key),
                },
                metadata: ToolExecutionMetadata::default(),
            };
        }
        match IssueAdapter::from_config(&self.config.tracker) {
            Ok(adapter) => match adapter.move_issue(input).await {
                Ok(output) => FugueToolResult::Ok {
                    output: FugueToolOutput::LinearMoveIssue(output),
                    metadata: ToolExecutionMetadata::default(),
                },
                Err(error) => FugueToolResult::Err {
                    error: FugueToolError {
                        operation: FugueToolOperation::LinearMoveIssue,
                        code: FugueToolErrorCode::ExecutionFailed,
                        message: error.to_string(),
                        retry: RetryAdvice::DoNotRetry,
                        idempotency_key: Some(idempotency_key),
                    },
                    metadata: ToolExecutionMetadata::default(),
                },
            },
            Err(error) => FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::LinearMoveIssue,
                    code: FugueToolErrorCode::BackendUnavailable,
                    message: error.to_string(),
                    retry: RetryAdvice::SafeToRetry,
                    idempotency_key: Some(input.idempotency_key),
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }

    async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult {
        match IssueAdapter::from_config(&self.config.tracker) {
            Ok(adapter) => adapter.upsert_workpad_tool_result(input).await,
            Err(error) => FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::LinearUpsertWorkpad,
                    code: FugueToolErrorCode::BackendUnavailable,
                    message: error.to_string(),
                    retry: RetryAdvice::SafeToRetry,
                    idempotency_key: Some(input.idempotency_key),
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }

    async fn spawn_or_resume_execution(
        &mut self,
        input: SpawnOrResumeExecutionInput,
    ) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::MobSpawnOrResumeExecution,
            input.idempotency_key,
        )
    }

    async fn spawn_or_resume_final_pr_gate(
        &mut self,
        request: FinalPrGateStartRequest,
    ) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::MobSpawnOrResumeExecution,
            request.spawn_input.idempotency_key,
        )
    }

    async fn spawn_or_resume_independent_review(
        &mut self,
        request: IndependentReviewStartRequest,
    ) -> FugueToolResult {
        unsupported_final_gate_operation(
            FugueToolOperation::MobSpawnOrResumeExecution,
            request.spawn_input.idempotency_key,
        )
    }

    async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult {
        crate::merge_gate::merge(
            &self.config,
            &self.registry,
            self.issue.clone(),
            input,
            crate::merge_gate::MergePurpose::FinalReview,
        )
        .await
    }
}

pub async fn execute_final_pr_gate_projection_with_executor(
    authority: &crate::kapellmeister::KapellmeisterToolAuthority,
    context: &KapellmeisterDecisionContext,
    projection: FinalPrGateProjectionPlan,
    executor: &mut impl KapellmeisterHostToolExecutor,
) -> Result<()> {
    let workpad_response = KapellmeisterDecisionResponse {
        request_id: ShadowDecisionId(format!(
            "final-pr-gate-projection:{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        )),
        decided_at: Utc::now(),
        decision: KapellmeisterHostDecision::WorkpadUpdate {
            input: projection.workpad,
        },
        rationale: Some("Kapellmeister final PR gate projection".to_string()),
    };
    let outcome =
        execute_guarded_kapellmeister_decision(authority, context, workpad_response, executor)
            .await?;
    if let Some(FugueToolResult::Err { error, .. }) = outcome.result {
        return Err(FugueError::ResponseError(error.message));
    }
    if let Some(input) = projection.merge {
        let response = KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId(format!(
                "final-pr-gate-merge:{}",
                input.idempotency_key.0
            )),
            decided_at: Utc::now(),
            decision: KapellmeisterHostDecision::Merge { input },
            rationale: Some("Kapellmeister final PR gate merge".to_string()),
        };
        let outcome =
            execute_guarded_kapellmeister_decision(authority, context, response, executor).await?;
        if let Some(FugueToolResult::Err { error, .. }) = outcome.result {
            return Err(FugueError::ResponseError(error.message));
        }
    }
    let move_response = KapellmeisterDecisionResponse {
        request_id: ShadowDecisionId(format!(
            "final-pr-gate-projection:{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        )),
        decided_at: Utc::now(),
        decision: KapellmeisterHostDecision::Move {
            input: projection.move_issue,
        },
        rationale: Some("Kapellmeister final PR gate projection".to_string()),
    };
    let outcome =
        execute_guarded_kapellmeister_decision(authority, context, move_response, executor).await?;
    if let Some(FugueToolResult::Err { error, .. }) = outcome.result {
        return Err(FugueError::ResponseError(error.message));
    }
    Ok(())
}

#[cfg(test)]
fn final_pr_gate_native_launch_template(
    request: &FinalPrGateStartRequest,
    mcp_tools: &[String],
) -> MaterializedMobTemplate {
    crate::kapellmeister_host::final_pr_gate_native_launch_template_with_profile(
        request,
        mcp_tools,
        &crate::config::IndependentReviewConfig::default().orchestrator_profile,
    )
}

fn agent_visible_mcp_tool_names(
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<String> {
    mcp_servers
        .keys()
        .filter(|server| server.as_str() != "fugue_host")
        .map(|server| format!("mcp:{server}"))
        .collect()
}

impl Orchestrator {
    pub fn new(config: WorkflowConfigManager, refresh_rx: mpsc::Receiver<()>) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel(256);
        let effective = config.effective();
        let meerkat_config = effective.meerkat.clone();
        let fugue_host_api_url = fugue_host_api_url_from_config(effective);
        let fugue_host_api_auth = fugue_host_api_auth_from_config(effective);
        let meta_runtime_registry = MetaRuntimeRegistry::with_config(
            meerkat_config,
            effective.tracker.clone(),
            fugue_host_api_url.clone(),
            fugue_host_api_auth,
            native_repo_kapellmeister_mcp_servers(
                &effective.mob.mcp_servers,
                &effective.workflow_dir,
                repository_scope_from_config(effective).as_ref(),
                effective.tracker.project_slug.as_deref(),
                &effective.tracker.terminal_states,
                &native_repo_review_handoff_states(effective),
                &native_action_lanes(effective),
                &native_workgraph_blocked_states(effective),
                &meta_runtime_state_path(&effective.meerkat).join("workgraph.sqlite3"),
                fugue_host_api_url.as_deref(),
            ),
            effective.mob.templates.clone(),
        )
        .with_final_review_config(effective.final_review.clone())
        .with_independent_review_config(effective.independent_review.clone());
        let native_event_observer = NativeMobEventObserver::for_config(effective);
        let state = Arc::new(RwLock::new(OrchestratorState {
            poll_interval_ms: effective.polling.interval_ms,
            max_concurrent_agents: effective.agent.max_concurrent_agents,
            running: HashMap::new(),
            claimed: HashSet::new(),
            retry_attempts: HashMap::new(),
            completed: HashSet::new(),
            meerkat_totals: TokenTotals::default(),
            ended_runtime_seconds: 0.0,
            meerkat_rate_limits: None,
            last_error: None,
            recent_events: HashMap::new(),
        }));
        Self {
            config,
            state,
            refresh_rx,
            internal_tx,
            internal_rx,
            native_execution_host: Arc::new(MetaRuntimeNativeMobExecutionHost::from_registry(
                meta_runtime_registry.clone(),
            )),
            meta_runtime_registry,
            native_event_observer,
            native_worker_execution: false,
            terminal_issue_refresh: true,
            kapellmeister_signal_deduper: Arc::new(Mutex::new(RecentSignalDeduper::new(
                KAPELLMEISTER_SIGNAL_DEDUP_LIMIT,
            ))),
            #[cfg(test)]
            captured_native_signal_messages: None,
            linear_backoff_until: None,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn new_with_native_execution_host(
        config: WorkflowConfigManager,
        refresh_rx: mpsc::Receiver<()>,
        native_execution_host: Arc<dyn NativeMobExecutionHost + Send + Sync>,
    ) -> Self {
        let mut orchestrator = Self::new(config, refresh_rx);
        orchestrator.native_execution_host = native_execution_host;
        orchestrator.native_worker_execution = false;
        // A fake worker host must not bootstrap a real coordinator as a side
        // effect. Real signal delivery is covered by the native runtime tests.
        orchestrator.captured_native_signal_messages = Some(Mutex::new(Vec::new()));
        orchestrator
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn new_with_native_execution_host_and_observer(
        config: WorkflowConfigManager,
        refresh_rx: mpsc::Receiver<()>,
        native_execution_host: Arc<dyn NativeMobExecutionHost + Send + Sync>,
        native_event_observer: NativeMobEventObserver,
    ) -> Self {
        let mut orchestrator =
            Self::new_with_native_execution_host(config, refresh_rx, native_execution_host);
        orchestrator.native_event_observer = native_event_observer;
        orchestrator
    }

    pub fn shared_state(&self) -> SharedState {
        self.state.clone()
    }

    pub fn config(&self) -> &WorkflowConfigManager {
        &self.config
    }

    pub fn meta_runtime_registry(&self) -> MetaRuntimeRegistry {
        self.meta_runtime_registry.clone()
    }

    pub fn internal_sender(&self) -> mpsc::Sender<InternalMessage> {
        self.internal_tx.clone()
    }

    pub async fn bootstrap_autonomous_hosts(&self) -> anyhow::Result<()> {
        if native_kapellmeister_prompt_control_enabled(self.config.effective()) {
            self.meta_runtime_registry
                .bootstrap_repo_kapellmeister()
                .await
                .map_err(|error| {
                    anyhow::anyhow!("bootstrap repo Kapellmeister Meta runtime: {error}")
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn without_native_worker_execution(mut self) -> Self {
        self.native_worker_execution = false;
        self.terminal_issue_refresh = false;
        self
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.run_until_shutdown(CancellationToken::new()).await
    }

    pub async fn run_until_shutdown(&mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.spawn_autonomous_host_bootstrap();
        self.startup_terminal_workspace_cleanup().await;
        self.reconcile_kapellmeister_shadow_host().await;
        self.tick().await;

        loop {
            let interval_ms = self.config.effective().polling.interval_ms;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {
                    self.tick().await;
                }
                Some(_) = self.refresh_rx.recv() => {
                    self.tick().await;
                }
                Some(message) = self.internal_rx.recv() => {
                    self.handle_internal(message).await;
                }
                _ = shutdown.cancelled() => {
                    self.shutdown_runtime("service_shutdown").await;
                    break;
                }
                else => break,
            }
        }
        Ok(())
    }

    async fn shutdown_runtime(&mut self, reason: &str) {
        let retry_handles = {
            let mut state = self.state.write().await;
            let retry_entries = state.retry_attempts.drain().collect::<Vec<_>>();
            for (issue_id, _) in &retry_entries {
                state.claimed.remove(issue_id);
            }
            let retry_handles = retry_entries
                .into_iter()
                .map(|(_, entry)| entry.handle)
                .collect::<Vec<_>>();
            for issue_id in state.running.keys().cloned().collect::<Vec<_>>() {
                state.claimed.remove(&issue_id);
            }
            retry_handles
        };
        for handle in retry_handles {
            handle.abort();
        }
        let running_ids = {
            let state = self.state.read().await;
            state.running.keys().cloned().collect::<Vec<_>>()
        };
        for issue_id in running_ids {
            // Embedded Meta members are durable. Runtime shutdown quiesces them;
            // retiring them here would destroy the identities needed on restart.
            if self
                .state
                .read()
                .await
                .running
                .get(&issue_id)
                .is_some_and(|entry| entry.native_run.is_some() && entry.handle.is_none())
            {
                continue;
            }
            self.terminate_running_issue(
                &issue_id,
                false,
                false,
                reason,
                TerminationAuthority::HostRuntime,
            )
            .await;
        }
        if let Err(error) = self.meta_runtime_registry.shutdown().await {
            warn!(%error, "Meta shutdown incomplete");
        }
    }

    fn spawn_autonomous_host_bootstrap(&self) {
        if !native_kapellmeister_prompt_control_enabled(self.config.effective()) {
            return;
        }
        let registry = self.meta_runtime_registry.clone();
        tokio::spawn(async move {
            if let Err(error) = registry.bootstrap_repo_kapellmeister().await {
                warn!(%error, "repo Kapellmeister bootstrap failed");
            }
        });
    }

    async fn tick(&mut self) {
        if self.linear_backoff_is_active() {
            debug!("dispatch skipped while Linear rate-limit backoff is active");
            return;
        }
        let dispatch_blocked = self.reload_workflow_if_needed().await;

        self.reconcile_running_issues().await;

        if dispatch_blocked {
            warn!("dispatch skipped because workflow reload is currently invalid");
            return;
        }

        if let Err(err) = self.config.validate_dispatch() {
            let mut state = self.state.write().await;
            state.last_error = Some(err.to_string());
            warn!(error = %err, "dispatch validation failed");
            return;
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                let mut state = self.state.write().await;
                state.last_error = Some(err.to_string());
                warn!(error = %err, "issue adapter configuration failed");
                return;
            }
        };
        if let Err(err) = self.ensure_linear_projection_lanes(&adapter).await {
            self.note_linear_error(&err);
            let mut state = self.state.write().await;
            state.last_error = Some(err.to_string());
            warn!(error = %err, "Linear projection lane preflight failed");
            return;
        }
        let mut candidates = match adapter.fetch_candidate_issues().await {
            Ok(issues) => issues,
            Err(err) => {
                self.note_linear_error(&err);
                let mut state = self.state.write().await;
                state.last_error = Some(err.to_string());
                warn!(error = %err, "candidate fetch failed");
                return;
            }
        };
        let pr_handoff_candidates = match adapter.fetch_open_project_issues().await {
            Ok(mut issues) => {
                merge_candidate_issues(&mut issues, candidates.iter().cloned());
                issues
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "native PR handoff reconciliation fell back to configured active lanes"
                );
                candidates.clone()
            }
        };
        {
            let mut state = self.state.write().await;
            state.last_error = None;
        }
        self.reconcile_linear_lane_observations(&pr_handoff_candidates)
            .await;
        if native_kapellmeister_prompt_control_enabled(self.config.effective()) {
            self.reconcile_native_deploy_lane(&adapter, &pr_handoff_candidates)
                .await;
            self.reconcile_native_kapellmeister_pr_attention_handoffs(
                &adapter,
                &pr_handoff_candidates,
            )
            .await;
            return;
        }
        let pr_handoff_reconciliation = self
            .reconcile_native_pr_handoffs(&adapter, &pr_handoff_candidates)
            .await;
        if pr_handoff_reconciliation.changed {
            candidates = match adapter.fetch_candidate_issues().await {
                Ok(issues) => issues,
                Err(err) => {
                    let mut state = self.state.write().await;
                    state.last_error = Some(err.to_string());
                    warn!(error = %err, "candidate refresh after native PR handoff reconciliation failed");
                    return;
                }
            };
        }
        sort_for_dispatch(&mut candidates);
        for issue in candidates {
            let policy_mode = classify_issue_policy_mode(&issue, self.config.effective());
            if kapellmeister_observation_suppresses_dispatch(
                policy_mode,
                dispatch_backend_for_issue(&issue, self.config.effective()),
            ) {
                continue;
            }
            if pr_handoff_reconciliation
                .monitored_issue_ids
                .contains(&issue.id)
            {
                continue;
            }
            if self.global_available_slots().await == 0 {
                break;
            }
            if !self.has_available_slot_for(&issue, false).await {
                continue;
            }
            if self.should_dispatch(&issue, false).await {
                self.dispatch_issue(issue, None, false).await;
            }
        }
    }

    fn linear_backoff_is_active(&self) -> bool {
        self.linear_backoff_until
            .is_some_and(|until| Utc::now() < until)
    }

    fn note_linear_error(&mut self, error: &FugueError) {
        if !linear_error_is_rate_limited(error) {
            return;
        }
        let until = Utc::now() + chrono::Duration::seconds(60);
        self.linear_backoff_until = Some(until);
        warn!(%until, "Linear rate limit observed; pausing Linear polling");
    }

    async fn reconcile_native_pr_handoffs(
        &self,
        adapter: &IssueAdapter,
        issues: &[Issue],
    ) -> NativePrHandoffReconcileResult {
        let github_host = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        let mut result = NativePrHandoffReconcileResult::default();
        for issue in issues {
            if matches!(
                classify_issue_policy_mode(issue, self.config.effective()),
                IssuePolicyMode::Kapellmeister
            ) {
                continue;
            }
            if !native_pr_handoff_monitor_candidate(self.config.effective(), issue) {
                continue;
            }
            let claimed_without_running = {
                let state = self.state.read().await;
                state.claimed.contains(&issue.id) && !state.running.contains_key(&issue.id)
            };
            if claimed_without_running {
                continue;
            }
            let issue_ref = issue_ref_for_identifier(&issue.identifier);
            let workpad_body = match adapter.workpad_body(&issue_ref, "## Fugue Workpad").await {
                Ok(body) => body.unwrap_or_else(|| "## Fugue Workpad".to_string()),
                Err(err) => {
                    warn!(
                        issue_identifier = %issue.identifier,
                        message = %err,
                        "native PR handoff monitor could not fetch Workpad"
                    );
                    continue;
                }
            };
            let linear = recover_native_linear_evidence(issue, Some(&workpad_body));
            if linear.reviewer_rework_requested {
                continue;
            }
            let Some(handoff) = recover_existing_native_pr_monitor_handoff_for_issue(
                self.config.effective(),
                issue,
                Some(&linear),
                &github_host,
            ) else {
                if native_pr_handoff_missing_durable_pr_evidence(
                    self.config.effective(),
                    issue,
                    &linear,
                ) {
                    result.monitored_issue_ids.insert(issue.id.clone());
                    self.deliver_kapellmeister_shadow_signal(native_pr_handoff_missing_pr_signal(
                        &issue.identifier,
                        Utc::now(),
                    ));
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "native PR handoff monitor reported {} to Kapellmeister because Workpad/Linear claims PR handoff but durable GitHub PR evidence is missing; Linear lane was not moved by the host",
                        issue.identifier
                    ));
                }
                continue;
            };
            let ci = if handoff.pull_request.state == PullRequestState::Open {
                inspect_native_pr_handoff_ci(&github_host, &handoff.pull_request)
                    .map_err(|message| {
                        warn!(
                            issue_identifier = %issue.identifier,
                            pull_request = %handoff.pull_request.url,
                            message = %message,
                            "native PR handoff monitor could not inspect CI"
                        );
                        message
                    })
                    .ok()
            } else {
                None
            };
            let action = native_pr_handoff_monitor_action(&handoff, ci.as_ref());
            if native_pr_handoff_monitor_blocks_dispatch(action) {
                result.monitored_issue_ids.insert(issue.id.clone());
            }
            let final_gate_start = match native_final_pr_gate_start_request_from_handoff(
                self.config.effective(),
                issue,
                &handoff,
            ) {
                Ok(request) => request,
                Err(err) => {
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "native PR handoff monitor could not prepare final PR gate start for {}: {err}",
                        issue.identifier
                    ));
                    continue;
                }
            };
            match self
                .record_native_pr_handoff_monitor(
                    adapter,
                    issue,
                    &workpad_body,
                    &handoff,
                    ci.as_ref(),
                    &action,
                    final_gate_start.as_ref(),
                )
                .await
            {
                Ok(updated) => {
                    result.changed |= updated;
                }
                Err(err) => {
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "native PR handoff monitor could not update Workpad for {}: {err}",
                        issue.identifier
                    ));
                    continue;
                }
            }
            if let Some(final_gate_start) = final_gate_start.as_ref() {
                match self
                    .deliver_native_final_pr_gate_request_to_kapellmeister(issue, final_gate_start)
                    .await
                {
                    Ok(started) => {
                        result.changed |= started;
                        result.monitored_issue_ids.insert(issue.id.clone());
                    }
                    Err(err) => {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "native PR handoff monitor could not deliver final PR gate request for {}: {err}",
                            issue.identifier
                        ));
                        continue;
                    }
                }
            }
            match action {
                NativePrHandoffMonitorAction::Waiting(_) => {
                    result.changed |= self.repair_native_pr_handoff_projection_lane(issue).await;
                }
                NativePrHandoffMonitorAction::Replan(_) => {
                    let Some(planning_lane) =
                        projected_native_lane_for_node(self.config.effective(), issue, "planner")
                    else {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "native PR handoff monitor requested replan but template projection has no planner lane for {}",
                            issue.identifier
                        ));
                        continue;
                    };
                    if self
                        .project_issue_to_native_lane(
                            issue,
                            &planning_lane,
                            "native-pr-handoff-monitor-replan",
                        )
                        .await
                    {
                        result.changed = true;
                    }
                }
                NativePrHandoffMonitorAction::Complete => {
                    let Some(done_lane) = projected_native_terminal_lane_for_status(
                        self.config.effective(),
                        issue,
                        MobRunStatus::Succeeded,
                    ) else {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "native PR handoff monitor observed merged PR but template projection has no success lane for {}",
                            issue.identifier
                        ));
                        continue;
                    };
                    if self
                        .project_issue_to_native_lane(
                            issue,
                            &done_lane,
                            "native-pr-handoff-monitor-complete",
                        )
                        .await
                    {
                        let mut state = self.state.write().await;
                        state.completed.insert(issue.id.clone());
                        result.changed = true;
                    }
                }
            }
        }
        result
    }

    async fn reconcile_native_deploy_lane(&self, adapter: &IssueAdapter, issues: &[Issue]) -> bool {
        if !self.config.effective().deploy.enabled {
            return false;
        }
        let github_host = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        let mut changed = false;
        for issue in issues {
            if !native_deploy_lane_candidate(self.config.effective(), issue) {
                continue;
            }
            match self
                .native_deploy_start_request(adapter, issue, &github_host)
                .await
            {
                Ok(Some(request)) => {
                    let result = crate::http::execute_deploy_transaction(
                        self.config.effective().clone(),
                        self.meta_runtime_registry(),
                        request,
                    )
                    .await;
                    if result.accepted {
                        info!(
                            issue_identifier = %result.issue_identifier,
                            message = %result.message,
                            "mechanical deploy lane transaction completed"
                        );
                        changed = true;
                    } else {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "mechanical deploy lane transaction for {} did not complete: {}",
                            result.issue_identifier, result.message
                        ));
                        warn!(
                            issue_identifier = %result.issue_identifier,
                            message = %result.message,
                            failure_category = ?result.failure_category,
                            "mechanical deploy lane transaction did not complete"
                        );
                        changed = true;
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "mechanical deploy lane reconciliation failed for {}: {err}",
                        issue.identifier
                    ));
                    warn!(
                        issue_identifier = %issue.identifier,
                        error = %err,
                        "mechanical deploy lane reconciliation failed"
                    );
                }
            }
        }
        changed
    }

    async fn native_deploy_start_request(
        &self,
        adapter: &IssueAdapter,
        issue: &Issue,
        github_host: &dyn NativeStatusGitHubInspector,
    ) -> Result<Option<DeployStartRequest>> {
        let issue_ref = issue_ref_for_identifier(&issue.identifier);
        let workpad_body = adapter
            .workpad_body(&issue_ref, "## Fugue Workpad")
            .await?
            .unwrap_or_else(|| "## Fugue Workpad".to_string());
        let linear = recover_native_linear_evidence(issue, Some(&workpad_body));
        let recorded = recover_recorded_native_pr_handoff(
            self.config.effective(),
            issue,
            Some(&linear),
            github_host,
        )?;
        let Some(handoff) = recorded.or_else(|| {
            recover_existing_native_pr_monitor_handoff_for_issue(
                self.config.effective(),
                issue,
                Some(&linear),
                github_host,
            )
        }) else {
            warn!(
                issue_identifier = %issue.identifier,
                "mechanical deploy lane found no durable PR evidence"
            );
            return Ok(None);
        };
        if !handoff.rejected_pull_requests.is_empty() {
            return Err(FugueError::ResponseError(
                "PR evidence conflicts with the current issue branch".into(),
            ));
        }
        crate::pr_handoff::record(self.config.effective(), issue, &handoff.pull_request)?;
        let ci = if handoff.pull_request.state == PullRequestState::Open {
            inspect_native_pr_handoff_ci(github_host, &handoff.pull_request)
                .map_err(|message| {
                    warn!(
                        issue_identifier = %issue.identifier,
                        pull_request = %handoff.pull_request.url,
                        message = %message,
                        "mechanical deploy lane could not inspect CI"
                    );
                    message
                })
                .ok()
        } else {
            None
        };
        let unresolved_workgraph_blockers = self
            .unresolved_deploy_workgraph_blocker_count(&issue.identifier)
            .await?;
        let idempotency_key = IdempotencyKey(format!(
            "deploy:{}:{}:{}",
            issue.identifier, handoff.pull_request.number.0, handoff.pull_request.head_sha.0
        ));
        Ok(Some(DeployStartRequest {
            issue: issue_ref,
            pull_request: handoff.pull_request.clone(),
            recorded_head_sha: Some(handoff.pull_request.head_sha.clone()),
            ci_status: ci.map(|output| output.status),
            unresolved_workgraph_blockers,
            idempotency_key,
            retry_deployment: None,
        }))
    }

    async fn unresolved_deploy_workgraph_blocker_count(
        &self,
        issue_identifier: &str,
    ) -> Result<usize> {
        let gate = crate::workgraph_gate::WorkGraphTerminalGate::new(
            meta_runtime_state_path(&self.config.effective().meerkat).join("workgraph.sqlite3"),
            self.config.effective().tracker.terminal_states.clone(),
            native_workgraph_blocked_states(self.config.effective()),
        )
        .for_mob(self.config.effective().meerkat.mob_id().into_owned());
        gate.unresolved_blocking_items(issue_identifier)
            .await
            .map(|items| items.len())
            .map_err(FugueError::ResponseError)
    }

    async fn reconcile_native_kapellmeister_pr_attention_handoffs(
        &self,
        adapter: &IssueAdapter,
        issues: &[Issue],
    ) -> bool {
        let github_host = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        let mut changed = false;
        for issue in issues {
            if !matches!(
                classify_issue_policy_mode(issue, self.config.effective()),
                IssuePolicyMode::Kapellmeister
            ) {
                continue;
            }
            if !native_pr_handoff_monitor_candidate(self.config.effective(), issue) {
                continue;
            }
            let claimed_without_running = {
                let state = self.state.read().await;
                state.claimed.contains(&issue.id) && !state.running.contains_key(&issue.id)
            };
            if claimed_without_running {
                continue;
            }
            let issue_ref = issue_ref_for_identifier(&issue.identifier);
            let workpad_body = match adapter.workpad_body(&issue_ref, "## Fugue Workpad").await {
                Ok(body) => body.unwrap_or_else(|| "## Fugue Workpad".to_string()),
                Err(err) => {
                    warn!(
                        issue_identifier = %issue.identifier,
                        message = %err,
                        "native Kapellmeister PR attention repair could not fetch Workpad"
                    );
                    continue;
                }
            };
            let linear = recover_native_linear_evidence(issue, Some(&workpad_body));
            if linear.reviewer_rework_requested {
                continue;
            }
            let Some(handoff) = recover_existing_native_pr_monitor_handoff_for_issue(
                self.config.effective(),
                issue,
                Some(&linear),
                &github_host,
            ) else {
                continue;
            };
            let ci = if handoff.pull_request.state == PullRequestState::Open {
                inspect_native_pr_handoff_ci(&github_host, &handoff.pull_request)
                    .map_err(|message| {
                        warn!(
                            issue_identifier = %issue.identifier,
                            pull_request = %handoff.pull_request.url,
                            message = %message,
                            "native Kapellmeister PR attention repair could not inspect CI"
                        );
                        message
                    })
                    .ok()
            } else {
                None
            };
            let action = native_pr_handoff_monitor_action(&handoff, ci.as_ref());
            let final_gate_start = match native_final_pr_gate_start_request_from_handoff(
                self.config.effective(),
                issue,
                &handoff,
            ) {
                Ok(request) => request,
                Err(err) => {
                    warn!(
                        issue_identifier = %issue.identifier,
                        message = %err,
                        "native Kapellmeister PR attention repair could not prepare final PR gate start"
                    );
                    None
                }
            };
            if let Err(err) = self
                .record_native_pr_handoff_monitor(
                    adapter,
                    issue,
                    &workpad_body,
                    &handoff,
                    ci.as_ref(),
                    &action,
                    final_gate_start.as_ref(),
                )
                .await
            {
                warn!(
                    issue_identifier = %issue.identifier,
                    message = %err,
                    "native Kapellmeister PR attention repair could not update Workpad"
                );
            }
            self.deliver_native_pr_handoff_monitor_signal_to_kapellmeister(
                issue,
                &handoff,
                ci.as_ref(),
                action,
            );
            if let Some(final_gate_start) = final_gate_start.as_ref() {
                match self
                    .deliver_native_final_pr_gate_request_to_kapellmeister(issue, final_gate_start)
                    .await
                {
                    Ok(delivered) => changed |= delivered,
                    Err(err) => warn!(
                        issue_identifier = %issue.identifier,
                        message = %err,
                        "native Kapellmeister PR attention repair could not deliver final PR gate request"
                    ),
                }
            }
        }
        changed
    }

    async fn ensure_linear_projection_lanes(&self, adapter: &IssueAdapter) -> Result<()> {
        adapter
            .ensure_projection_lanes(
                &self.config.effective().tracker,
                &self.config.effective().linear,
            )
            .await
    }

    async fn reconcile_linear_lane_observations(&self, issues: &[Issue]) {
        let mut observations = Vec::new();
        for issue in issues {
            observations.push(IssueLaneRuntimeObservation {
                issue: issue.clone(),
                running_mob: self.running_mob_observation_for_issue(issue).await,
            });
        }
        let signals = match reconcile_issue_lane_observation_state(
            &self.config.effective().workflow_dir,
            self.config.effective(),
            observations,
        ) {
            Ok(signals) => signals,
            Err(err) => {
                let mut orchestrator_state = self.state.write().await;
                orchestrator_state.last_error = Some(err.to_string());
                warn!(error = %err, "failed to reconcile issue lane observations");
                return;
            }
        };
        if !signals.is_empty() {
            info!(
                count = signals.len(),
                "observed external issue lane changes"
            );
            self.deliver_kapellmeister_shadow_signals(signals);
        }
        self.reconcile_linear_comment_observations(issues).await;
    }

    async fn reconcile_linear_comment_observations(&self, issues: &[Issue]) {
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(error = %err, "issue comment observation skipped because issue adapter is unavailable");
                return;
            }
        };
        let signals = match adapter
            .reconcile_issue_comment_observation_state(
                &self.config.effective().workflow_dir,
                issues,
            )
            .await
        {
            Ok(signals) => signals,
            Err(err) => {
                let mut orchestrator_state = self.state.write().await;
                orchestrator_state.last_error = Some(err.to_string());
                warn!(error = %err, "failed to reconcile issue comment observations");
                return;
            }
        };
        if !signals.is_empty() {
            info!(
                count = signals.len(),
                "delivering issue comment control-plane observations to Kapellmeister"
            );
            self.deliver_kapellmeister_shadow_signals(signals);
        }
    }

    async fn running_mob_observation_for_issue(
        &self,
        issue: &Issue,
    ) -> Option<KapellmeisterRunningMobObservation> {
        let state = self.state.read().await;
        let entry = state.running.get(&issue.id)?;
        let native_run = entry.native_run.as_ref()?;
        Some(KapellmeisterRunningMobObservation {
            run_id: native_run.run_id.clone(),
            status: MobRunStatus::Running,
            phase_labels: config_phase_labels_for_running_entry(self.config.effective(), entry),
        })
    }

    async fn reconcile_kapellmeister_shadow_host(&mut self) {
        let effective = self.config.effective();
        if !effective.kapellmeister.shadow_mode.enabled {
            return;
        }
        if matches!(effective.mob.backend, MobExecutionBackend::NativeExecution) {
            info!(
                "skipping startup Kapellmeister shadow reconcile for native Meta repo Kapellmeister"
            );
            return;
        }
        let repo_scope = KapellmeisterRepoScope::new(
            WorkspaceKey("repo".to_string()),
            effective.workflow_dir.clone(),
        );
        let host =
            KapellmeisterShadowHost::new(effective.kapellmeister.shadow_mode.clone(), repo_scope);
        let mut runtime = RkatKapellmeisterShadowRuntime::new(
            effective.meerkat.clone(),
            effective.tracker.clone(),
            effective.workflow_dir.clone(),
        );
        match tokio::time::timeout(
            Duration::from_millis(KAPELLMEISTER_SHADOW_RECONCILE_TIMEOUT_MS),
            host.reconcile(&mut runtime),
        )
        .await
        {
            Ok(Ok(status)) => info!(?status, "kapellmeister shadow host reconciled"),
            Ok(Err(err)) => {
                let mut state = self.state.write().await;
                state.last_error = Some(err.to_string());
                warn!(error = %err, "kapellmeister shadow host reconcile failed");
            }
            Err(err) => {
                warn!(
                    error = %err,
                    timeout_ms = KAPELLMEISTER_SHADOW_RECONCILE_TIMEOUT_MS,
                    "kapellmeister shadow host reconcile timed out; dispatch will continue"
                );
            }
        }
    }

    fn deliver_kapellmeister_shadow_signal(&self, signal: KapellmeisterShadowSignal) {
        self.deliver_kapellmeister_shadow_signals(vec![signal]);
    }

    fn deliver_kapellmeister_shadow_signals(&self, signals: Vec<KapellmeisterShadowSignal>) {
        let original_count = signals.len();
        if original_count == 0 {
            return;
        }
        let signals = match self.kapellmeister_signal_deduper.lock() {
            Ok(mut deduper) => signals
                .into_iter()
                .filter(|signal| deduper.accept(&signal.id))
                .collect::<Vec<_>>(),
            Err(_) => {
                warn!("kapellmeister signal deduper lock is poisoned; dropping signal batch");
                Vec::new()
            }
        };
        if signals.is_empty() {
            debug!(
                skipped_duplicate_count = original_count,
                "suppressed duplicate Fugue host signal batch for repo Kapellmeister"
            );
            return;
        }
        let signal_ids = signals
            .iter()
            .map(|signal| signal.id.0.clone())
            .collect::<Vec<_>>();
        match repo_kapellmeister_signal_message_with_lanes(
            &signals,
            &self.config.effective().linear.lanes.success,
            self.config.effective().linear.lanes.review_ready.as_deref(),
            self.config
                .effective()
                .independent_review
                .enabled
                .then_some(self.config.effective().independent_review.lane.as_str()),
        ) {
            Ok(message) => {
                #[cfg(test)]
                if let Some(messages) = &self.captured_native_signal_messages {
                    messages.lock().expect("test signal recorder").push(message);
                    return;
                }
                let registry = self.meta_runtime_registry.clone();
                tokio::spawn(async move {
                    for attempt in 1..=3 {
                        let mut delivery = Box::pin(
                            registry.deliver_repo_kapellmeister_signal_batch(message.clone()),
                        );
                        let mut pending_logs = 0_u64;
                        loop {
                            let pending_log = tokio::time::sleep(Duration::from_millis(
                                KAPELLMEISTER_REPO_SIGNAL_PENDING_LOG_MS,
                            ));
                            tokio::pin!(pending_log);
                            tokio::select! {
                                result = &mut delivery => {
                                    match result {
                                        Ok(accepted) => {
                                            info!(
                                                interaction_id = %accepted.interaction_id,
                                                identity = %accepted.identity,
                                                attempt,
                                                signal_ids = ?signal_ids,
                                                "accepted Fugue host signal batch for repo Kapellmeister delivery"
                                            );
                                            return;
                                        }
                                        Err(error) => {
                                            warn!(
                                                error = %error,
                                                attempt,
                                                signal_ids = ?signal_ids,
                                                "failed to deliver Fugue host signal batch to repo Kapellmeister"
                                            );
                                            break;
                                        }
                                    }
                                }
                                () = &mut pending_log => {
                                    pending_logs = pending_logs.saturating_add(1);
                                    warn!(
                                        pending_for_ms = pending_logs.saturating_mul(KAPELLMEISTER_REPO_SIGNAL_PENDING_LOG_MS),
                                        pending_log_interval_ms = KAPELLMEISTER_REPO_SIGNAL_PENDING_LOG_MS,
                                        attempt,
                                        signal_ids = ?signal_ids,
                                        "delivery still pending for Fugue host signal batch to repo Kapellmeister"
                                    );
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                });
            }
            Err(error) => warn!(
                error = %error,
                "failed to encode Fugue host signal batch for repo Kapellmeister"
            ),
        }

        // Host signals have one canonical live path: the Meta repo Kapellmeister.
        // The Kapellmeister shadow runtime is only reconciled at startup for persisted-state
        // delivering every signal through both paths spawns duplicate
        // rkat-rpc clients and can starve the service of file descriptors.
    }

    async fn reload_workflow_if_needed(&mut self) -> bool {
        if self.config.reload_if_changed() {
            let effective = self.config.effective();
            let mut state = self.state.write().await;
            state.poll_interval_ms = effective.polling.interval_ms;
            state.max_concurrent_agents = effective.agent.max_concurrent_agents;
            state.last_error = None;
            info!("workflow reload completed");
        } else if let Some(error) = self.config.last_reload_error() {
            let mut state = self.state.write().await;
            state.last_error = Some(error.to_string());
            warn!(
                error = error,
                "workflow reload failed keeping last_good_config"
            );
            return true;
        }
        false
    }

    async fn startup_terminal_workspace_cleanup(&self) {
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(error = %err, "startup cleanup skipped");
                return;
            }
        };
        match adapter
            .fetch_issues_by_states(&self.config.effective().tracker.terminal_states)
            .await
        {
            Ok(issues) => {
                for issue in issues {
                    clean_workspace_for_issue_identifier_best_effort(
                        self.config.effective(),
                        &issue.identifier,
                    )
                    .await;
                }
            }
            Err(err) => warn!(error = %err, "startup terminal cleanup failed"),
        }
    }

    async fn reconcile_running_issues(&mut self) {
        self.recover_native_running_entries_after_restart().await;
        self.reconcile_native_mob_events().await;
        self.reconcile_stalled_runs().await;
        let running_ids = {
            let state = self.state.read().await;
            state.running.keys().cloned().collect::<Vec<_>>()
        };
        if running_ids.is_empty() {
            return;
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(error = %err, "reconciliation issue adapter unavailable keeping workers");
                return;
            }
        };
        let refreshed = match adapter.fetch_issue_states_by_ids(&running_ids).await {
            Ok(issues) => issues,
            Err(err) => {
                warn!(error = %err, "reconciliation state refresh failed keeping workers");
                return;
            }
        };
        let refreshed_ids = refreshed
            .iter()
            .map(|issue| issue.id.clone())
            .collect::<HashSet<_>>();
        for issue in refreshed {
            let lane_action =
                if native_kapellmeister_prompt_control_enabled(self.config.effective())
                    && matches!(
                        dispatch_backend_for_issue(&issue, self.config.effective()),
                        DispatchBackend::NativeMob
                    )
                {
                    interpret_native_prompt_control_lane(
                        &self.config.effective().tracker,
                        &issue.state,
                    )
                } else {
                    interpret_human_lane_move(&self.config.effective().tracker, &issue.state)
                };
            if matches!(
                classify_issue_policy_mode(&issue, self.config.effective()),
                IssuePolicyMode::Kapellmeister
            ) {
                if matches!(lane_action, HumanLaneAction::StopAndCleanup) {
                    self.terminate_running_issue(
                        &issue.id,
                        true,
                        false,
                        "terminal_state",
                        TerminationAuthority::LinearTerminalLane,
                    )
                    .await;
                    continue;
                }
                let mut state = self.state.write().await;
                if let Some(entry) = state.running.get_mut(&issue.id) {
                    entry.issue = issue;
                }
                continue;
            }
            match lane_action {
                HumanLaneAction::StopAndCleanup => {
                    self.terminate_running_issue(
                        &issue.id,
                        true,
                        false,
                        "terminal_state",
                        TerminationAuthority::LinearTerminalLane,
                    )
                    .await;
                }
                HumanLaneAction::Continue => {
                    let mut state = self.state.write().await;
                    if let Some(entry) = state.running.get_mut(&issue.id) {
                        entry.issue = issue;
                    }
                }
                HumanLaneAction::StopNoRetry => {
                    let native_run = {
                        let state = self.state.read().await;
                        state
                            .running
                            .get(&issue.id)
                            .and_then(|entry| entry.native_run.clone())
                    };
                    if let Some(repaired_issue) = self
                        .repair_native_compact_lane_drift_for_issue(&issue, native_run.as_ref())
                        .await
                    {
                        let mut state = self.state.write().await;
                        if let Some(entry) = state.running.get_mut(&repaired_issue.id) {
                            entry.issue = repaired_issue.clone();
                        }
                        let events = state
                            .recent_events
                            .entry(repaired_issue.id.clone())
                            .or_default();
                        events.push(RecentEvent {
                            at: Utc::now(),
                            event: "native_compact_lane_repair".to_string(),
                            message: Some(format!(
                                "repaired external Linear state {} -> {}",
                                issue.state, repaired_issue.state
                            )),
                        });
                        state.last_error = None;
                        continue;
                    }
                    self.terminate_running_issue(
                        &issue.id,
                        false,
                        false,
                        "non_active_state",
                        TerminationAuthority::LinearNonterminalLane,
                    )
                    .await;
                }
            }
        }
        for missing_id in running_ids
            .into_iter()
            .filter(|issue_id| !refreshed_ids.contains(issue_id))
        {
            self.terminate_running_issue(
                &missing_id,
                false,
                false,
                "missing_issue",
                TerminationAuthority::HostRuntime,
            )
            .await;
        }
    }

    async fn reconcile_stalled_runs(&mut self) {
        let configured_stall_timeout_ms = self.config.effective().meerkat.stall_timeout_ms;
        if configured_stall_timeout_ms <= 0 {
            return;
        }
        let now = Utc::now();
        let candidates = {
            let state = self.state.read().await;
            state
                .running
                .iter()
                .filter_map(|(issue_id, entry)| {
                    if matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister) {
                        return None;
                    }
                    let last = entry
                        .session
                        .last_meerkat_timestamp
                        .unwrap_or(entry.started_at);
                    let elapsed = (now - last).num_milliseconds();
                    let effective_stall_timeout_ms =
                        self.native_stall_timeout_ms_for_entry(entry, configured_stall_timeout_ms);
                    (elapsed > effective_stall_timeout_ms).then(|| {
                        (
                            issue_id.clone(),
                            entry.identifier.clone(),
                            entry
                                .native_run
                                .as_ref()
                                .map(|native_run| native_run.run_id.clone()),
                            effective_stall_timeout_ms,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        for (issue_id, identifier, native_run_id, effective_stall_timeout_ms) in candidates {
            if let Some(run_id) = native_run_id.as_ref()
                && self
                    .record_active_native_status_heartbeat(
                        &issue_id,
                        &identifier,
                        run_id,
                        "stall_timeout_guard",
                    )
                    .await
            {
                continue;
            }
            if let Some(run_id) = native_run_id.as_ref() {
                let detail = format!(
                    "native Meta run {} had no fresh progress before effective stall timeout {}ms elapsed (configured meerkat.stall_timeout_ms={}ms)",
                    run_id.0, effective_stall_timeout_ms, configured_stall_timeout_ms
                );
                if self
                    .park_running_native_issue_for_liveness(
                        &issue_id,
                        NativeRunParkReason::Stalled,
                        Some(detail),
                    )
                    .await
                {
                    continue;
                }
            }
            self.terminate_running_issue(
                &issue_id,
                false,
                true,
                "stall_timeout",
                TerminationAuthority::LinearNonterminalLane,
            )
            .await;
        }
    }

    fn native_stall_timeout_ms_for_entry(
        &self,
        entry: &RunningEntry,
        configured_stall_timeout_ms: i64,
    ) -> i64 {
        let template_floor = self
            .config
            .effective()
            .select_template_for_issue(&entry.issue)
            .ok()
            .and_then(|selected| native_template_step_timeout_floor_ms(selected.template))
            .map(|timeout| timeout.saturating_add(NATIVE_STALL_TIMEOUT_STEP_GRACE_MS))
            .unwrap_or(0);
        configured_stall_timeout_ms.max(template_floor)
    }

    async fn record_active_native_status_heartbeat(
        &self,
        issue_id: &str,
        identifier: &str,
        run_id: &MobRunId,
        source: &str,
    ) -> bool {
        let run = match self
            .native_execution_host
            .status(crate::tools::InspectRunInput {
                run_id: run_id.clone(),
            }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobInspectRun(inspected),
                ..
            } => inspected.run,
            FugueToolResult::Ok { output, .. } => {
                warn!(
                    ?output,
                    issue_identifier = %identifier,
                    run_id = %run_id.0,
                    "native mob status heartbeat returned unexpected output"
                );
                return false;
            }
            FugueToolResult::Err { error, .. } => {
                warn!(
                    message = %error.message,
                    issue_identifier = %identifier,
                    run_id = %run_id.0,
                    "native mob status heartbeat failed"
                );
                return false;
            }
        };
        if !native_run_status_is_active(run.status) {
            return false;
        }
        let observed_at = Utc::now();
        let event = "native_mob_status_heartbeat".to_string();
        let message = Some(format!(
            "{source}: Meta reports run {} is {:?}",
            run.run_id.0, run.status
        ));
        let mut state = self.state.write().await;
        let Some(entry) = state.running.get_mut(issue_id) else {
            return true;
        };
        if entry
            .session
            .last_meerkat_timestamp
            .is_some_and(|last| run.updated_at <= last)
        {
            warn!(
                issue_identifier = %identifier,
                run_id = %run_id.0,
                run_updated_at = %run.updated_at,
                source,
                "native mob status heartbeat had no fresh runtime progress"
            );
            return false;
        }
        entry.session.last_meerkat_event = Some(event.clone());
        entry.session.last_meerkat_timestamp = Some(run.updated_at);
        entry.session.last_meerkat_message = message.clone();
        if let Some(native_run) = entry.native_run.as_mut() {
            native_run.recovery = native_status_recovery_from_durable_evidence(
                &run,
                NativeDurableEvidence {
                    run: Some(run.clone()),
                    linear: native_run.recovery.linear.clone(),
                    github: native_run.recovery.github.clone(),
                    workspace: native_run.recovery.workspace.clone(),
                    events: native_run.observed_events.clone(),
                },
                self.config
                    .effective()
                    .select_template_for_issue(&entry.issue)
                    .ok()
                    .and_then(|selected| selected.template.max_review_cycles),
            );
        }
        let events = state.recent_events.entry(issue_id.to_string()).or_default();
        events.push(RecentEvent {
            at: observed_at,
            event,
            message,
        });
        if events.len() > 50 {
            let excess = events.len() - 50;
            events.drain(0..excess);
        }
        true
    }

    async fn recover_native_running_entries_after_restart(&mut self) {
        if self.config.effective().mob.backend != MobExecutionBackend::NativeExecution
            || !self.config.effective().mob.native.enabled
        {
            return;
        }
        let existing = {
            let state = self.state.read().await;
            state.running.len()
        };
        if existing >= self.config.effective().agent.max_concurrent_agents {
            return;
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(error = %err, "native mob restart reconciliation issue adapter unavailable");
                return;
            }
        };
        let issues = match adapter.fetch_open_project_issues().await {
            Ok(issues) => issues,
            Err(err) => {
                warn!(error = %err, "native mob restart reconciliation issue refresh failed");
                return;
            }
        };
        let restore_capacity = self.config.effective().agent.max_concurrent_agents - existing;
        let restore_keys =
            native_runtime_restore_keys(self.config.effective(), &issues, restore_capacity);
        if let Err(error) = self
            .native_execution_host
            .restore_persisted_runtimes(restore_keys, restore_capacity)
        {
            warn!(error = %error, "native mob persisted runtime restore skipped");
            let mut state = self.state.write().await;
            state.last_error = Some(format!(
                "native mob persisted runtime restore skipped: {error}"
            ));
            return;
        }
        let result = self
            .native_execution_host
            .list_runs(crate::tools::ListRunsInput {
                issue: None,
                status: Some(MobRunStatus::Running),
                cursor: None,
                limit: 100,
            });
        let runs = match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobListRuns(output),
                ..
            } => output.runs,
            FugueToolResult::Ok { output, .. } => {
                let mut state = self.state.write().await;
                state.last_error = Some(format!(
                    "native mob restart reconciliation returned unexpected output {output:?}"
                ));
                return;
            }
            FugueToolResult::Err { error, .. } => {
                warn!(message = %error.message, "native mob restart reconciliation skipped");
                return;
            }
        };
        if runs.is_empty() {
            return;
        }
        let workspace = WorkspaceManager::for_service(self.config.effective());
        let github_host = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        let mut recovered = Vec::new();
        for run in newest_native_runs_by_issue(runs) {
            let workpad_body = match run.issue.as_ref() {
                Some(issue_ref) => {
                    match adapter.workpad_body(issue_ref, "## Fugue Workpad").await {
                        Ok(body) => body,
                        Err(err) => {
                            warn!(
                                error = %err,
                                run_id = %run.run_id.0,
                                "native mob restart reconciliation workpad fetch failed"
                            );
                            None
                        }
                    }
                }
                None => None,
            };
            if let Some(workpad_run_id) = workpad_body
                .as_deref()
                .and_then(parse_workpad_native_run_id)
                && native_run_generation_sort_key(&workpad_run_id)
                    > native_run_generation_sort_key(&run.run_id)
            {
                info!(
                    run_id = %run.run_id.0,
                    workpad_run_id = %workpad_run_id.0,
                    "native mob restart reconciliation skipped stale run because Workpad points at a newer run"
                );
                continue;
            }
            if let Some((issue, mut entry)) = recover_native_running_entry(
                &run,
                &issues,
                self.config.effective(),
                &workspace,
                workpad_body.as_deref(),
            ) {
                if matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister) {
                    self.deliver_kapellmeister_shadow_signal(runtime_recovered_signal(
                        self.config.effective(),
                        &issue,
                        &run,
                        entry.native_run.as_ref(),
                    ));
                }
                if let Some(native_run) = entry.native_run.as_mut() {
                    native_run.recovery = refresh_native_status_recovery(
                        &run,
                        &entry.issue,
                        self.config.effective(),
                        &workspace,
                        &native_run.observed_events,
                        &native_run.recovery,
                        workpad_body.as_deref(),
                        Some(&github_host),
                    );
                }
                recovered.push((issue, entry));
            }
        }
        let mut state = self.state.write().await;
        for (issue, entry) in recovered {
            if state.running.contains_key(&issue.id) || state.claimed.contains(&issue.id) {
                continue;
            }
            state.claimed.insert(issue.id.clone());
            state.running.insert(issue.id, entry);
        }
    }

    async fn reconcile_native_mob_events(&mut self) {
        let github_host = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        self.reconcile_native_mob_events_with_tools(None, Some(&github_host))
            .await;
    }

    async fn fetch_native_workpad_body(
        &self,
        issue_ref: &LinearIssueRef,
        issue_identifier: &str,
        run_id: &MobRunId,
        context: &'static str,
    ) -> Option<String> {
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier,
                    run_id = %run_id.0,
                    context,
                    "native mob Workpad refresh skipped"
                );
                return None;
            }
        };
        match adapter.workpad_body(issue_ref, "## Fugue Workpad").await {
            Ok(body) => body,
            Err(err) => {
                warn!(
                    message = %err,
                    issue_identifier,
                    run_id = %run_id.0,
                    context,
                    "native mob Workpad refresh failed"
                );
                None
            }
        }
    }

    async fn reconcile_native_mob_events_with_tools(
        &mut self,
        mut workpad_executor: Option<&mut (dyn KapellmeisterHostToolExecutor + Send)>,
        github_host: Option<&dyn NativeStatusGitHubInspector>,
    ) {
        let native_runs = {
            let state = self.state.read().await;
            state
                .running
                .iter()
                .filter_map(|(issue_id, entry)| {
                    entry.native_run.as_ref().map(|native_run| {
                        (
                            issue_id.clone(),
                            entry.identifier.clone(),
                            native_run.run_id.clone(),
                            entry.issue.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        for (issue_id, identifier, run_id, issue) in native_runs {
            let kapellmeister_mode = matches!(
                classify_issue_policy_mode(&issue, self.config.effective()),
                IssuePolicyMode::Kapellmeister
            );
            let observation = match self.native_event_observer.observe(
                self.native_execution_host.as_ref(),
                &run_id,
                100,
            ) {
                Ok(observation) => observation,
                Err(err) => {
                    let detail = err.to_string();
                    warn!(
                        error = %detail,
                        issue_identifier = %identifier,
                        run_id = %run_id.0,
                        "native mob structural event observation failed"
                    );
                    if native_mechanism_failure_is_nonretryable(&detail)
                        && self
                            .park_running_native_issue_for_liveness(
                                &issue_id,
                                NativeRunParkReason::HostMechanics,
                                Some(detail.clone()),
                            )
                            .await
                    {
                        continue;
                    }
                    let mut state = self.state.write().await;
                    state.last_error = Some(detail);
                    continue;
                }
            };
            let inspected_run =
                match self
                    .native_execution_host
                    .status(crate::tools::InspectRunInput {
                        run_id: run_id.clone(),
                    }) {
                    FugueToolResult::Ok {
                        output: FugueToolOutput::MobInspectRun(inspected),
                        ..
                    } => Some(inspected.run),
                    FugueToolResult::Ok { output, .. } => {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "native mob status returned unexpected output {output:?}"
                        ));
                        None
                    }
                    FugueToolResult::Err { error, .. } => {
                        if native_mechanism_failure_is_nonretryable(&error.message)
                            && self
                                .park_running_native_issue_for_liveness(
                                    &issue_id,
                                    NativeRunParkReason::HostMechanics,
                                    Some(error.message.clone()),
                                )
                                .await
                        {
                            continue;
                        }
                        warn!(
                            message = %error.message,
                            issue_identifier = %identifier,
                            run_id = %run_id.0,
                            "native mob status inspection failed"
                        );
                        None
                    }
                };
            if observation.events.is_empty() {
                if let Err(err) = self.native_event_observer.commit(&run_id, &observation) {
                    let mut state = self.state.write().await;
                    state.last_error = Some(err.to_string());
                    warn!(
                        error = %err,
                        issue_identifier = %identifier,
                        run_id = %run_id.0,
                        "native mob cursor commit failed"
                    );
                }
                let mut detected_handoff = None;
                if let Some(run) = inspected_run.as_ref()
                    && native_run_status_is_active(run.status)
                {
                    let refreshed_workpad_body = self
                        .fetch_native_workpad_body(
                            &issue_ref_for_identifier(&identifier),
                            &identifier,
                            &run_id,
                            "native mob status heartbeat",
                        )
                        .await;
                    let workspace = WorkspaceManager::for_service(self.config.effective());
                    let mut state = self.state.write().await;
                    if let Some(entry) = state.running.get_mut(&issue_id) {
                        let status_is_fresh = entry
                            .session
                            .last_meerkat_timestamp
                            .is_none_or(|last| run.updated_at > last);
                        if status_is_fresh {
                            entry.session.last_meerkat_event =
                                Some("native_mob_status_heartbeat".to_string());
                            entry.session.last_meerkat_timestamp = Some(run.updated_at);
                            entry.session.last_meerkat_message =
                                Some(format!("native mob status {:?}", run.status));
                        } else {
                            warn!(
                                issue_identifier = %identifier,
                                run_id = %run_id.0,
                                run_updated_at = %run.updated_at,
                                "native mob status inspection had no fresh runtime progress"
                            );
                        }
                        if let Some(native_run) = entry.native_run.as_mut() {
                            native_run.recovery = refresh_native_status_recovery(
                                run,
                                &entry.issue,
                                self.config.effective(),
                                &workspace,
                                &native_run.observed_events,
                                &native_run.recovery,
                                refreshed_workpad_body.as_deref(),
                                github_host,
                            );
                            if let Some(github) = native_run.recovery.github.as_ref() {
                                let workpad_has_pr =
                                    native_run.recovery.linear.as_ref().is_some_and(|linear| {
                                        linear.workpad_pr_number == Some(github.pull_request.number)
                                            || linear.workpad_mentions_pr
                                    });
                                if !workpad_has_pr {
                                    detected_handoff = Some(DetectedNativePrHandoff {
                                        issue: entry.issue.clone(),
                                        run_id: native_run.run_id.clone(),
                                        pull_request: github.pull_request.clone(),
                                        workpad_has_pr,
                                    });
                                }
                            }
                        }
                    }
                }
                if let Some(handoff) = detected_handoff {
                    self.record_detected_native_pr_handoff(handoff).await;
                }
                if let Some(run) = inspected_run
                    && matches!(
                        run.status,
                        MobRunStatus::Succeeded | MobRunStatus::Failed | MobRunStatus::Cancelled
                    )
                {
                    self.handle_native_run_finished(issue_id.clone(), issue, run.status)
                        .await;
                }
                continue;
            }
            let cumulative_events = {
                let mut state = self.state.write().await;
                let mut cumulative_events = None;
                if let Some(entry) = state.running.get_mut(&issue_id) {
                    let last = observation.events.last();
                    entry.session.last_meerkat_event =
                        Some("native_mob_structural_event".to_string());
                    entry.session.last_meerkat_timestamp = last.map(|event| event.occurred_at);
                    entry.session.last_meerkat_message =
                        last.and_then(|event| event.message.clone());
                    if let Some(native_run) = entry.native_run.as_mut() {
                        merge_mob_event_snapshots(
                            &mut native_run.observed_events,
                            &observation.events,
                        );
                        cumulative_events = Some(native_run.observed_events.clone());
                        if let Some(run) = inspected_run.as_ref() {
                            let workspace = WorkspaceManager::for_service(self.config.effective());
                            native_run.recovery = refresh_native_status_recovery(
                                run,
                                &entry.issue,
                                self.config.effective(),
                                &workspace,
                                &native_run.observed_events,
                                &native_run.recovery,
                                None,
                                github_host,
                            );
                        }
                    }
                    let recent = observation
                        .events
                        .iter()
                        .map(|event| RecentEvent {
                            at: event.occurred_at,
                            event: format!("native_mob:{:?}", event.kind),
                            message: event.message.clone(),
                        })
                        .collect::<Vec<_>>();
                    let events = state.recent_events.entry(issue_id.clone()).or_default();
                    events.extend(recent);
                    if events.len() > 50 {
                        let excess = events.len() - 50;
                        events.drain(0..excess);
                    }
                }
                cumulative_events
            };
            let Some(cumulative_events) = cumulative_events else {
                continue;
            };
            for event in turn_limit_events(&observation.events) {
                if let Err(err) = self
                    .native_execution_host
                    .notify_issue_lead_member_turn_limit(
                        &identifier,
                        &run_id,
                        event.member,
                        event.detail,
                    )
                {
                    let mut state = self.state.write().await;
                    state.last_error = Some(err.clone());
                    warn!(
                        error = %err,
                        issue_identifier = %identifier,
                        run_id = %run_id.0,
                        member = %event.member,
                        "native mob turn-limit notification to issue lead failed"
                    );
                }
            }
            for event in member_terminal_failure_events(&observation.events) {
                if let Err(err) = self
                    .native_execution_host
                    .notify_issue_lead_member_terminal_failure(
                        &identifier,
                        &run_id,
                        event.member,
                        event.detail,
                    )
                {
                    let mut state = self.state.write().await;
                    state.last_error = Some(err.clone());
                    warn!(
                        error = %err,
                        issue_identifier = %identifier,
                        run_id = %run_id.0,
                        member = %event.member,
                        "native mob terminal-failure notification to issue lead failed"
                    );
                }
            }
            let observed_at = Utc::now();
            self.deliver_kapellmeister_shadow_signal(KapellmeisterShadowSignal::worker_started(
                observed_at,
                Some(worker_cursor(
                    &identifier,
                    "native_mob_structural_event",
                    1,
                    observed_at,
                )),
                issue_ref_for_identifier(&identifier),
                workspace_key_for_identifier(&identifier),
                1,
                Some(run_id.0.clone()),
            ));
            let Some(inspected_run) = inspected_run else {
                let mut state = self.state.write().await;
                state.last_error = Some(format!(
                    "native mob status unavailable before cursor commit for {identifier}"
                ));
                continue;
            };
            let effective_status = native_terminal_status_from_events(&observation.events)
                .unwrap_or(inspected_run.status);
            if let Some(projected_lane) = projected_native_lane_for_observation(
                self.config.effective(),
                &issue,
                effective_status,
                &cumulative_events,
            ) && !kapellmeister_mode
                && !issue.state.eq_ignore_ascii_case(&projected_lane)
                && self
                    .project_issue_to_native_lane(
                        &issue,
                        &projected_lane,
                        "native-mob-state-linear-projection",
                    )
                    .await
            {
                let mut state = self.state.write().await;
                if let Some(entry) = state.running.get_mut(&issue_id) {
                    entry.issue.state = projected_lane;
                }
            }
            if kapellmeister_mode {
                if let Err(err) = self.native_event_observer.commit(&run_id, &observation) {
                    let mut state = self.state.write().await;
                    state.last_error = Some(err.to_string());
                    warn!(
                        error = %err,
                        issue_identifier = %identifier,
                        run_id = %run_id.0,
                        "native mob cursor commit failed"
                    );
                    continue;
                }
                if matches!(
                    effective_status,
                    MobRunStatus::Succeeded | MobRunStatus::Failed | MobRunStatus::Cancelled
                ) || observation.events.iter().any(|event| {
                    matches!(event.kind, crate::tools::MobEventKind::RunCompleted { .. })
                }) {
                    self.handle_native_run_finished(issue_id.clone(), issue, effective_status)
                        .await;
                }
                continue;
            }
            let existing_workpad_body = if workpad_executor.is_none() {
                match IssueAdapter::from_config(&self.config.effective().tracker) {
                    Ok(adapter) => match adapter
                        .workpad_body(&issue_ref_for_identifier(&identifier), "## Fugue Workpad")
                        .await
                    {
                        Ok(body) => body,
                        Err(err) => {
                            warn!(
                                error = %err,
                                issue_identifier = %identifier,
                                run_id = %run_id.0,
                                "native mob workpad preservation fetch failed"
                            );
                            None
                        }
                    },
                    Err(err) => {
                        warn!(
                            error = %err,
                            issue_identifier = %identifier,
                            run_id = %run_id.0,
                            "native mob workpad preservation fetch skipped"
                        );
                        None
                    }
                }
            } else {
                None
            };
            let input = native_mob_workpad_input_with_existing(
                issue_ref_for_identifier(&identifier),
                &inspected_run,
                &cumulative_events,
                existing_workpad_body.as_deref(),
            );
            let workpad_body = input.body_markdown.clone();
            if native_mob_workpad_should_upsert(
                &cumulative_events,
                existing_workpad_body.as_deref(),
            ) {
                let workpad_result = if let Some(executor) = workpad_executor.as_deref_mut() {
                    executor.upsert_workpad(input).await
                } else {
                    match IssueAdapter::from_config(&self.config.effective().tracker) {
                        Ok(adapter) => adapter.upsert_workpad_tool_result(input).await,
                        Err(err) => {
                            let mut state = self.state.write().await;
                            state.last_error = Some(err.to_string());
                            warn!(
                                error = %err,
                                issue_identifier = %identifier,
                                run_id = %run_id.0,
                                "native mob workpad cursor commit blocked by issue adapter configuration"
                            );
                            continue;
                        }
                    }
                };
                match workpad_result {
                    FugueToolResult::Ok { .. } => {
                        let workspace = WorkspaceManager::for_service(self.config.effective());
                        let mut state = self.state.write().await;
                        if let Some(entry) = state.running.get_mut(&issue_id)
                            && let Some(native_run) = entry.native_run.as_mut()
                        {
                            native_run.recovery = refresh_native_status_recovery(
                                &inspected_run,
                                &entry.issue,
                                self.config.effective(),
                                &workspace,
                                &native_run.observed_events,
                                &native_run.recovery,
                                Some(&workpad_body),
                                github_host,
                            );
                        }
                    }
                    FugueToolResult::Err { error, .. } => {
                        let mut state = self.state.write().await;
                        state.last_error = Some(error.message.clone());
                        warn!(
                            message = %error.message,
                            issue_identifier = %identifier,
                            run_id = %run_id.0,
                            "native mob workpad upsert failed before cursor commit"
                        );
                        continue;
                    }
                }
            }
            if let Err(err) = self.native_event_observer.commit(&run_id, &observation) {
                let mut state = self.state.write().await;
                state.last_error = Some(err.to_string());
                warn!(
                    error = %err,
                    issue_identifier = %identifier,
                    run_id = %run_id.0,
                    "native mob cursor commit failed"
                );
                continue;
            }
            if matches!(
                effective_status,
                MobRunStatus::Succeeded | MobRunStatus::Failed | MobRunStatus::Cancelled
            ) || observation
                .events
                .iter()
                .any(|event| matches!(event.kind, crate::tools::MobEventKind::RunCompleted { .. }))
            {
                self.handle_native_run_finished(issue_id.clone(), issue, effective_status)
                    .await;
            }
        }
    }

    async fn handle_native_run_finished(
        &mut self,
        issue_id: String,
        issue: Issue,
        status: MobRunStatus,
    ) {
        {
            let state = self.state.read().await;
            if let Some(entry) = state.running.get(&issue_id)
                && matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister)
            {
                info!(
                    issue_id = %issue_id,
                    issue_identifier = %entry.identifier,
                    status = ?status,
                    "kapellmeister-mode native terminal cleanup proceeding without host lane mutation policy"
                );
            }
        }
        let removed = {
            let mut state = self.state.write().await;
            state.running.remove(&issue_id).map(|entry| {
                state.claimed.remove(&issue_id);
                let ended_runtime_seconds =
                    (Utc::now() - entry.started_at).num_milliseconds().max(0) as f64 / 1000.0;
                state.ended_runtime_seconds += ended_runtime_seconds;
                (entry, ended_runtime_seconds)
            })
        };
        let Some((entry, ended_runtime_seconds)) = removed else {
            return;
        };
        let mut entry = entry;
        let Some(mut issue) = self
            .refresh_terminal_native_issue_state(&issue_id, &issue)
            .await
        else {
            let mut state = self.state.write().await;
            state.last_error = Some(format!(
                "native mob terminal handling skipped {} because the Linear issue could not be refreshed",
                entry.identifier
            ));
            return;
        };
        let lane_action = if native_kapellmeister_prompt_control_enabled(self.config.effective()) {
            interpret_native_prompt_control_lane(&self.config.effective().tracker, &issue.state)
        } else {
            interpret_human_lane_move(&self.config.effective().tracker, &issue.state)
        };
        match lane_action {
            HumanLaneAction::Continue => {
                entry.issue = issue.clone();
            }
            HumanLaneAction::StopAndCleanup => {
                if let Err(cleanup_error) =
                    self.cleanup_native_execution_for_entry(&entry, "native-terminal-lane-cleanup")
                {
                    self.park_native_issue_for_liveness(
                        issue_id,
                        entry,
                        NativeRunParkReason::HostMechanics,
                        Some(format!(
                            "native execution cleanup failed after terminal lane observation: {cleanup_error}"
                        )),
                    )
                    .await;
                    return;
                }
                clean_workspace_for_issue_identifier_best_effort(
                    self.config.effective(),
                    &entry.identifier,
                )
                .await;
                let mut state = self.state.write().await;
                state.completed.insert(issue_id);
                return;
            }
            HumanLaneAction::StopNoRetry => {
                if let Some(repaired_issue) = self
                    .repair_native_compact_lane_drift_for_issue(&issue, entry.native_run.as_ref())
                    .await
                {
                    issue = repaired_issue;
                    entry.issue = issue.clone();
                } else {
                    info!(
                        issue_identifier = %entry.identifier,
                        issue_state = %issue.state,
                        "native mob terminal handling stopped because issue is no longer active"
                    );
                    return;
                }
            }
        }
        self.refresh_native_run_linear_recovery_for_terminal_action(&mut entry, &issue)
            .await;
        let selected = self
            .config
            .effective()
            .select_template_for_issue(&entry.issue)
            .ok();
        let evidence = selected.as_ref().map(|selected| {
            NativeMobE2eEvidence::inspect(
                entry
                    .native_run
                    .as_ref()
                    .map(|native_run| native_run.observed_events.as_slice())
                    .unwrap_or(&[]),
                selected.template,
            )
        });
        let recovery_review = entry
            .native_run
            .as_ref()
            .and_then(|native_run| native_run.recovery.review.as_ref())
            .cloned();
        let observed_events = entry
            .native_run
            .as_ref()
            .map(|native_run| native_run.observed_events.as_slice())
            .unwrap_or(&[]);
        let reviewer_gate = latest_reviewer_gate_observation(observed_events);
        let typed_review =
            reviewer_gate
                .invalid_reason()
                .map(|reason| NativeStatusReviewEvidence {
                    reason: reason.to_string(),
                });
        let recovery_review = recovery_review.or(typed_review);
        let run_requests_replan = entry
            .native_run
            .as_ref()
            .is_some_and(native_run_requests_replan);
        let pr_handoff_candidate = native_pr_handoff_candidate(&entry);
        let action = select_native_run_terminal_action(
            status,
            evidence.as_ref(),
            observed_events,
            &reviewer_gate,
            run_requests_replan,
            recovery_review.is_some(),
            pr_handoff_candidate,
        );
        if matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister)
            && !self
                .config
                .effective()
                .tracker
                .is_terminal_state(&issue.state)
        {
            entry.issue = issue;
            let mut state = self.state.write().await;
            state.claimed.insert(issue_id.clone());
            state.ended_runtime_seconds =
                (state.ended_runtime_seconds - ended_runtime_seconds).max(0.0);
            state.running.insert(issue_id, entry);
            return;
        }
        let cleanup_result =
            self.cleanup_native_execution_for_entry(&entry, "native-terminal-finished");
        if let Err(cleanup_error) = cleanup_result {
            self.park_native_issue_for_liveness(
                issue_id,
                entry,
                NativeRunParkReason::HostMechanics,
                Some(format!(
                    "native execution cleanup failed before terminal action: {cleanup_error}"
                )),
            )
            .await;
            return;
        }
        match action {
            NativeRunTerminalAction::Complete => {
                if let Some(done_lane) =
                    projected_native_lane_for_entry(self.config.effective(), &entry, status)
                {
                    let _ = self
                        .project_issue_to_native_lane(
                            &entry.issue,
                            &done_lane,
                            "native-flow-terminal-complete",
                        )
                        .await;
                }
                let mut state = self.state.write().await;
                state.completed.insert(issue_id);
            }
            NativeRunTerminalAction::Retry => {
                let next_attempt = next_native_terminal_attempt(&entry);
                let reason = native_retry_reason(status, evidence.as_ref(), observed_events);
                self.schedule_retry(
                    issue_id,
                    entry.identifier,
                    entry.policy_mode,
                    next_attempt,
                    Some(reason),
                    false,
                )
                .await;
            }
            NativeRunTerminalAction::Replan => {
                let replan_lane = projected_native_lane_for_node(
                    self.config.effective(),
                    &entry.issue,
                    "planner",
                );
                if let Some(replan_lane) = replan_lane {
                    if !self
                        .project_issue_to_native_lane(
                            &entry.issue,
                            &replan_lane,
                            "native-flow-reviewer-red-to-planning",
                        )
                        .await
                    {
                        let mut state = self.state.write().await;
                        state.last_error = Some(format!(
                            "native reviewer requested rework but Fugue could not move {} back to {}",
                            entry.identifier, replan_lane
                        ));
                    }
                } else {
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "native reviewer requested rework but template projection has no planner lane for {}",
                        entry.identifier
                    ));
                }
                let next_attempt = next_native_terminal_attempt(&entry);
                self.schedule_native_rework_retry(
                    issue_id,
                    entry.identifier,
                    entry.policy_mode,
                    next_attempt,
                )
                .await;
            }
            NativeRunTerminalAction::CreatePrHandoff => {
                match self.create_native_pr_handoff(&entry, &issue).await {
                    Ok(pull_request) => {
                        info!(
                            issue_identifier = %entry.identifier,
                            pull_request = %pull_request.url,
                            "native repo boundary created pull request handoff"
                        );
                        if let Some(review_lane) =
                            native_pr_handoff_waiting_lane(self.config.effective(), &entry.issue)
                        {
                            let _ = self
                                .project_issue_to_native_lane(
                                    &entry.issue,
                                    &review_lane,
                                    "native-flow-pr-handoff-reviewing",
                                )
                                .await;
                        }
                        let mut state = self.state.write().await;
                        state.completed.insert(issue_id);
                        state.last_error = None;
                    }
                    Err(err) => {
                        let error_message = err.to_string();
                        if native_pr_handoff_error_requires_operator_park(&error_message) {
                            self.park_native_issue_for_liveness(
                                issue_id,
                                entry,
                                NativeRunParkReason::HostMechanics,
                                Some(error_message),
                            )
                            .await;
                        } else {
                            let mut state = self.state.write().await;
                            state.last_error = Some(format!(
                                "native repo boundary PR handoff failed for {}: {err}",
                                entry.identifier
                            ));
                            drop(state);
                            let next_attempt = next_native_terminal_attempt(&entry);
                            self.schedule_native_rework_retry(
                                issue_id,
                                entry.identifier,
                                entry.policy_mode,
                                next_attempt,
                            )
                            .await;
                        }
                    }
                }
            }
            NativeRunTerminalAction::HumanReview => {
                let review_lane = native_attention_lane(self.config.effective());
                let _ = self
                    .project_issue_to_native_lane(
                        &entry.issue,
                        &review_lane,
                        "native-flow-needs-attention",
                    )
                    .await;
                let mut state = self.state.write().await;
                state.completed.insert(issue_id.clone());
                state.last_error = Some(recovery_review.map_or_else(
                    || {
                        format!(
                            "native mob completed successfully but E2E evidence is incomplete for {}: {:?}",
                            entry.identifier, evidence
                        )
                    },
                    |review| review.reason,
                ));
            }
            NativeRunTerminalAction::Park(reason) => {
                let detail = native_terminal_park_detail(observed_events);
                self.park_native_issue_for_liveness(issue_id, entry, reason, detail)
                    .await;
            }
            NativeRunTerminalAction::KeepRunning => {
                let mut state = self.state.write().await;
                state.running.insert(issue.id.clone(), entry);
            }
        }
    }

    async fn refresh_terminal_native_issue_state(
        &self,
        issue_id: &str,
        fallback: &Issue,
    ) -> Option<Issue> {
        if !self.terminal_issue_refresh {
            return Some(fallback.clone());
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %fallback.identifier,
                    "native terminal issue refresh skipped"
                );
                return Some(fallback.clone());
            }
        };
        match adapter
            .fetch_issue_states_by_ids(&[issue_id.to_string()])
            .await
        {
            Ok(mut issues) => issues.pop().or_else(|| Some(fallback.clone())),
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %fallback.identifier,
                    "native terminal issue refresh failed; using cached issue state"
                );
                Some(fallback.clone())
            }
        }
    }

    async fn create_native_pr_handoff(
        &self,
        entry: &RunningEntry,
        issue: &Issue,
    ) -> Result<GitHubPullRequestSnapshot> {
        let Some(repo) = repository_scope_from_config(self.config.effective()) else {
            return Err(FugueError::InvalidConfig(
                "native PR handoff requires a configured GitHub repository scope".to_string(),
            ));
        };
        let workspace = WorkspaceManager::for_service(self.config.effective());
        let workspace_key = workspace_key_for_identifier(&entry.identifier);
        let inspection = workspace
            .inspect_workspace_key(&workspace_key.0)?
            .ok_or_else(|| {
                FugueError::Workspace(format!(
                    "workspace {} was not found for native PR handoff",
                    workspace_key.0
                ))
            })?;
        if !inspection.dirty {
            return Err(FugueError::Workspace(format!(
                "workspace {} has no changes to hand off as a PR",
                inspection.path.display()
            )));
        }
        let head_branch = native_pr_handoff_branch(issue, &inspection);
        ensure_workspace_branch_for_pr(&inspection.path, &head_branch)?;
        git_add_workspace_changes_excluding_runtime(&inspection.path)?;
        if git_status_excluding_rkat(&inspection.path)?
            .trim()
            .is_empty()
        {
            return Err(FugueError::Workspace(
                "native PR handoff found no non-runtime workspace changes".to_string(),
            ));
        }
        let commit_message = format!("{} {}", issue.identifier, issue.title);
        if !git_staged_changes_empty(&inspection.path)? {
            git_run(&inspection.path, &["commit", "-m", &commit_message])?;
        }
        git_output_with_connections(
            &inspection.path,
            &["push", "-u", "origin", &head_branch.0],
            self.config
                .effective()
                .meerkat
                .runtime_connections
                .as_deref(),
        )?;

        let github = GitHubToolHost::with_connections(
            self.config.effective().meerkat.runtime_connections.clone(),
        );
        let base_branch = match github.inspect_repo(InspectRepoInput { repo: repo.clone() }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubInspectRepo(output),
                ..
            } => output.repo.default_branch,
            FugueToolResult::Err { error, .. } => {
                return Err(FugueError::ResponseError(error.message));
            }
            FugueToolResult::Ok { output, .. } => {
                return Err(FugueError::ResponseError(format!(
                    "github.inspect_repo returned unexpected output {output:?}"
                )));
            }
        };
        let pull_request = match github.create_pr(CreatePrInput {
            repo: repo.clone(),
            title: format!("{} {}", issue.identifier, issue.title),
            body_markdown: native_pr_handoff_body(issue, entry),
            head_branch: head_branch.clone(),
            base_branch,
            draft: false,
            existing_pr_match: ExistingPrMatch::HeadBranch,
            idempotency_key: IdempotencyKey(format!("{}:native-pr-handoff", issue.identifier)),
        }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubCreatePr(output),
                ..
            } => output.pull_request,
            FugueToolResult::Err { error, .. } => {
                return Err(FugueError::ResponseError(error.message));
            }
            FugueToolResult::Ok { output, .. } => {
                return Err(FugueError::ResponseError(format!(
                    "github.create_pr returned unexpected output {output:?}"
                )));
            }
        };
        self.record_native_pr_handoff(issue, entry, &pull_request)
            .await?;
        Ok(pull_request)
    }

    async fn record_native_pr_handoff(
        &self,
        issue: &Issue,
        entry: &RunningEntry,
        pull_request: &GitHubPullRequestSnapshot,
    ) -> Result<()> {
        crate::pr_handoff::record(self.config.effective(), issue, pull_request)?;
        let adapter = IssueAdapter::from_config(&self.config.effective().tracker)?;
        let issue_ref = issue_ref_for_identifier(&issue.identifier);
        let body = adapter
            .workpad_body(&issue_ref, "## Fugue Workpad")
            .await?
            .unwrap_or_else(|| "## Fugue Workpad".to_string());
        let body = append_native_pr_handoff_evidence(&body, entry, pull_request);
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: issue_ref,
                heading: "## Fugue Workpad".to_string(),
                body_markdown: body,
                create_if_missing: true,
                idempotency_key: IdempotencyKey(format!(
                    "{}:native-pr-handoff-workpad",
                    issue.identifier
                )),
            })
            .await?;
        Ok(())
    }

    async fn record_detected_native_pr_handoff(&self, handoff: DetectedNativePrHandoff) {
        if let Err(error) = crate::pr_handoff::record(
            self.config.effective(),
            &handoff.issue,
            &handoff.pull_request,
        ) {
            self.state.write().await.last_error = Some(error.to_string());
            return;
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                let mut state = self.state.write().await;
                state.last_error = Some(format!(
                    "native detected PR handoff could not load issue adapter for {}: {err}",
                    handoff.issue.identifier
                ));
                return;
            }
        };
        let issue_ref = issue_ref_for_identifier(&handoff.issue.identifier);
        if !handoff.workpad_has_pr {
            let body = match adapter.workpad_body(&issue_ref, "## Fugue Workpad").await {
                Ok(body) => body.unwrap_or_else(|| "## Fugue Workpad".to_string()),
                Err(err) => {
                    let mut state = self.state.write().await;
                    state.last_error = Some(format!(
                        "native detected PR handoff could not fetch Workpad for {}: {err}",
                        handoff.issue.identifier
                    ));
                    return;
                }
            };
            let body = append_native_pr_handoff_evidence_for_run(
                &body,
                Some(&handoff.run_id),
                &handoff.pull_request,
            );
            if let Err(err) = adapter
                .upsert_workpad(UpsertWorkpadInput {
                    issue: issue_ref.clone(),
                    heading: "## Fugue Workpad".to_string(),
                    body_markdown: body,
                    create_if_missing: true,
                    idempotency_key: IdempotencyKey(format!(
                        "{}:detected-native-pr-handoff-workpad",
                        handoff.issue.identifier
                    )),
                })
                .await
            {
                let mut state = self.state.write().await;
                state.last_error = Some(format!(
                    "native detected PR handoff could not update Workpad for {}: {err}",
                    handoff.issue.identifier
                ));
                return;
            }
        }
        info!(
            issue_identifier = %handoff.issue.identifier,
            pull_request = %handoff.pull_request.url,
            "native recorded existing pull request handoff evidence"
        );
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "explicit host composition boundary"
    )]
    async fn record_native_pr_handoff_monitor(
        &self,
        adapter: &IssueAdapter,
        issue: &Issue,
        workpad_body: &str,
        github: &RecoveredGitHubEvidence,
        ci: Option<&CheckCiOutput>,
        action: &NativePrHandoffMonitorAction,
        final_gate_start: Option<&FinalPrGateStartRequest>,
    ) -> Result<bool> {
        if github.rejected_pull_requests.is_empty() {
            crate::pr_handoff::record(self.config.effective(), issue, &github.pull_request)?;
        }
        let body = append_native_pr_handoff_monitor_evidence(
            workpad_body,
            github,
            ci,
            action,
            final_gate_start,
        );
        if body == workpad_body {
            return Ok(false);
        }
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: issue_ref_for_identifier(&issue.identifier),
                heading: "## Fugue Workpad".to_string(),
                body_markdown: body,
                create_if_missing: true,
                idempotency_key: IdempotencyKey(format!(
                    "{}:native-pr-handoff-monitor-workpad",
                    issue.identifier
                )),
            })
            .await?;
        Ok(true)
    }

    async fn deliver_native_final_pr_gate_request_to_kapellmeister(
        &self,
        issue: &Issue,
        final_gate_start: &FinalPrGateStartRequest,
    ) -> Result<bool> {
        self.deliver_kapellmeister_shadow_signal(
            KapellmeisterShadowSignal::final_pr_gate_requested(
                Utc::now(),
                None,
                issue_ref_for_identifier(&issue.identifier),
                final_gate_start.pr.clone(),
                final_gate_start.clone(),
            ),
        );
        info!(
            issue_identifier = %issue.identifier,
            pull_request = final_gate_start.pr.number.0,
            head_sha = %final_gate_start.pr.head_sha.0,
            trigger_mode = ?final_gate_start.trigger_mode,
            "delivered final PR gate request observation to repo Kapellmeister"
        );
        Ok(true)
    }

    fn deliver_native_pr_handoff_monitor_signal_to_kapellmeister(
        &self,
        issue: &Issue,
        handoff: &RecoveredGitHubEvidence,
        ci: Option<&CheckCiOutput>,
        action: NativePrHandoffMonitorAction,
    ) {
        let (decision, detail) = native_pr_handoff_monitor_signal_decision(action);
        self.deliver_kapellmeister_shadow_signal(
            KapellmeisterShadowSignal::pr_handoff_monitor_changed(
                Utc::now(),
                None,
                issue_ref_for_identifier(&issue.identifier),
                handoff.pull_request.clone(),
                ci.map(|output| output.status),
                decision,
                detail.to_string(),
            ),
        );
        info!(
            issue_identifier = %issue.identifier,
            pull_request = handoff.pull_request.number.0,
            head_sha = %handoff.pull_request.head_sha.0,
            ci_status = ?ci.map(|output| output.status),
            decision = ?decision,
            "delivered PR handoff monitor observation to repo Kapellmeister"
        );
    }

    #[cfg(test)]
    async fn start_native_final_pr_gate_from_kapellmeister_decision(
        &self,
        issue: &Issue,
        final_gate_start: &FinalPrGateStartRequest,
    ) -> Result<bool> {
        let mut executor = OrchestratorFinalPrGateStartExecutor {
            native_execution_host: Arc::clone(&self.native_execution_host),
            config: self.config.effective().clone(),
            issue: issue.clone(),
        };
        let authority = &self.config.effective().kapellmeister.shadow_mode.authority;
        let context =
            kapellmeister_context_for_issue(issue, Some(final_gate_start.pr.repo.clone()));
        let trigger_mode = final_gate_start.trigger_mode;
        let reviewer_count = final_gate_start.reviewers.len();
        let idempotency_key = final_gate_start.spawn_input.idempotency_key.clone();
        let response = KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId(format!(
                "final-pr-gate-start:{}:{}",
                issue.identifier, final_gate_start.pr.head_sha.0
            )),
            decided_at: Utc::now(),
            decision: KapellmeisterHostDecision::FinalPrGateStart {
                request: final_gate_start.clone(),
            },
            rationale: Some(
                "Kapellmeister-owned repo-level final PR gate start/resume".to_string(),
            ),
        };
        let outcome =
            execute_guarded_kapellmeister_decision(authority, &context, response, &mut executor)
                .await?;
        if matches!(
            outcome.mode,
            crate::kapellmeister_host::KapellmeisterExecutionMode::ShadowOnly { .. }
        ) {
            return Err(FugueError::ResponseError(
                "Kapellmeister guarded authority did not execute final PR gate start".to_string(),
            ));
        }
        let Some(result) = outcome.result else {
            return Err(FugueError::ResponseError(
                "Kapellmeister final PR gate start produced no host tool result".to_string(),
            ));
        };
        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(output),
                ..
            } => {
                info!(
                    issue_identifier = %issue.identifier,
                    pull_request = final_gate_start.pr.number.0,
                    head_sha = %final_gate_start.pr.head_sha.0,
                    trigger_mode = ?trigger_mode,
                    idempotency_key = %idempotency_key.0,
                    reviewer_count,
                    run_id = %output.run.run_id.0,
                    spawned = output.spawned,
                    resumed = output.resumed,
                    "Kapellmeister-owned final PR gate start/resume invoked through native Meta execution"
                );
                Ok(output.spawned || output.resumed)
            }
            FugueToolResult::Ok { output, .. } => Err(FugueError::ResponseError(format!(
                "final PR gate start returned unexpected output {output:?}"
            ))),
            FugueToolResult::Err { error, .. } => Err(FugueError::ResponseError(error.message)),
        }
    }

    async fn record_native_liveness_monitor(
        &self,
        issue: &Issue,
        reason: NativeRunParkReason,
        detail: Option<&str>,
    ) -> Result<bool> {
        let adapter = IssueAdapter::from_config(&self.config.effective().tracker)?;
        let issue_ref = issue_ref_for_identifier(&issue.identifier);
        let body = adapter
            .workpad_body(&issue_ref, "## Fugue Workpad")
            .await?
            .unwrap_or_else(|| "## Fugue Workpad".to_string());
        let updated = append_native_liveness_monitor_evidence(&body, reason, detail);
        if updated == body {
            return Ok(false);
        }
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: issue_ref,
                heading: "## Fugue Workpad".to_string(),
                body_markdown: updated,
                create_if_missing: true,
                idempotency_key: IdempotencyKey(format!(
                    "{}:native-liveness-monitor-workpad",
                    issue.identifier
                )),
            })
            .await?;
        Ok(true)
    }

    async fn record_native_host_blocked(&self, issue: &Issue, detail: &str) -> Result<bool> {
        let adapter = IssueAdapter::from_config(&self.config.effective().tracker)?;
        let issue_ref = issue_ref_for_identifier(&issue.identifier);
        let body = adapter
            .workpad_body(&issue_ref, "## Fugue Workpad")
            .await?
            .unwrap_or_else(|| "## Fugue Workpad".to_string());
        let updated = append_native_host_blocked_evidence(&body, detail);
        if updated == body {
            return Ok(false);
        }
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: issue_ref,
                heading: "## Fugue Workpad".to_string(),
                body_markdown: updated,
                create_if_missing: true,
                idempotency_key: IdempotencyKey(format!(
                    "{}:native-host-blocked-workpad",
                    issue.identifier
                )),
            })
            .await?;
        Ok(true)
    }

    async fn block_native_issue_for_host_failure(
        &self,
        issue: &Issue,
        detail: String,
        idempotency_suffix: &str,
    ) {
        let blocked_lane = native_liveness_blocked_lane(self.config.effective());
        let workpad_error = match self.record_native_host_blocked(issue, &detail).await {
            Ok(_) => None,
            Err(err) => Some(err.to_string()),
        };
        let lane_moved = self
            .move_issue_to_native_lane_host_mechanism(
                &issue.identifier,
                &blocked_lane,
                idempotency_suffix,
            )
            .await;
        let mut message = format!(
            "native host blocked {} in {}: {}",
            issue.identifier,
            blocked_lane,
            native_liveness_detail_for_workpad(&detail)
        );
        if !lane_moved {
            message.push_str("; blocked-lane move failed");
        }
        if let Some(workpad_error) = workpad_error {
            message.push_str("; Workpad evidence update failed: ");
            message.push_str(&workpad_error);
        }
        let mut state = self.state.write().await;
        state.claimed.remove(&issue.id);
        state.completed.insert(issue.id.clone());
        state.last_error = Some(message);
    }

    async fn park_native_issue_for_liveness(
        &self,
        issue_id: String,
        entry: RunningEntry,
        reason: NativeRunParkReason,
        detail: Option<String>,
    ) {
        let observed_at = Utc::now();
        self.deliver_kapellmeister_shadow_signal(native_liveness_blocked_signal(
            &entry.identifier,
            reason,
            detail.as_deref(),
            entry.retry_attempt.unwrap_or(0),
            observed_at,
        ));
        warn!(
            issue_identifier = %entry.identifier,
            reason = ?reason,
            detail = detail.as_deref().unwrap_or(""),
            "parking native issue after non-retryable liveness failure"
        );
        let _ = self.cleanup_native_execution_for_entry(&entry, "native-liveness-park");
        let workpad_error = match self
            .record_native_liveness_monitor(&entry.issue, reason, detail.as_deref())
            .await
        {
            Ok(_) => None,
            Err(err) => Some(err.to_string()),
        };
        let parked_lane = native_liveness_blocked_lane(self.config.effective());
        let lane_moved = self
            .project_issue_to_native_lane(&entry.issue, &parked_lane, "native-liveness-park")
            .await;
        let mut message = format!(
            "native liveness parked {} in {}: {}",
            entry.identifier,
            parked_lane,
            native_run_park_reason(reason)
        );
        if let Some(detail) = detail.as_deref() {
            message.push_str("; detail=");
            message.push_str(&native_liveness_detail_for_workpad(detail));
        }
        if !lane_moved {
            message.push_str("; Linear parked-lane move failed");
        }
        if let Some(workpad_error) = workpad_error {
            message.push_str("; Workpad evidence update failed: ");
            message.push_str(&workpad_error);
        }
        let mut state = self.state.write().await;
        state.completed.insert(issue_id);
        state.last_error = Some(message);
    }

    fn cleanup_native_execution_for_entry(
        &self,
        entry: &RunningEntry,
        reason: &str,
    ) -> std::result::Result<usize, String> {
        let Some(native_run) = entry.native_run.as_ref() else {
            return Ok(0);
        };
        match self
            .native_execution_host
            .cleanup_execution(&native_run.run_id, reason)
        {
            Ok(0) => Ok(0),
            Ok(terminated) => {
                info!(
                    issue_identifier = %entry.identifier,
                    run_id = %native_run.run_id.0,
                    reason = %reason,
                    terminated = terminated,
                    "native execution cleanup retired Meta members"
                );
                Ok(terminated)
            }
            Err(err) => {
                warn!(
                    issue_identifier = %entry.identifier,
                    run_id = %native_run.run_id.0,
                    reason = %reason,
                    error = %err,
                    "native execution cleanup failed"
                );
                Err(err)
            }
        }
    }

    async fn park_running_native_issue_for_liveness(
        &self,
        issue_id: &str,
        reason: NativeRunParkReason,
        detail: Option<String>,
    ) -> bool {
        {
            let state = self.state.read().await;
            if let Some(entry) = state.running.get(issue_id)
                && matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister)
            {
                warn!(
                    issue_id = %issue_id,
                    issue_identifier = %entry.identifier,
                    reason = ?reason,
                    detail = detail.as_deref().unwrap_or(""),
                    "kapellmeister-mode liveness parking suppressed; host delivered observation only"
                );
                return true;
            }
        }
        let removed = {
            let mut state = self.state.write().await;
            state.running.remove(issue_id).inspect(|entry| {
                state.claimed.remove(issue_id);
                state.ended_runtime_seconds +=
                    (Utc::now() - entry.started_at).num_milliseconds().max(0) as f64 / 1000.0;
            })
        };
        let Some(entry) = removed else {
            return false;
        };
        self.park_native_issue_for_liveness(issue_id.to_string(), entry, reason, detail)
            .await;
        true
    }

    async fn refresh_native_run_linear_recovery_for_terminal_action(
        &self,
        entry: &mut RunningEntry,
        issue: &Issue,
    ) {
        let Some(native_run) = entry.native_run.as_mut() else {
            return;
        };
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %entry.identifier,
                    run_id = %native_run.run_id.0,
                    "native terminal action used cached Linear evidence because issue adapter is unavailable"
                );
                return;
            }
        };
        match adapter
            .workpad_body(
                &issue_ref_for_identifier(&entry.identifier),
                "## Fugue Workpad",
            )
            .await
        {
            Ok(workpad_body) => {
                refresh_native_run_linear_recovery_from_workpad(
                    native_run,
                    issue,
                    workpad_body.as_deref(),
                );
            }
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %entry.identifier,
                    run_id = %native_run.run_id.0,
                    "native terminal action used cached Linear evidence because Workpad refresh failed"
                );
            }
        }
    }

    async fn repair_native_compact_lane_drift_for_issue(
        &self,
        issue: &Issue,
        native_run: Option<&NativeRunHandle>,
    ) -> Option<Issue> {
        let target_lane = native_compact_lane_repair_target(
            self.config.effective(),
            issue,
            native_run?,
            MobRunStatus::Running,
        )?;
        warn!(
            issue_identifier = %issue.identifier,
            from_state = %issue.state,
            to_state = %target_lane,
            "repairing native compact lane drift caused by external Linear state sync"
        );
        if !self
            .project_issue_to_native_lane(issue, &target_lane, "native-compact-lane-repair")
            .await
        {
            return None;
        }

        let mut repaired = issue.clone();
        repaired.state = target_lane;
        Some(repaired)
    }

    async fn project_issue_to_native_lane(
        &self,
        issue: &Issue,
        lane: &str,
        idempotency_suffix: &str,
    ) -> bool {
        let config = self.config.effective().clone();
        let policy_mode = classify_issue_policy_mode(issue, &config);
        if matches!(policy_mode, IssuePolicyMode::Kapellmeister)
            && !kapellmeister_host_projection_repair_allowed(idempotency_suffix)
        {
            warn!(
                issue_identifier = %issue.identifier,
                requested_lane = %lane,
                reason = %idempotency_suffix,
                "kapellmeister-mode Linear projection suppressed; host delivered observation only"
            );
            return false;
        }
        self.move_issue_to_native_lane_host_mechanism(&issue.identifier, lane, idempotency_suffix)
            .await
    }

    async fn move_issue_to_native_lane_host_mechanism(
        &self,
        identifier: &str,
        lane: &str,
        idempotency_suffix: &str,
    ) -> bool {
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %identifier,
                    target_lane = %lane,
                    "native compact lane transition skipped because issue adapter is unavailable"
                );
                return false;
            }
        };
        let result = adapter
            .move_issue(MoveIssueInput {
                issue: issue_ref_for_identifier(identifier),
                state: LinearStateRef::Name {
                    name: lane.to_string(),
                },
                expected_updated_at: None,
                idempotency_key: IdempotencyKey(format!(
                    "{idempotency_suffix}:{identifier}:{lane}"
                )),
            })
            .await;
        match result {
            Ok(output) => {
                if native_lane_move_satisfied(&output) {
                    self.record_internal_linear_lane(identifier, &output.issue.state.name)
                        .await;
                    true
                } else {
                    false
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %identifier,
                    target_lane = %lane,
                    "native compact lane transition failed"
                );
                false
            }
        }
    }

    async fn record_internal_linear_lane(&self, identifier: &str, lane: &str) {
        let result = record_internal_lane_for_workflow(
            &self.config.effective().workflow_dir,
            identifier,
            lane,
        );
        if let Err(err) = result {
            let mut state = self.state.write().await;
            state.last_error = Some(err.to_string());
            warn!(
                error = %err,
                issue_identifier = %identifier,
                lane = %lane,
                "failed to record internal Linear lane"
            );
        }
    }

    async fn terminate_running_issue(
        &mut self,
        issue_id: &str,
        cleanup_workspace: bool,
        retry: bool,
        reason: &str,
        authority: TerminationAuthority,
    ) {
        {
            let state = self.state.read().await;
            if let Some(entry) = state.running.get(issue_id)
                && matches!(entry.policy_mode, IssuePolicyMode::Kapellmeister)
                && !authority.permits_kapellmeister_termination()
            {
                warn!(
                    issue_id = %issue_id,
                    issue_identifier = %entry.identifier,
                    cleanup_workspace,
                    retry,
                    reason,
                    authority = ?authority,
                    "kapellmeister-mode termination suppressed; host delivered observation only"
                );
                return;
            }
        }
        let target = {
            let mut state = self.state.write().await;
            state.claimed.remove(issue_id);
            if let Some(entry) = state.running.get_mut(issue_id) {
                entry.termination = Some(TerminationPlan {
                    cleanup_workspace,
                    retry,
                    reason: reason.to_string(),
                });
                if let Some(native_run) = entry.native_run.clone() {
                    if let Some(handle) = entry.handle.take() {
                        Some(RunningTerminationTarget::NativeWorker {
                            identifier: entry.identifier.clone(),
                            session_id: entry.session.session_id.clone().unwrap_or_default(),
                            run_id: native_run.run_id,
                            cancel: entry.cancel.clone(),
                            handle,
                        })
                    } else {
                        Some(RunningTerminationTarget::Native {
                            identifier: entry.identifier.clone(),
                            session_id: entry.session.session_id.clone().unwrap_or_default(),
                            run_id: native_run.run_id,
                        })
                    }
                } else {
                    return;
                }
            } else {
                None
            }
        };
        let Some(target) = target else {
            return;
        };
        let (identifier, session_id) = match target {
            RunningTerminationTarget::NativeWorker {
                identifier,
                session_id,
                run_id,
                cancel,
                mut handle,
            } => {
                let _ = self
                    .native_execution_host
                    .cancel_execution(CancelExecutionInput {
                        run_id,
                        reason: reason.to_string(),
                        idempotency_key: IdempotencyKey(format!(
                            "native-execution-cancel:{issue_id}:{reason}"
                        )),
                    });
                cancel.cancel();
                let shutdown_timeout = self.worker_shutdown_timeout();
                match tokio::time::timeout(shutdown_timeout, &mut handle).await {
                    Ok(Ok(())) => {
                        info!(
                            issue_id = %issue_id,
                            issue_identifier = %identifier,
                            session_id = %session_id,
                            reason = reason,
                            "native worker graceful_shutdown completed"
                        );
                    }
                    Ok(Err(err)) => {
                        warn!(
                            issue_id = %issue_id,
                            issue_identifier = %identifier,
                            session_id = %session_id,
                            reason = reason,
                            error = %err,
                            "native worker graceful_shutdown join_failed"
                        );
                    }
                    Err(_) => {
                        warn!(
                            issue_id = %issue_id,
                            issue_identifier = %identifier,
                            session_id = %session_id,
                            reason = reason,
                            timeout_ms = shutdown_timeout.as_millis(),
                            "native worker graceful_shutdown timed_out"
                        );
                        handle.abort();
                        let _ = handle.await;
                    }
                }
                (identifier, session_id)
            }
            RunningTerminationTarget::Native {
                identifier,
                session_id,
                run_id,
            } => {
                let _ = self
                    .native_execution_host
                    .cancel_execution(CancelExecutionInput {
                        run_id,
                        reason: reason.to_string(),
                        idempotency_key: IdempotencyKey(format!(
                            "native-execution-cancel:{issue_id}:{reason}"
                        )),
                    });
                (identifier, session_id)
            }
        };
        self.drain_internal_messages().await;
        let orphaned_entry = {
            let mut state = self.state.write().await;
            let entry = state.running.remove(issue_id);
            if let Some(entry) = &entry {
                state.ended_runtime_seconds +=
                    (Utc::now() - entry.started_at).num_milliseconds().max(0) as f64 / 1000.0;
            }
            entry
        };
        if let Some(entry) = orphaned_entry
            && let Some(plan) = entry.termination.clone()
        {
            self.apply_termination_plan(issue_id, entry, plan).await;
        }
        info!(
            issue_id = %issue_id,
            issue_identifier = %identifier,
            session_id = %session_id,
            reason = reason,
            "running issue terminated"
        );
    }

    async fn drain_internal_messages(&mut self) {
        while let Ok(message) = self.internal_rx.try_recv() {
            self.handle_internal(message).await;
        }
    }

    async fn handle_internal(&mut self, message: InternalMessage) {
        match message {
            InternalMessage::Worker(message) => match *message {
                WorkerMessage::AgentUpdate { issue_id, update } => {
                    self.apply_agent_update(&issue_id, *update).await;
                }
                WorkerMessage::WorkerFinished { issue_id, outcome } => {
                    self.handle_worker_finished(issue_id, *outcome).await;
                }
                WorkerMessage::ShadowSignals {
                    issue_id: _,
                    signals,
                } => {
                    self.deliver_kapellmeister_shadow_signals(signals);
                }
            },
            InternalMessage::RetryDue { issue_id } => self.handle_retry_due(issue_id).await,
            InternalMessage::KapellmeisterStartIssue {
                issue_identifier,
                response,
            } => {
                let result = self
                    .handle_kapellmeister_start_issue(issue_identifier)
                    .await;
                let _ = response.send(result);
            }
            InternalMessage::KapellmeisterStartFinalPrGate { request, response } => {
                let result = self.handle_kapellmeister_start_final_pr_gate(request).await;
                let _ = response.send(result);
            }
            InternalMessage::KapellmeisterStartIndependentReview { request, response } => {
                let result = self
                    .handle_kapellmeister_start_independent_review(request)
                    .await;
                let _ = response.send(result);
            }
        }
    }

    async fn handle_kapellmeister_start_final_pr_gate(
        &self,
        mut request: FinalPrGateStartRequest,
    ) -> KapellmeisterStartIssueResult {
        let issue_identifier =
            issue_identifier_from_ref(&request.issue).unwrap_or_else(|| "unknown".to_string());
        let config = self.config.effective();
        if !config.final_review.enabled {
            return KapellmeisterStartIssueResult {
                issue_identifier,
                accepted: false,
                message: "Final review is disabled for this repository.".into(),
            };
        }
        request.reviewers = crate::final_pr_gate::final_reviewer_member_specs(&config.final_review);
        request.trigger_mode = config.final_review.mode;
        request.spawn_input.instructions =
            Some(crate::final_pr_gate::final_pr_gate_reviewer_instructions(
                &config.final_review,
                &request.issue,
                &request.pr,
                request.trigger_mode,
            ));
        let issue = Issue {
            id: issue_identifier.clone(),
            identifier: issue_identifier.clone(),
            title: format!("Final PR gate for PR #{}", request.pr.number.0),
            description: None,
            priority: None,
            state: self.config.effective().final_review.lane.clone(),
            branch_name: Some(request.pr.head_branch.0.clone()),
            url: Some(request.pr.url.clone()),
            labels: Vec::new(),
            blocked_by: Vec::new(),
            attachments: Vec::new(),
            created_at: None,
            updated_at: None,
        };
        let mut executor = OrchestratorFinalPrGateStartExecutor {
            native_execution_host: Arc::clone(&self.native_execution_host),
            config: self.config.effective().clone(),
            issue: issue.clone(),
        };
        let response = KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId(format!(
                "final-pr-gate-start:{}:{}",
                issue.identifier, request.pr.head_sha.0
            )),
            decided_at: Utc::now(),
            decision: KapellmeisterHostDecision::FinalPrGateStart {
                request: request.clone(),
            },
            rationale: Some(
                "Repo Kapellmeister requested final PR gate start through production host surface"
                    .to_string(),
            ),
        };
        match execute_guarded_kapellmeister_decision(
            &self.config.effective().kapellmeister.shadow_mode.authority,
            &kapellmeister_context_for_issue(&issue, Some(request.pr.repo.clone())),
            response,
            &mut executor,
        )
        .await
        {
            Ok(outcome) => match outcome.result {
                Some(FugueToolResult::Ok {
                    output: FugueToolOutput::MobSpawnOrResumeExecution(output),
                    ..
                }) => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: true,
                    message: format!(
                        "Kapellmeister-authorized final PR gate start/resume accepted: run_id={}, spawned={}, resumed={}",
                        output.run.run_id.0, output.spawned, output.resumed
                    ),
                },
                Some(FugueToolResult::Err { error, .. }) => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: format!("final PR gate start tool failed: {}", error.message),
                },
                Some(FugueToolResult::Ok { output, .. }) => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: format!("final PR gate start returned unexpected output {output:?}"),
                },
                None => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: "Kapellmeister guarded authority did not execute final PR gate start"
                        .to_string(),
                },
            },
            Err(error) => KapellmeisterStartIssueResult {
                issue_identifier,
                accepted: false,
                message: format!("final PR gate start failed: {error}"),
            },
        }
    }

    async fn handle_kapellmeister_start_independent_review(
        &self,
        mut request: IndependentReviewStartRequest,
    ) -> KapellmeisterStartIssueResult {
        let issue_identifier =
            issue_identifier_from_ref(&request.issue).unwrap_or_else(|| "unknown".to_string());
        if !self.config.effective().independent_review.enabled {
            return KapellmeisterStartIssueResult {
                issue_identifier,
                accepted: false,
                message: "independent review lane is disabled by configuration".to_string(),
            };
        }
        request.orchestrator_profile = self
            .config
            .effective()
            .independent_review
            .orchestrator_profile
            .clone();
        if request
            .spawn_input
            .worktree_path
            .as_deref()
            .is_none_or(str::is_empty)
        {
            match WorkspaceManager::for_service(self.config.effective())
                .path_for_issue(&issue_identifier)
            {
                Ok(path) => {
                    request.spawn_input.worktree_path = Some(path.display().to_string());
                }
                Err(error) => {
                    return KapellmeisterStartIssueResult {
                        issue_identifier,
                        accepted: false,
                        message: format!(
                            "independent review start could not derive worktree_path: {error}"
                        ),
                    };
                }
            }
        }
        let issue = Issue {
            id: issue_identifier.clone(),
            identifier: issue_identifier.clone(),
            title: format!(
                "Independent review for PR #{}",
                request.pull_request.number.0
            ),
            description: None,
            priority: None,
            state: self.config.effective().independent_review.lane.clone(),
            branch_name: Some(request.pull_request.head_branch.0.clone()),
            url: Some(request.pull_request.url.clone()),
            labels: Vec::new(),
            blocked_by: Vec::new(),
            attachments: Vec::new(),
            created_at: None,
            updated_at: None,
        };
        let mut executor = OrchestratorFinalPrGateStartExecutor {
            native_execution_host: Arc::clone(&self.native_execution_host),
            config: self.config.effective().clone(),
            issue: issue.clone(),
        };
        let response = KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId(format!(
                "independent-review-start:{}:{}",
                issue.identifier, request.pull_request.head_sha.0
            )),
            decided_at: Utc::now(),
            decision: KapellmeisterHostDecision::IndependentReviewStart {
                request: request.clone(),
            },
            rationale: Some(
                "Repo Kapellmeister requested independent review start through production host surface"
                    .to_string(),
            ),
        };
        match execute_guarded_kapellmeister_decision(
            &self.config.effective().kapellmeister.shadow_mode.authority,
            &kapellmeister_context_for_issue(&issue, Some(request.pull_request.repo.clone())),
            response,
            &mut executor,
        )
        .await
        {
            Ok(outcome) => match outcome.result {
                Some(FugueToolResult::Ok { .. }) => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: true,
                    message:
                        "Kapellmeister-authorized independent review start/resume was accepted"
                            .to_string(),
                },
                Some(FugueToolResult::Err { error, .. }) => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: error.message,
                },
                None => KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: format!(
                        "independent review start was not executed: {:?}",
                        outcome.mode
                    ),
                },
            },
            Err(error) => KapellmeisterStartIssueResult {
                issue_identifier,
                accepted: false,
                message: error.to_string(),
            },
        }
    }

    async fn handle_kapellmeister_start_issue(
        &self,
        issue_identifier: String,
    ) -> KapellmeisterStartIssueResult {
        let config = self.config.effective().clone();
        let adapter = match IssueAdapter::from_config(&config.tracker) {
            Ok(adapter) => adapter,
            Err(error) => {
                return KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: format!("issue adapter unavailable: {error}"),
                };
            }
        };
        let issues = match adapter.fetch_open_project_issues().await {
            Ok(issues) => issues,
            Err(error) => {
                return KapellmeisterStartIssueResult {
                    issue_identifier,
                    accepted: false,
                    message: format!("failed to fetch open project issues: {error}"),
                };
            }
        };
        self.handle_kapellmeister_start_issue_from_issues(issue_identifier, issues, &config)
            .await
    }

    async fn handle_kapellmeister_start_issue_from_issues(
        &self,
        issue_identifier: String,
        issues: Vec<Issue>,
        config: &ServiceConfig,
    ) -> KapellmeisterStartIssueResult {
        let Some(issue) = issues
            .into_iter()
            .find(|issue| issue.identifier.eq_ignore_ascii_case(&issue_identifier))
        else {
            return KapellmeisterStartIssueResult {
                issue_identifier,
                accepted: false,
                message: "issue is not currently visible as an open Linear project issue"
                    .to_string(),
            };
        };
        if let Some(block_reason) = kapellmeister_start_issue_block_reason(config, &issue) {
            return KapellmeisterStartIssueResult {
                issue_identifier: issue.identifier,
                accepted: false,
                message: block_reason,
            };
        }
        if !native_kapellmeister_prompt_control_enabled(config)
            && !matches!(
                classify_issue_policy_mode(&issue, config),
                IssuePolicyMode::Kapellmeister
            )
        {
            return KapellmeisterStartIssueResult {
                issue_identifier: issue.identifier,
                accepted: false,
                message: "issue is not in Kapellmeister-controlled policy scope".to_string(),
            };
        }
        if !matches!(
            dispatch_backend_for_issue(&issue, config),
            DispatchBackend::NativeMob
        ) {
            return KapellmeisterStartIssueResult {
                issue_identifier: issue.identifier,
                accepted: false,
                message: "issue does not select a native execution mob backend".to_string(),
            };
        }
        {
            let state = self.state.read().await;
            if state.running.contains_key(&issue.id) || state.claimed.contains(&issue.id) {
                return KapellmeisterStartIssueResult {
                    issue_identifier: issue.identifier,
                    accepted: true,
                    message: "issue mob is already running or launching".to_string(),
                };
            }
        }
        if !self.has_available_slot_for(&issue, false).await {
            return KapellmeisterStartIssueResult {
                issue_identifier: issue.identifier,
                accepted: false,
                message: "no execution slot is currently available for this issue".to_string(),
            };
        }
        let issue_identifier = issue.identifier.clone();
        self.dispatch_native_execution_mob(
            issue,
            None,
            None,
            IssuePolicyMode::Kapellmeister,
            config.clone(),
        )
        .await;
        KapellmeisterStartIssueResult {
            issue_identifier,
            accepted: true,
            message: "Kapellmeister-authorized issue mob start/resume was accepted".to_string(),
        }
    }

    async fn apply_agent_update(&self, issue_id: &str, update: AgentUpdate) {
        let mut state = self.state.write().await;
        let mut token_delta = TokenTotals::default();
        let mut has_token_delta = false;
        if let Some(entry) = state.running.get_mut(issue_id) {
            {
                if let Some(session_id) = update.session_id.clone() {
                    entry.session.session_id = Some(session_id);
                }
                if let Some(session_ref) = update.session_ref.clone() {
                    entry.session.session_ref = Some(session_ref);
                }
                entry.session.active_turn_request_id = update.active_turn_request_id.clone();
                if let Some(stream_id) = update.stream_id.clone() {
                    entry.session.stream_id = Some(stream_id);
                }
                if let Some(sequence) = update.stream_sequence {
                    entry.session.last_stream_sequence = Some(sequence);
                }
                entry.session.stream_close_reason = update.stream_close_reason.clone();
                entry.session.stream_error = update.stream_error.clone();
                entry.session.meerkat_rpc_pid = update.meerkat_rpc_pid.clone();
                entry.session.last_meerkat_event = Some(update.event.clone());
                entry.session.last_meerkat_timestamp = Some(update.timestamp);
                entry.session.last_meerkat_message = update.message.clone();
                if let Some(turn_count) = update.turn_count {
                    entry.session.turn_count = turn_count;
                }
                if let Some(usage) = update.absolute_usage.clone() {
                    token_delta = TokenTotals {
                        input_tokens: usage
                            .input_tokens
                            .saturating_sub(entry.session.last_reported_tokens.input_tokens),
                        output_tokens: usage
                            .output_tokens
                            .saturating_sub(entry.session.last_reported_tokens.output_tokens),
                        total_tokens: usage
                            .total_tokens
                            .saturating_sub(entry.session.last_reported_tokens.total_tokens),
                    };
                    has_token_delta = true;
                    entry.session.last_reported_tokens = usage.clone();
                    entry.session.meerkat_tokens = usage;
                }
            }
        } else {
            return;
        }
        if has_token_delta {
            state.meerkat_totals.input_tokens += token_delta.input_tokens;
            state.meerkat_totals.output_tokens += token_delta.output_tokens;
            state.meerkat_totals.total_tokens += token_delta.total_tokens;
        }
        if let Some(rate_limits) = update.rate_limits.clone() {
            state.meerkat_rate_limits = Some(rate_limits);
        }
        state
            .recent_events
            .entry(issue_id.to_string())
            .or_default()
            .push(RecentEvent {
                at: update.timestamp,
                event: update.event,
                message: update.message,
            });
        if let Some(events) = state.recent_events.get_mut(issue_id)
            && events.len() > 50
        {
            let excess = events.len() - 50;
            events.drain(0..excess);
        }
    }

    async fn handle_worker_finished(&mut self, issue_id: String, outcome: WorkerOutcome) {
        let removed = {
            let mut state = self.state.write().await;
            state.running.remove(&issue_id).map(|mut entry| {
                state.ended_runtime_seconds +=
                    (Utc::now() - entry.started_at).num_milliseconds().max(0) as f64 / 1000.0;
                if entry.session.session_id.is_none() {
                    entry.session = outcome.session.clone();
                }
                (entry, outcome)
            })
        };
        let Some((entry, outcome)) = removed else {
            return;
        };
        let session_id_for_log = entry
            .session
            .session_id
            .as_deref()
            .or(outcome.session.session_id.as_deref())
            .unwrap_or("");
        match outcome.exit {
            WorkerExit::Normal => info!(
                issue_id = %issue_id,
                issue_identifier = %entry.identifier,
                session_id = session_id_for_log,
                "worker finished"
            ),
            _ => warn!(
                issue_id = %issue_id,
                issue_identifier = %entry.identifier,
                session_id = session_id_for_log,
                error = outcome.error.as_deref().unwrap_or(""),
                "worker finished"
            ),
        }
        let observed_at = Utc::now();
        self.deliver_kapellmeister_shadow_signal(worker_finished_signal(
            &entry.identifier,
            outcome.exit,
            outcome.error.clone(),
            entry.retry_attempt.unwrap_or(0).saturating_add(1),
            self.config
                .effective()
                .tracker
                .is_terminal_state(&outcome.final_issue.state),
            observed_at,
        ));
        match outcome.exit {
            WorkerExit::Normal => {
                if let Some(plan) = entry.termination.clone() {
                    self.apply_termination_plan(&issue_id, entry, plan).await;
                } else {
                    if self
                        .config
                        .effective()
                        .tracker
                        .is_terminal_state(&outcome.final_issue.state)
                    {
                        clean_workspace_for_issue_identifier_best_effort(
                            self.config.effective(),
                            &entry.identifier,
                        )
                        .await;
                    }
                    {
                        let mut state = self.state.write().await;
                        state.completed.insert(issue_id.clone());
                    }
                    self.schedule_retry(
                        issue_id,
                        entry.identifier,
                        entry.policy_mode,
                        1,
                        None,
                        true,
                    )
                    .await;
                }
            }
            WorkerExit::Failed
            | WorkerExit::TimedOut
            | WorkerExit::Stalled
            | WorkerExit::CanceledByReconciliation => {
                if let Some(plan) = entry.termination.clone() {
                    self.apply_termination_plan(&issue_id, entry, plan).await;
                } else {
                    let next_attempt = entry.retry_attempt.unwrap_or(0) + 1;
                    self.schedule_retry(
                        issue_id,
                        entry.identifier,
                        entry.policy_mode,
                        next_attempt,
                        outcome.error,
                        false,
                    )
                    .await;
                }
            }
        }
    }

    async fn apply_termination_plan(
        &self,
        issue_id: &str,
        entry: RunningEntry,
        plan: TerminationPlan,
    ) {
        if plan.cleanup_workspace {
            clean_workspace_for_issue_identifier_best_effort(
                self.config.effective(),
                &entry.identifier,
            )
            .await;
        }
        if plan.retry {
            self.schedule_retry(
                issue_id.to_string(),
                entry.identifier,
                entry.policy_mode,
                entry.retry_attempt.unwrap_or(0) + 1,
                Some(plan.reason),
                false,
            )
            .await;
        }
    }

    async fn handle_retry_due(&mut self, issue_id: String) {
        let dispatch_blocked = self.reload_workflow_if_needed().await;
        let retry_entry = {
            let mut state = self.state.write().await;
            state.retry_attempts.remove(&issue_id)
        };
        let Some(retry_entry) = retry_entry else {
            return;
        };
        if matches!(retry_entry.policy_mode, IssuePolicyMode::Kapellmeister) {
            let mut state = self.state.write().await;
            state.claimed.remove(&issue_id);
            return;
        }
        if dispatch_blocked {
            self.schedule_retry(
                issue_id,
                retry_entry.identifier,
                retry_entry.policy_mode,
                retry_entry.attempt + 1,
                Some("workflow reload failed; dispatch blocked".to_string()),
                false,
            )
            .await;
            return;
        }
        if let Err(err) = self.config.validate_dispatch() {
            let mut state = self.state.write().await;
            state.last_error = Some(err.to_string());
            drop(state);
            self.schedule_retry(
                issue_id,
                retry_entry.identifier,
                retry_entry.policy_mode,
                retry_entry.attempt + 1,
                Some(format!("dispatch validation failed: {err}")),
                false,
            )
            .await;
            return;
        }
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                self.schedule_retry(
                    issue_id,
                    retry_entry.identifier,
                    retry_entry.policy_mode,
                    retry_entry.attempt + 1,
                    Some(format!("retry poll failed: {err}")),
                    false,
                )
                .await;
                return;
            }
        };
        let candidates = match adapter.fetch_candidate_issues().await {
            Ok(issues) => issues,
            Err(err) => {
                self.schedule_retry(
                    issue_id,
                    retry_entry.identifier,
                    retry_entry.policy_mode,
                    retry_entry.attempt + 1,
                    Some(format!("retry poll failed: {err}")),
                    false,
                )
                .await;
                return;
            }
        };
        let Some(issue) = candidates.into_iter().find(|issue| issue.id == issue_id) else {
            let mut state = self.state.write().await;
            state.claimed.remove(&issue_id);
            return;
        };
        if !self.has_available_slot_for(&issue, true).await {
            self.schedule_retry(
                issue_id,
                issue.identifier,
                retry_entry.policy_mode,
                retry_entry.attempt + 1,
                Some("no available orchestrator slots".to_string()),
                false,
            )
            .await;
            return;
        }
        if self.should_dispatch(&issue, true).await {
            self.dispatch_issue_with_policy_and_native_generation(
                issue,
                Some(retry_entry.attempt),
                retry_entry.native_run_generation,
                true,
            )
            .await;
        } else {
            let mut state = self.state.write().await;
            state.claimed.remove(&issue_id);
        }
    }

    async fn schedule_native_rework_retry(
        &self,
        issue_id: String,
        identifier: String,
        policy_mode: IssuePolicyMode,
        attempt: u32,
    ) {
        self.schedule_retry_with_native_generation(
            issue_id,
            identifier,
            policy_mode,
            attempt,
            Some("native reviewer requested focused rework".to_string()),
            true,
            Some(attempt),
        )
        .await;
    }

    async fn schedule_retry(
        &self,
        issue_id: String,
        identifier: String,
        policy_mode: IssuePolicyMode,
        attempt: u32,
        error: Option<String>,
        continuation: bool,
    ) {
        self.schedule_retry_with_native_generation(
            issue_id,
            identifier,
            policy_mode,
            attempt,
            error,
            continuation,
            None,
        )
        .await;
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "explicit host composition boundary"
    )]
    async fn schedule_retry_with_native_generation(
        &self,
        issue_id: String,
        identifier: String,
        policy_mode: IssuePolicyMode,
        attempt: u32,
        error: Option<String>,
        continuation: bool,
        native_run_generation: Option<u32>,
    ) {
        if matches!(policy_mode, IssuePolicyMode::Kapellmeister) {
            let mut state = self.state.write().await;
            state.claimed.remove(&issue_id);
            if let Some(entry) = state.retry_attempts.remove(&issue_id) {
                entry.handle.abort()
            }
            warn!(
                issue_id = %issue_id,
                issue_identifier = %identifier,
                "kapellmeister-mode retry suppressed; host must not schedule work"
            );
            return;
        }
        let delay_ms = if continuation {
            1_000
        } else {
            let uncapped = 10_000u64.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
            uncapped.min(self.config.effective().agent.max_retry_backoff_ms)
        };
        let due_at = Utc::now()
            + chrono::Duration::from_std(Duration::from_millis(delay_ms))
                .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
        let tx = self.internal_tx.clone();
        let issue_for_timer = issue_id.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            let _ = tx
                .send(InternalMessage::RetryDue {
                    issue_id: issue_for_timer,
                })
                .await;
        });

        let mut state = self.state.write().await;
        if let Some(old) = state.retry_attempts.remove(&issue_id) {
            old.handle.abort();
        }
        state.claimed.insert(issue_id.clone());
        state.retry_attempts.insert(
            issue_id.clone(),
            RetryEntry {
                workspace_path: self
                    .config
                    .effective()
                    .workspace
                    .root
                    .join(sanitize_workspace_key(&identifier))
                    .display()
                    .to_string(),
                identifier,
                policy_mode,
                attempt,
                native_run_generation,
                due_at,
                error,
                handle,
            },
        );
    }

    async fn dispatch_issue(&self, issue: Issue, attempt: Option<u32>, allow_claimed: bool) {
        self.dispatch_issue_with_policy(issue, attempt, allow_claimed)
            .await;
    }

    async fn dispatch_issue_with_policy(
        &self,
        issue: Issue,
        attempt: Option<u32>,
        allow_claimed: bool,
    ) {
        self.dispatch_issue_with_policy_and_native_generation(issue, attempt, None, allow_claimed)
            .await;
    }

    async fn dispatch_issue_with_policy_and_native_generation(
        &self,
        issue: Issue,
        attempt: Option<u32>,
        native_run_generation: Option<u32>,
        allow_claimed: bool,
    ) {
        if !self.should_dispatch(&issue, allow_claimed).await {
            return;
        }
        let config = self.config.effective().clone();
        let policy_mode = classify_issue_policy_mode(&issue, &config);
        let backend = dispatch_backend_for_issue(&issue, &config);
        if matches!(policy_mode, IssuePolicyMode::Kapellmeister) {
            if !kapellmeister_observation_suppresses_dispatch(policy_mode, backend) {
                info!(
                    issue_id = %issue.id,
                    issue_identifier = %issue.identifier,
                    policy_mode = ?policy_mode,
                    "kapellmeister-mode native dispatch proceeding after observation delivery"
                );
            } else {
                let mut state = self.state.write().await;
                state.claimed.remove(&issue.id);
                info!(
                    issue_id = %issue.id,
                    issue_identifier = %issue.identifier,
                    policy_mode = ?policy_mode,
                    "kapellmeister-mode host dispatch suppressed after observation delivery"
                );
                return;
            }
        }
        if matches!(backend, DispatchBackend::NativeMob) {
            self.dispatch_native_execution_mob(
                issue,
                attempt,
                native_run_generation,
                policy_mode,
                config,
            )
            .await;
            return;
        }
        let cancel = CancellationToken::new();
        let worker_tx = self.internal_tx.clone();
        let issue_id = issue.id.clone();
        let identifier = issue.identifier.clone();
        let workspace_path = config
            .workspace
            .root
            .join(sanitize_workspace_key(&identifier))
            .display()
            .to_string();
        let issue_for_worker = issue.clone();
        let cancel_for_worker = cancel.clone();
        let handle = tokio::spawn(async move {
            let (tx, mut rx) = mpsc::channel(128);
            let forward = tokio::spawn({
                let worker_tx = worker_tx.clone();
                async move {
                    while let Some(message) = rx.recv().await {
                        let _ = worker_tx
                            .send(InternalMessage::Worker(Box::new(message)))
                            .await;
                    }
                }
            });
            run_agent_worker(issue_for_worker, attempt, config, tx, cancel_for_worker).await;
            let _ = forward.await;
        });

        let mut state = self.state.write().await;
        if let Some(entry) = state.retry_attempts.remove(&issue_id) {
            entry.handle.abort();
        }
        state.claimed.insert(issue_id.clone());
        state.running.insert(
            issue_id.clone(),
            RunningEntry {
                issue,
                identifier: identifier.clone(),
                policy_mode,
                workspace_path,
                session: LiveSession::default(),
                started_at: Utc::now(),
                retry_attempt: attempt,
                cancel,
                handle: Some(handle),
                termination: None,
                native_run: None,
            },
        );
        drop(state);
        let observed_at = Utc::now();
        self.deliver_kapellmeister_shadow_signal(KapellmeisterShadowSignal::worker_started(
            observed_at,
            Some(worker_cursor(
                &identifier,
                "started",
                attempt.unwrap_or(0).saturating_add(1),
                observed_at,
            )),
            issue_ref_for_identifier(&identifier),
            workspace_key_for_identifier(&identifier),
            attempt.unwrap_or(0).saturating_add(1),
            None,
        ));
        info!(issue_id = %issue_id, issue_identifier = %identifier, "dispatch started");
    }

    async fn dispatch_native_execution_mob(
        &self,
        mut issue: Issue,
        attempt: Option<u32>,
        native_run_generation: Option<u32>,
        policy_mode: IssuePolicyMode,
        config: ServiceConfig,
    ) {
        let issue_id_for_claim = issue.id.clone();
        match self
            .refresh_native_issue_before_launch(&issue, &config)
            .await
        {
            NativeLaunchRefreshAction::Continue(refreshed) => {
                issue = refreshed;
            }
            NativeLaunchRefreshAction::Stop { reason } => {
                let mut state = self.state.write().await;
                state.claimed.remove(&issue_id_for_claim);
                info!(
                    issue_id = %issue_id_for_claim,
                    issue_identifier = %issue.identifier,
                    reason = %reason,
                    "native execution launch stopped after fresh Linear state refresh"
                );
                return;
            }
        }
        if self
            .native_pr_handoff_blocks_fresh_dispatch(&issue, &config)
            .await
        {
            let mut state = self.state.write().await;
            state.claimed.remove(&issue.id);
            state.completed.insert(issue.id.clone());
            return;
        }
        let native_run_generation =
            native_run_generation_for_dispatch(&issue, &config, native_run_generation.or(attempt));
        let native_run_generation = self
            .next_native_run_generation_for_active_dispatch(&issue, native_run_generation)
            .await;
        let issue_id = issue.id.clone();
        let identifier = issue.identifier.clone();
        let tool_layer = FugueToolLayer::new(&config);
        let workspace = match tool_layer.create_workspace_for_issue(&issue).await {
            Ok(workspace) => workspace,
            Err(err) => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("workspace creation failed: {err}"),
                    "native-launch-workspace-blocked",
                )
                .await;
                return;
            }
        };
        if let Err(err) = tool_layer.run_repository_setup_for_issue(&issue).await {
            self.block_native_issue_for_host_failure(
                &issue,
                format!("repository setup failed: {err}"),
                "native-launch-repo-setup-blocked",
            )
            .await;
            return;
        }
        let workspace_path_buf = workspace.path.clone();
        let workspace_path = workspace_path_buf.display().to_string();
        let mut issue = match materialize_issue_attachments_for_native_launch(
            &config.tracker,
            &issue,
            &workspace_path_buf,
        )
        .await
        {
            Ok(issue) => issue,
            Err(err) => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("attachment materialization failed: {err}"),
                    "native-launch-attachments-blocked",
                )
                .await;
                return;
            }
        };
        let selected = match config.select_template_for_issue(&issue) {
            Ok(selected) => selected,
            Err(err) => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("template selection failed: {err}"),
                    "native-launch-template-blocked",
                )
                .await;
                return;
            }
        };
        let instructions = match config.render_native_execution_instructions(
            &issue,
            attempt,
            &workspace_path_buf,
            &selected.template_id,
            selected.template,
        ) {
            Ok(instructions) => instructions,
            Err(err) => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("native launch instruction rendering failed: {err}"),
                    "native-launch-instructions-blocked",
                )
                .await;
                return;
            }
        };
        let idempotency_key = native_execution_idempotency_key(&identifier, native_run_generation);
        let plan = match NativeMobLaunchPlan::new(
            SpawnOrResumeExecutionInput {
                issue: issue_ref_for_identifier(&identifier),
                workspace_key: workspace_key_for_identifier(&identifier),
                template_id: selected.template_id.clone(),
                worktree_path: Some(workspace_path.clone()),
                instructions: Some(instructions),
                idempotency_key,
            },
            selected.template.clone(),
            native_member_mcp_servers_for_issue(
                &self.config.effective().mob.mcp_servers,
                &self.config.effective().workflow_dir,
                selected.template,
                &issue,
                repository_scope_from_config(self.config.effective()).as_ref(),
                &self.config.effective().tracker.terminal_states,
                &native_issue_review_handoff_states(self.config.effective(), &issue),
                &native_action_lanes(self.config.effective()),
            ),
            native_member_direct_tool_servers(&self.config.effective().tracker, selected.template),
            self.config.effective().tracker.project_slug.clone(),
            repository_scope_from_config(self.config.effective()),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("native launch plan validation failed: {}", error.message),
                    "native-launch-plan-blocked",
                )
                .await;
                return;
            }
        };
        let issue_ref = issue_ref_for_identifier(&identifier);
        let result = self.native_execution_host.spawn_or_resume_plan(plan);
        let run = match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(output),
                ..
            } => output.run,
            FugueToolResult::Ok { output, .. } => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("native mob launch returned unexpected output {output:?}"),
                    "native-launch-output-blocked",
                )
                .await;
                return;
            }
            FugueToolResult::Err { error, .. } => {
                self.block_native_issue_for_host_failure(
                    &issue,
                    format!("native mob launch failed: {}", error.message),
                    "native-launch-failed-blocked",
                )
                .await;
                return;
            }
        };
        if !matches!(policy_mode, IssuePolicyMode::Kapellmeister)
            && is_native_ready_state(&config, &issue.state)
            && let Some(planning_lane) = projected_native_lane_for_node(&config, &issue, "planner")
            && !issue.state.eq_ignore_ascii_case(&planning_lane)
        {
            let lane_moved = self
                .project_issue_to_native_lane(
                    &issue,
                    &planning_lane,
                    "native-flow-dispatch-planning",
                )
                .await;
            if !lane_moved {
                warn!(
                    issue_identifier = %identifier,
                    target_lane = %planning_lane,
                    "native issue mob launched but Fugue could not immediately project the Linear lane"
                );
            }
            issue.state = planning_lane;
        }

        let cancel = CancellationToken::new();
        let handle = self.native_worker_execution.then(|| {
            let (tx, mut rx) = mpsc::channel(128);
            let worker_tx = self.internal_tx.clone();
            let issue_for_worker = issue.clone();
            let config_for_worker = config.clone();
            let cancel_for_worker = cancel.clone();
            tokio::spawn(async move {
                let forward = tokio::spawn({
                    let worker_tx = worker_tx.clone();
                    async move {
                        while let Some(message) = rx.recv().await {
                            let _ = worker_tx
                                .send(InternalMessage::Worker(Box::new(message)))
                                .await;
                        }
                    }
                });
                run_agent_worker(
                    issue_for_worker,
                    attempt,
                    config_for_worker,
                    tx,
                    cancel_for_worker,
                )
                .await;
                let _ = forward.await;
            })
        });
        let session = LiveSession {
            session_id: Some(run.run_id.0.clone()),
            last_meerkat_event: Some("native_mob_spawn_or_resume".to_string()),
            last_meerkat_timestamp: Some(Utc::now()),
            last_meerkat_message: Some(format!(
                "native execution mob {} is running for {}",
                run.run_id.0, identifier
            )),
            ..LiveSession::default()
        };
        let launch_workspace_evidence = RecoveredWorkspaceEvidence {
            path: workspace_path.clone(),
            branch: issue.branch_name.clone().map(BranchName),
            head_sha: None,
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };
        let existing_recovery = NativeStatusRecovery {
            workspace: Some(launch_workspace_evidence),
            ..NativeStatusRecovery::default()
        };
        let workpad_body = if is_native_ready_state(&config, &issue.state) {
            None
        } else {
            self.fetch_native_workpad_body(
                &issue_ref,
                &identifier,
                &run.run_id,
                "native launch recovery",
            )
            .await
        };
        let workspace_manager = WorkspaceManager::for_service(&config);
        let github_host =
            GitHubToolHost::with_connections(config.meerkat.runtime_connections.clone());
        let recovery = refresh_native_status_recovery(
            &run,
            &issue,
            &config,
            &workspace_manager,
            &[],
            &existing_recovery,
            workpad_body.as_deref(),
            Some(&github_host),
        );
        let mut state = self.state.write().await;
        if let Some(entry) = state.retry_attempts.remove(&issue_id) {
            entry.handle.abort();
        }
        state.claimed.insert(issue_id.clone());
        state.running.insert(
            issue_id.clone(),
            RunningEntry {
                issue,
                identifier: identifier.clone(),
                policy_mode,
                workspace_path,
                session,
                started_at: Utc::now(),
                retry_attempt: native_run_generation.or(attempt),
                cancel,
                handle,
                termination: None,
                native_run: Some(NativeRunHandle {
                    run_id: run.run_id.clone(),
                    observed_events: Vec::new(),
                    recovery,
                }),
            },
        );
        drop(state);

        let observed_at = Utc::now();
        self.deliver_kapellmeister_shadow_signal(KapellmeisterShadowSignal::worker_started(
            observed_at,
            Some(worker_cursor(
                &identifier,
                "native_mob_started",
                attempt.unwrap_or(0).saturating_add(1),
                observed_at,
            )),
            issue_ref_for_identifier(&identifier),
            workspace_key_for_identifier(&identifier),
            attempt.unwrap_or(0).saturating_add(1),
            None,
        ));
        info!(
            issue_id = %issue_id,
            issue_identifier = %identifier,
            run_id = %run.run_id.0,
            "native execution mob dispatch started"
        );
    }

    async fn refresh_native_issue_before_launch(
        &self,
        issue: &Issue,
        config: &ServiceConfig,
    ) -> NativeLaunchRefreshAction {
        let adapter = match IssueAdapter::from_config(&config.tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %issue.identifier,
                    "native execution launch state refresh skipped"
                );
                return NativeLaunchRefreshAction::Continue(issue.clone());
            }
        };
        match adapter
            .fetch_issue_states_by_ids(std::slice::from_ref(&issue.id))
            .await
        {
            Ok(mut issues) => {
                if let Some(refreshed) = issues.pop() {
                    native_launch_refresh_action(config, refreshed)
                } else {
                    NativeLaunchRefreshAction::Stop {
                        reason: "issue disappeared before native launch".to_string(),
                    }
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %issue.identifier,
                    "native execution launch state refresh failed; using cached issue state"
                );
                NativeLaunchRefreshAction::Continue(issue.clone())
            }
        }
    }

    async fn next_native_run_generation_for_active_dispatch(
        &self,
        issue: &Issue,
        requested_generation: Option<u32>,
    ) -> Option<u32> {
        if is_todo_state(&issue.state) {
            return requested_generation;
        }
        match self.native_execution_host.list_runs(ListRunsInput {
            issue: Some(issue_ref_for_identifier(&issue.identifier)),
            status: None,
            cursor: None,
            limit: 100,
        }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobListRuns(output),
                ..
            } => {
                if requested_generation.is_some()
                    && output
                        .runs
                        .iter()
                        .any(|run| native_run_status_is_active(run.status))
                {
                    return None;
                }
                if output.runs.is_empty() && requested_generation.is_none() {
                    return requested_generation;
                }
                let generation = next_native_run_generation_from_durable_runs(
                    &output.runs,
                    requested_generation,
                );
                self.next_native_run_generation_with_workpad(issue, generation)
                    .await
            }
            FugueToolResult::Ok { output, .. } => {
                warn!(
                    issue_identifier = %issue.identifier,
                    output = ?output,
                    "native active dispatch could not inspect durable run generations"
                );
                requested_generation
            }
            FugueToolResult::Err { error, .. } => {
                warn!(
                    issue_identifier = %issue.identifier,
                    message = %error.message,
                    "native active dispatch using requested generation because durable run inspection failed"
                );
                requested_generation
            }
        }
    }

    async fn next_native_run_generation_with_workpad(
        &self,
        issue: &Issue,
        requested_generation: Option<u32>,
    ) -> Option<u32> {
        let adapter = match IssueAdapter::from_config(&self.config.effective().tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %issue.identifier,
                    "native active dispatch workpad generation lookup skipped"
                );
                return requested_generation;
            }
        };
        match adapter
            .workpad_body(
                &issue_ref_for_identifier(&issue.identifier),
                "## Fugue Workpad",
            )
            .await
        {
            Ok(Some(body)) => {
                let workpad_generation = parse_workpad_native_run_id(&body)
                    .as_ref()
                    .map(native_run_generation_sort_key);
                next_native_run_generation_from_workpad(requested_generation, workpad_generation)
            }
            Ok(None) => requested_generation,
            Err(err) => {
                warn!(
                    issue_identifier = %issue.identifier,
                    error = %err,
                    "native active dispatch workpad generation lookup failed"
                );
                requested_generation
            }
        }
    }

    async fn native_pr_handoff_blocks_fresh_dispatch(
        &self,
        issue: &Issue,
        config: &ServiceConfig,
    ) -> bool {
        if is_native_ready_state(config, &issue.state) {
            return false;
        }
        let adapter = match IssueAdapter::from_config(&config.tracker) {
            Ok(adapter) => adapter,
            Err(err) => {
                warn!(
                    error = %err,
                    issue_identifier = %issue.identifier,
                    "native PR handoff dispatch guard skipped"
                );
                return false;
            }
        };
        let workpad_body = match adapter
            .workpad_body(
                &issue_ref_for_identifier(&issue.identifier),
                "## Fugue Workpad",
            )
            .await
        {
            Ok(workpad_body) => workpad_body,
            Err(err) => {
                warn!(
                    issue_identifier = %issue.identifier,
                    message = %err,
                    "native PR handoff dispatch guard could not fetch Workpad"
                );
                return false;
            }
        };
        let linear = recover_native_linear_evidence(issue, workpad_body.as_deref());
        if native_pr_handoff_blocks_fresh_dispatch_for_evidence(config, issue, &linear, false) {
            self.repair_native_pr_handoff_projection_lane(issue).await;
            info!(
                issue_identifier = %issue.identifier,
                issue_state = %issue.state,
                "native issue dispatch skipped because Workpad PR handoff is awaiting review/check feedback"
            );
            return true;
        }
        if linear.reviewer_rework_requested {
            return false;
        }
        if let Some(github) =
            recover_existing_native_pr_handoff_for_issue(config, issue, Some(&linear))
        {
            if !native_pr_handoff_waiting_state(config, issue) {
                return false;
            }
            self.repair_native_pr_handoff_projection_lane(issue).await;
            info!(
                issue_identifier = %issue.identifier,
                issue_state = %issue.state,
                pull_request = %github.pull_request.url,
                head_branch = %github.pull_request.head_branch.0,
                "native issue dispatch skipped because durable GitHub PR handoff is awaiting review/check feedback"
            );
            return true;
        }
        if native_pr_handoff_missing_durable_pr_evidence(config, issue, &linear) {
            self.deliver_kapellmeister_shadow_signal(native_pr_handoff_missing_pr_signal(
                &issue.identifier,
                Utc::now(),
            ));
            let mut state = self.state.write().await;
            state.last_error = Some(format!(
                "native dispatch reported {} to Kapellmeister because durable Workpad/Linear evidence points to a PR handoff but no matching GitHub PR exists; Linear lane was not moved by the host",
                issue.identifier
            ));
            info!(
                issue_identifier = %issue.identifier,
                issue_state = %issue.state,
                "native issue dispatch skipped because PR handoff evidence is missing its durable GitHub PR"
            );
            return true;
        }
        false
    }

    async fn repair_native_pr_handoff_projection_lane(&self, issue: &Issue) -> bool {
        let Some(review_lane) = native_pr_handoff_waiting_lane(self.config.effective(), issue)
        else {
            return false;
        };
        if normalize_state(&issue.state) == normalize_state(&review_lane) {
            return false;
        }
        warn!(
            issue_identifier = %issue.identifier,
            from_state = %issue.state,
            to_state = %review_lane,
            "repairing native PR handoff projection lane"
        );
        self.project_issue_to_native_lane(
            issue,
            &review_lane,
            "native-pr-handoff-projection-repair",
        )
        .await
    }

    async fn should_dispatch(&self, issue: &Issue, allow_claimed: bool) -> bool {
        let state = self.state.read().await;
        should_dispatch_locked(issue, &state, self.config.effective(), allow_claimed)
    }

    async fn has_available_slot_for(&self, issue: &Issue, allow_claimed: bool) -> bool {
        let state = self.state.read().await;
        has_available_slot_locked(issue, &state, self.config.effective(), allow_claimed)
    }

    fn worker_shutdown_timeout(&self) -> Duration {
        Duration::from_millis(
            self.config
                .effective()
                .meerkat
                .read_timeout_ms
                .saturating_add(self.config.effective().hooks.timeout_ms)
                .saturating_add(5_000),
        )
    }

    async fn global_available_slots(&self) -> usize {
        let state = self.state.read().await;
        self.config
            .effective()
            .agent
            .max_concurrent_agents
            .saturating_sub(state.running.len())
    }
}

fn native_lane_move_satisfied(output: &MoveIssueOutput) -> bool {
    // Linear move operations are idempotent: a successful no-op means the issue
    // is already in the target lane and the compact mob handoff is satisfied.
    output.changed || output.previous_state.is_none()
}

fn native_launch_refresh_action(
    config: &ServiceConfig,
    refreshed: Issue,
) -> NativeLaunchRefreshAction {
    if native_kapellmeister_prompt_control_enabled(config) {
        if let Some(reason) = kapellmeister_start_issue_block_reason(config, &refreshed) {
            return NativeLaunchRefreshAction::Stop {
                reason: format!(
                    "fresh Linear state {} is not eligible before native launch: {reason}",
                    refreshed.state
                ),
            };
        }
        return NativeLaunchRefreshAction::Continue(refreshed);
    }
    if config.tracker.is_terminal_state(&refreshed.state) {
        return NativeLaunchRefreshAction::Stop {
            reason: format!(
                "fresh Linear state {} is terminal before native launch",
                refreshed.state
            ),
        };
    }
    if !config.tracker.is_intake_state(&refreshed.state)
        && !config.tracker.is_active_state(&refreshed.state)
    {
        return NativeLaunchRefreshAction::Stop {
            reason: format!(
                "fresh Linear state {} is not eligible before native launch",
                refreshed.state
            ),
        };
    }
    NativeLaunchRefreshAction::Continue(refreshed)
}

fn kapellmeister_start_issue_block_reason(config: &ServiceConfig, issue: &Issue) -> Option<String> {
    if config.is_parked_state(&issue.state) {
        return Some(
            "issue is parked; Kapellmeister must choose a nonterminal control lane before starting a mob"
                .to_string(),
        );
    }
    if config
        .linear
        .lanes
        .blocked
        .as_ref()
        .is_some_and(|blocked| same_issue_lane(&issue.state, blocked))
    {
        return Some(
            "issue is blocked; Kapellmeister must choose a nonterminal control lane before starting a mob"
                .to_string(),
        );
    }
    if config.tracker.is_terminal_state(&issue.state)
        && !config
            .linear
            .lanes
            .review_ready
            .as_ref()
            .is_some_and(|review_ready| same_issue_lane(&issue.state, review_ready))
    {
        return Some(format!(
            "issue is in terminal lane {}; Kapellmeister must choose a nonterminal control lane before starting a mob",
            issue.state
        ));
    }
    None
}

fn same_issue_lane(left: &str, right: &str) -> bool {
    normalize_state(left) == normalize_state(right)
}

fn native_pr_handoff_candidate(entry: &RunningEntry) -> bool {
    git_status_excluding_rkat(Path::new(&entry.workspace_path))
        .map(|status| !status.trim().is_empty())
        .unwrap_or(false)
}

fn native_pr_handoff_branch(
    issue: &Issue,
    inspection: &crate::workspace::WorkspaceInspection,
) -> BranchName {
    inspection
        .current_branch
        .clone()
        .map(BranchName)
        .or_else(|| issue.branch_name.clone().map(BranchName))
        .filter(|branch| !matches!(branch.0.as_str(), "" | "main" | "master"))
        .unwrap_or_else(|| BranchName(format!("fugue/{}", issue.identifier.to_ascii_lowercase())))
}

fn native_pr_handoff_body(issue: &Issue, entry: &RunningEntry) -> String {
    let run_id = entry
        .native_run
        .as_ref()
        .map(|native_run| native_run.run_id.0.as_str())
        .unwrap_or("unknown");
    format!(
        "## Fugue native PR handoff\n\nIssue: {}\n\nThis PR was created by the repo-boundary handoff after the native planner/coder/reviewer mob completed with workspace changes.\n\nNative run: `{}`\n",
        issue.identifier, run_id
    )
}

fn append_native_pr_handoff_evidence(
    body: &str,
    entry: &RunningEntry,
    pull_request: &GitHubPullRequestSnapshot,
) -> String {
    append_native_pr_handoff_evidence_for_run(
        body,
        entry
            .native_run
            .as_ref()
            .map(|native_run| &native_run.run_id),
        pull_request,
    )
}

fn append_native_pr_handoff_evidence_for_run(
    body: &str,
    run_id: Option<&MobRunId>,
    pull_request: &GitHubPullRequestSnapshot,
) -> String {
    if body.contains(&pull_request.url) {
        return body.to_string();
    }
    let run_id = run_id.map(|run_id| run_id.0.as_str()).unwrap_or("unknown");
    format!(
        "{}\n\n### Pull request or Handoff\n- [x] Repo boundary PR handoff: {} (PR #{}, head `{}`, run `{}`).\n- [x] PR handoff validations: accepted focused head `{}` at `{}`; state `{:?}`; draft `{}`.\n",
        body.trim_end(),
        pull_request.url,
        pull_request.number.0,
        pull_request.head_branch.0,
        run_id,
        pull_request.head_branch.0,
        pull_request.head_sha.0,
        pull_request.state,
        pull_request.draft
    )
}

const NATIVE_PR_HANDOFF_MONITOR_HEADING: &str = "### PR Review Monitor";
const NATIVE_LIVENESS_MONITOR_HEADING: &str = "### Fugue Liveness Monitor";
const NATIVE_HOST_BLOCKED_HEADING: &str = "### Fugue Host Blocked";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativePrHandoffWaitingReason {
    CiPending,
    AwaitingReviewOrMerge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativePrHandoffReplanReason {
    ConflictingPrEvidence,
    PullRequestClosed,
    CiFailed,
    DraftRequiresReviewerGate,
    CiUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativePrHandoffMonitorAction {
    Waiting(NativePrHandoffWaitingReason),
    Replan(NativePrHandoffReplanReason),
    Complete,
}

fn native_pr_handoff_monitor_candidate(config: &ServiceConfig, issue: &Issue) -> bool {
    matches!(
        dispatch_backend_for_issue(issue, config),
        DispatchBackend::NativeMob
    ) && !config.tracker.is_terminal_state(&issue.state)
        && !is_native_ready_state(config, &issue.state)
        && !is_native_parked_state(config, &issue.state)
}

fn native_deploy_lane_candidate(config: &ServiceConfig, issue: &Issue) -> bool {
    config.deploy.enabled
        && matches!(
            dispatch_backend_for_issue(issue, config),
            DispatchBackend::NativeMob
        )
        && normalize_state(&config.deploy.lane) == normalize_state(&issue.state)
}

fn is_native_parked_state(config: &ServiceConfig, state: &str) -> bool {
    config.is_parked_state(state)
}

fn native_pr_handoff_monitor_action(
    github: &RecoveredGitHubEvidence,
    ci: Option<&CheckCiOutput>,
) -> NativePrHandoffMonitorAction {
    if !github.rejected_pull_requests.is_empty() {
        return NativePrHandoffMonitorAction::Replan(
            NativePrHandoffReplanReason::ConflictingPrEvidence,
        );
    }
    match github.pull_request.state {
        PullRequestState::Merged => return NativePrHandoffMonitorAction::Complete,
        PullRequestState::Closed => {
            return NativePrHandoffMonitorAction::Replan(
                NativePrHandoffReplanReason::PullRequestClosed,
            );
        }
        PullRequestState::Open => {}
    }
    if let Some(ci) = ci {
        match ci.status {
            CiStatus::Failure | CiStatus::Error | CiStatus::Cancelled => {
                return NativePrHandoffMonitorAction::Replan(NativePrHandoffReplanReason::CiFailed);
            }
            CiStatus::Pending if !ci.checks.is_empty() => {
                return NativePrHandoffMonitorAction::Waiting(
                    NativePrHandoffWaitingReason::CiPending,
                );
            }
            CiStatus::NoChecks | CiStatus::Pending | CiStatus::Success | CiStatus::Skipped => {}
        }
    }
    if github.pull_request.draft {
        return NativePrHandoffMonitorAction::Replan(
            NativePrHandoffReplanReason::DraftRequiresReviewerGate,
        );
    }
    if ci.is_none() {
        return NativePrHandoffMonitorAction::Replan(NativePrHandoffReplanReason::CiUnavailable);
    }
    NativePrHandoffMonitorAction::Waiting(NativePrHandoffWaitingReason::AwaitingReviewOrMerge)
}

fn native_final_pr_gate_start_request_from_handoff(
    config: &ServiceConfig,
    issue: &Issue,
    handoff: &RecoveredGitHubEvidence,
) -> Result<Option<FinalPrGateStartRequest>> {
    let final_review = config.final_review.clone();
    let issue_ref = issue_ref_for_identifier(&issue.identifier);
    let worktree_path = WorkspaceManager::for_service(config)
        .path_for_issue(&issue.identifier)?
        .display()
        .to_string();
    Ok(final_pr_gate_start_request(
        FinalPrGateRuntimeTriggerInput {
            config: final_review,
            issue: issue_ref,
            issue_state: issue.state.clone(),
            review_ready_lane: config.linear.lanes.review_ready.clone(),
            typed_pr_handoff_present: true,
            external_lane_move: normalize_state(&issue.state)
                == normalize_state(&config.final_review.lane),
            pr: handoff.pull_request.clone(),
            workspace_key: workspace_key_for_identifier(&issue.identifier),
            worktree_path: Some(worktree_path),
        },
    ))
}

fn native_pr_handoff_monitor_blocks_dispatch(action: NativePrHandoffMonitorAction) -> bool {
    matches!(
        action,
        NativePrHandoffMonitorAction::Waiting(_) | NativePrHandoffMonitorAction::Complete
    )
}

fn native_pr_handoff_monitor_signal_decision(
    action: NativePrHandoffMonitorAction,
) -> (PrHandoffMonitorDecision, &'static str) {
    match action {
        NativePrHandoffMonitorAction::Waiting(NativePrHandoffWaitingReason::CiPending) => (
            PrHandoffMonitorDecision::CiPending,
            "CI is still pending; Fugue host will keep polling and deliver a new signal when the status changes.",
        ),
        NativePrHandoffMonitorAction::Waiting(
            NativePrHandoffWaitingReason::AwaitingReviewOrMerge,
        ) => (
            PrHandoffMonitorDecision::ReadyForDone,
            "PR is eligible for handoff: CI is acceptable or no checks were reported. Follow the configured review holding lane and completion policy after reviewer and WorkGraph obligations are satisfied. This signal does not authorize a merge or claim checks ran.",
        ),
        NativePrHandoffMonitorAction::Replan(_) => (
            PrHandoffMonitorDecision::Replan,
            "PR handoff monitor found red or unusable PR evidence; route the issue back to Active for focused remediation.",
        ),
        NativePrHandoffMonitorAction::Complete => (
            PrHandoffMonitorDecision::Merged,
            "Merged PR observed; project to the configured terminal success lane after verifying remaining completion obligations.",
        ),
    }
}

fn kapellmeister_host_projection_repair_allowed(idempotency_suffix: &str) -> bool {
    matches!(
        idempotency_suffix,
        "native-kapellmeister-pr-handoff-attention"
    )
}

fn inspect_native_pr_handoff_ci(
    github: &dyn NativeStatusGitHubInspector,
    pull_request: &GitHubPullRequestSnapshot,
) -> std::result::Result<CheckCiOutput, String> {
    match github.check_pr_ci(pull_request) {
        FugueToolResult::Ok {
            output: FugueToolOutput::GitHubCheckCi(output),
            ..
        } => Ok(output),
        FugueToolResult::Err { error, .. } => Err(error.message),
        FugueToolResult::Ok { output, .. } => Err(format!(
            "github.check_ci returned unexpected output {output:?}"
        )),
    }
}

fn append_native_pr_handoff_monitor_evidence(
    body: &str,
    github: &RecoveredGitHubEvidence,
    ci: Option<&CheckCiOutput>,
    action: &NativePrHandoffMonitorAction,
    final_gate_start: Option<&FinalPrGateStartRequest>,
) -> String {
    upsert_markdown_section(
        body,
        NATIVE_PR_HANDOFF_MONITOR_HEADING,
        &native_pr_handoff_monitor_section(github, ci, action, final_gate_start),
    )
}

fn append_native_liveness_monitor_evidence(
    body: &str,
    reason: NativeRunParkReason,
    detail: Option<&str>,
) -> String {
    upsert_markdown_section(
        body,
        NATIVE_LIVENESS_MONITOR_HEADING,
        &native_liveness_monitor_section(reason, detail),
    )
}

fn append_native_host_blocked_evidence(body: &str, detail: &str) -> String {
    upsert_markdown_section(
        body,
        NATIVE_HOST_BLOCKED_HEADING,
        &native_host_blocked_section(detail),
    )
}

fn native_host_blocked_section(detail: &str) -> String {
    [
        NATIVE_HOST_BLOCKED_HEADING.to_string(),
        "- [x] Host decision: blocked issue; Fugue could not launch a viable native mob."
            .to_string(),
        format!(
            "- [x] Blocker detail: {}.",
            native_liveness_detail_for_workpad(detail)
        ),
        "- [x] Human intervention is required before leaving Blocked; once resolved, resume through Queue or Active with WorkGraph/Workpad evidence."
            .to_string(),
    ]
    .join("\n")
}

fn native_liveness_monitor_section(reason: NativeRunParkReason, detail: Option<&str>) -> String {
    let mut lines = vec![
        NATIVE_LIVENESS_MONITOR_HEADING.to_string(),
        format!(
            "- [x] Liveness decision: parked issue; {}.",
            native_run_park_reason(reason)
        ),
        "- [x] Fugue will not start another mob attempt until the blocker is resolved and the issue is moved back to a ready lane.".to_string(),
    ];
    if let Some(detail) = detail {
        lines.push(format!(
            "- [x] Last terminal detail: {}.",
            native_liveness_detail_for_workpad(detail)
        ));
    }
    lines.join("\n")
}

fn native_run_park_reason(reason: NativeRunParkReason) -> &'static str {
    match reason {
        NativeRunParkReason::ResourceExhausted => {
            "Meta reported resource exhaustion/session capacity, so retrying would rotate lanes without progress"
        }
        NativeRunParkReason::HostMechanics => {
            "Fugue host mechanics failed after the mob handed off, so retrying the mob would rotate lanes without product progress"
        }
        NativeRunParkReason::Stalled => {
            "Meta did not report fresh progress before the configured stall timeout, so retrying automatically would rotate lanes without diagnosing the stuck runtime"
        }
    }
}

fn native_liveness_detail_for_workpad(detail: &str) -> String {
    const MAX_CHARS: usize = 1_200;
    let compact = detail.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut truncated = String::new();
    for (index, ch) in compact.chars().enumerate() {
        if index >= MAX_CHARS {
            truncated.push_str("...");
            break;
        }
        truncated.push(ch);
    }
    truncated
}

fn native_pr_handoff_monitor_section(
    github: &RecoveredGitHubEvidence,
    ci: Option<&CheckCiOutput>,
    action: &NativePrHandoffMonitorAction,
    final_gate_start: Option<&FinalPrGateStartRequest>,
) -> String {
    let pull_request = &github.pull_request;
    let mut lines = vec![
        NATIVE_PR_HANDOFF_MONITOR_HEADING.to_string(),
        format!(
            "- [x] PR #{} observed: {}; state `{:?}`; draft `{}`; head `{}` at `{}`.",
            pull_request.number.0,
            pull_request.url,
            pull_request.state,
            pull_request.draft,
            pull_request.head_branch.0,
            pull_request.head_sha.0
        ),
    ];
    if let Some(ci) = ci {
        lines.push(format!(
            "- [x] CI status: `{:?}`; checks: {}.",
            ci.status,
            ci.checks.len()
        ));
    } else {
        lines.push("- [ ] CI status unavailable or not required for this PR state.".to_string());
    }
    if let Some(final_gate) = final_gate_start {
        lines.push(format!(
            "- [x] Final PR gate trigger: {:?}; Kapellmeister start/resume idempotency `{}`; {} configured final reviewer(s).",
            final_gate.trigger_mode,
            final_gate.spawn_input.idempotency_key.0,
            final_gate.reviewers.len()
        ));
        lines.push("- [x] Host role: observed/recorded trigger evidence only; Kapellmeister owns typed verdict aggregation, lane projection, and any merge authority.".to_string());
    }
    if !github.rejected_pull_requests.is_empty() {
        for rejected in github.rejected_pull_requests.iter().take(5) {
            lines.push(format!(
                "- [ ] Rejected PR evidence: PR #{}: {}.",
                rejected.number.0, rejected.reason
            ));
        }
    }
    match action {
        NativePrHandoffMonitorAction::Waiting(reason) => {
            lines.push(format!(
                "- [ ] PR monitor decision: waiting; {}.",
                native_pr_handoff_waiting_reason(*reason)
            ));
        }
        NativePrHandoffMonitorAction::Replan(reason) => {
            let reason = native_pr_handoff_replan_reason(*reason);
            lines.push(format!(
                "- [x] Reviewer decision: red / return to Active focused remediation; {reason}."
            ));
            lines.push(format!(
                "- [ ] Reviewer findings requiring Active remediation with WorkGraph/Workpad/phase evidence: {reason}."
            ));
        }
        NativePrHandoffMonitorAction::Complete => {
            lines.push(
                "- [x] PR monitor decision: merged PR observed; move issue to terminal success lane."
                    .to_string(),
            );
        }
    }
    lines.join("\n")
}

fn native_pr_handoff_waiting_reason(reason: NativePrHandoffWaitingReason) -> &'static str {
    match reason {
        NativePrHandoffWaitingReason::CiPending => {
            "CI is still pending; keep the issue Active and continue polling"
        }
        NativePrHandoffWaitingReason::AwaitingReviewOrMerge => {
            "PR is open and eligible for handoff; follow the configured holding lane after reviewer and WorkGraph obligations are satisfied; no merge is implied"
        }
    }
}

fn native_pr_handoff_replan_reason(reason: NativePrHandoffReplanReason) -> &'static str {
    match reason {
        NativePrHandoffReplanReason::ConflictingPrEvidence => {
            "durable PR evidence conflicts with the current issue or workspace branch"
        }
        NativePrHandoffReplanReason::PullRequestClosed => "PR was closed before merge",
        NativePrHandoffReplanReason::CiFailed => "PR checks are red or cancelled",
        NativePrHandoffReplanReason::DraftRequiresReviewerGate => {
            "draft PR still requires reviewer or repo-gate work, so Fugue must re-enter the mob instead of passively waiting"
        }
        NativePrHandoffReplanReason::CiUnavailable => {
            "CI evidence is unavailable, so Fugue must create a focused plan to obtain or repair verification evidence"
        }
    }
}

pub(crate) fn upsert_markdown_section(body: &str, heading: &str, section: &str) -> String {
    let body = body.trim_end();
    let section = section.trim_end();
    let lines = body.lines().collect::<Vec<_>>();
    let Some(start) = lines.iter().position(|line| line.trim() == heading) else {
        if body.is_empty() {
            return section.to_string();
        }
        return format!("{body}\n\n{section}");
    };
    let mut end = lines.len();
    for (index, line) in lines.iter().enumerate().skip(start + 1) {
        let trimmed = line.trim_start();
        if (trimmed.starts_with("### ") || trimmed.starts_with("## ")) && trimmed.trim() != heading
        {
            end = index;
            break;
        }
    }
    let before = lines[..start].join("\n");
    let after = lines[end..].join("\n");
    match (before.trim().is_empty(), after.trim().is_empty()) {
        (true, true) => section.to_string(),
        (true, false) => format!("{section}\n\n{}", after.trim_start()),
        (false, true) => format!("{}\n\n{section}", before.trim_end()),
        (false, false) => format!(
            "{}\n\n{section}\n\n{}",
            before.trim_end(),
            after.trim_start()
        ),
    }
}

fn ensure_workspace_branch_for_pr(workspace: &Path, branch: &BranchName) -> Result<()> {
    let current = git_output(workspace, &["branch", "--show-current"])?;
    if current.trim() == branch.0 {
        return Ok(());
    }
    git_run(workspace, &["switch", "-C", &branch.0])
}

fn git_staged_changes_empty(workspace: &Path) -> Result<bool> {
    let output = git_output(workspace, &["diff", "--cached", "--name-only"])?;
    Ok(output.trim().is_empty())
}

fn git_add_workspace_changes_excluding_runtime(workspace: &Path) -> Result<()> {
    ensure_workspace_runtime_git_excludes(workspace)?;
    git_run(workspace, &["add", "-A", "--", "."])
}

fn git_status_excluding_rkat(workspace: &Path) -> Result<String> {
    ensure_workspace_runtime_git_excludes(workspace)?;
    git_output(workspace, &["status", "--porcelain", "--", "."])
}

fn ensure_workspace_runtime_git_excludes(workspace: &Path) -> Result<()> {
    let exclude_path = git_output(workspace, &["rev-parse", "--git-path", "info/exclude"])?;
    let exclude_path = {
        let path = Path::new(exclude_path.trim());
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            workspace.join(path)
        }
    };
    let existing = match fs::read_to_string(&exclude_path) {
        Ok(existing) => existing,
        Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
        Err(err) => {
            return Err(FugueError::ProcessExit(format!(
                "failed to read git exclude '{}': {err}",
                exclude_path.display()
            )));
        }
    };
    let mut updated = existing.clone();
    for runtime_dir in [".rkat/", ".fugue/", "runtime.sqlite", "blobs/"] {
        if !existing.lines().any(|line| line.trim() == runtime_dir) {
            if !updated.ends_with('\n') && !updated.is_empty() {
                updated.push('\n');
            }
            updated.push_str(runtime_dir);
            updated.push('\n');
        }
    }
    if updated != existing {
        if let Some(parent) = exclude_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                FugueError::ProcessExit(format!(
                    "failed to create git exclude directory '{}': {err}",
                    parent.display()
                ))
            })?;
        }
        fs::write(&exclude_path, updated).map_err(|err| {
            FugueError::ProcessExit(format!(
                "failed to write git exclude '{}': {err}",
                exclude_path.display()
            ))
        })?;
    }
    Ok(())
}

fn git_output(workspace: &Path, args: &[&str]) -> Result<String> {
    git_output_with_connections(workspace, args, None)
}

fn git_output_with_connections(
    workspace: &Path,
    args: &[&str],
    connections: Option<&crate::setup::connections::Connections>,
) -> Result<String> {
    let mut command = StdCommand::new("git");
    if let Some(connections) = connections {
        command.envs(connections.github_environment());
    }
    let output = command
        .args(args)
        .current_dir(workspace)
        .output()
        .map_err(|err| FugueError::ProcessExit(format!("failed to execute git {args:?}: {err}")))?;
    if !output.status.success() {
        return Err(FugueError::ProcessExit(format!(
            "git {args:?} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_run(workspace: &Path, args: &[&str]) -> Result<()> {
    git_output(workspace, args).map(|_| ())
}

fn native_run_terminal_action(
    status: MobRunStatus,
    evidence: Option<&NativeMobE2eEvidence>,
) -> NativeRunTerminalAction {
    match status {
        MobRunStatus::Succeeded => {
            let Some(evidence) = evidence else {
                return NativeRunTerminalAction::HumanReview;
            };
            if evidence.completed_issue_with_single_pr()
                && evidence.review_rework_loop_is_bounded_and_routed()
            {
                NativeRunTerminalAction::Complete
            } else if !evidence.stays_within_review_cycle_limit()
                || !evidence.routes_every_rejection_to_coder()
            {
                NativeRunTerminalAction::HumanReview
            } else {
                NativeRunTerminalAction::Replan
            }
        }
        MobRunStatus::Failed | MobRunStatus::Cancelled => NativeRunTerminalAction::Retry,
        MobRunStatus::Pending | MobRunStatus::Running | MobRunStatus::WaitingForInput => {
            NativeRunTerminalAction::KeepRunning
        }
    }
}

fn select_native_run_terminal_action(
    status: MobRunStatus,
    evidence: Option<&NativeMobE2eEvidence>,
    observed_events: &[MobEventSnapshot],
    reviewer_gate: &ReviewerGateObservation,
    run_requests_replan: bool,
    recovery_review_present: bool,
    pr_handoff_candidate: bool,
) -> NativeRunTerminalAction {
    if reviewer_gate.requests_replan() {
        return NativeRunTerminalAction::Replan;
    }
    if reviewer_gate.allows_pr_handoff() && pr_handoff_candidate {
        return NativeRunTerminalAction::CreatePrHandoff;
    }
    if let Some(reason) = native_terminal_park_reason(observed_events) {
        return NativeRunTerminalAction::Park(reason);
    }
    if run_requests_replan {
        return NativeRunTerminalAction::Replan;
    }
    if reviewer_gate.is_invalid() {
        return NativeRunTerminalAction::HumanReview;
    }
    if recovery_review_present && pr_handoff_candidate {
        return NativeRunTerminalAction::CreatePrHandoff;
    }
    if recovery_review_present {
        return NativeRunTerminalAction::HumanReview;
    }
    native_run_terminal_action(status, evidence)
}

fn native_retry_reason(
    status: MobRunStatus,
    evidence: Option<&NativeMobE2eEvidence>,
    events: &[MobEventSnapshot],
) -> String {
    let mut reason = match evidence {
        Some(evidence) => format!(
            "native mob completed with status {status:?}; pr_count={}; review_rejections={}; routed_rejections={}; max_review_cycles={:?}",
            evidence.opened_pr_count,
            evidence.review_rejections,
            evidence.rejections_routed_to_coder,
            evidence.max_review_cycles
        ),
        None => format!("native mob completed with status {status:?}"),
    };
    if let Some(detail) = native_terminal_detail(events) {
        reason.push_str("; detail=");
        reason.push_str(detail);
    }
    reason
}

fn native_terminal_status_from_events(events: &[MobEventSnapshot]) -> Option<MobRunStatus> {
    events.iter().rev().find_map(|event| {
        if let crate::tools::MobEventKind::RunCompleted { status, .. } = event.kind {
            Some(status)
        } else {
            None
        }
    })
}

struct TurnLimitEvent<'a> {
    member: &'a str,
    detail: &'a str,
}

struct MemberTerminalFailureEvent<'a> {
    member: &'a str,
    detail: &'a str,
}

fn turn_limit_events(events: &[MobEventSnapshot]) -> impl Iterator<Item = TurnLimitEvent<'_>> {
    events.iter().filter_map(|event| match &event.kind {
        crate::tools::MobEventKind::MemberKickoffUpdated { member, phase }
            if phase == "turn_limit_reached" =>
        {
            Some(TurnLimitEvent {
                member,
                detail: event
                    .message
                    .as_deref()
                    .unwrap_or("member hit autonomous turn limit"),
            })
        }
        _ => None,
    })
}

fn member_terminal_failure_events(
    events: &[MobEventSnapshot],
) -> impl Iterator<Item = MemberTerminalFailureEvent<'_>> {
    events.iter().filter_map(|event| match &event.kind {
        crate::tools::MobEventKind::MemberKickoffUpdated { member, phase }
            if phase == "member_terminal_failure" =>
        {
            Some(MemberTerminalFailureEvent {
                member,
                detail: event
                    .message
                    .as_deref()
                    .unwrap_or("member had a terminal runtime turn failure"),
            })
        }
        _ => None,
    })
}

fn native_terminal_detail(events: &[MobEventSnapshot]) -> Option<&str> {
    events.iter().rev().find_map(|event| {
        if matches!(
            event.kind,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed | MobRunStatus::Cancelled,
                ..
            }
        ) {
            event.message.as_deref()
        } else {
            None
        }
    })
}

fn native_terminal_diagnostic_details(events: &[MobEventSnapshot]) -> impl Iterator<Item = &str> {
    events
        .iter()
        .rev()
        .filter_map(|event| event.message.as_deref())
}

fn native_terminal_park_reason(events: &[MobEventSnapshot]) -> Option<NativeRunParkReason> {
    native_terminal_detail(events)?;
    if native_terminal_diagnostic_details(events).any(native_terminal_detail_is_resource_exhausted)
    {
        return Some(NativeRunParkReason::ResourceExhausted);
    }
    if native_terminal_diagnostic_details(events).any(native_mechanism_failure_is_nonretryable) {
        return Some(NativeRunParkReason::HostMechanics);
    }
    None
}

fn native_terminal_park_detail(events: &[MobEventSnapshot]) -> Option<String> {
    native_terminal_diagnostic_details(events)
        .find(|detail| {
            native_terminal_detail_is_resource_exhausted(detail)
                || native_mechanism_failure_is_nonretryable(detail)
        })
        .or_else(|| native_terminal_detail(events))
        .map(ToString::to_string)
}

fn native_terminal_detail_is_resource_exhausted(detail: &str) -> bool {
    let normalized = detail.to_ascii_lowercase();
    normalized.contains("max sessions reached")
        || normalized.contains("resource exhausted")
        || normalized.contains("session capacity")
        || normalized.contains("too many sessions")
}

fn native_mechanism_failure_is_nonretryable(detail: &str) -> bool {
    let normalized = detail.to_ascii_lowercase();
    normalized.contains("flow authority projection mismatch")
        || normalized.contains("duplicate persisted mob id during restore")
        || normalized.contains("cannot resume flow run")
        || normalized.contains("llmfailure")
        || normalized.contains("llm failure terminal turn")
        || normalized.contains("turn abandoned: apply failed")
}

fn native_pr_handoff_error_requires_operator_park(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("ignored by one of your .gitignore")
        && (normalized.contains(".rkat") || normalized.contains(".fugue"))
}

fn native_run_status_is_active(status: MobRunStatus) -> bool {
    matches!(
        status,
        MobRunStatus::Pending | MobRunStatus::Running | MobRunStatus::WaitingForInput
    )
}

fn projected_native_lane_for_entry(
    config: &ServiceConfig,
    entry: &RunningEntry,
    status: MobRunStatus,
) -> Option<String> {
    let selected = config.select_template_for_issue(&entry.issue).ok()?;
    let events = entry
        .native_run
        .as_ref()
        .map(|native_run| native_run.observed_events.as_slice())
        .unwrap_or(&[]);
    evaluate_issue_projection(selected.template, status, events)
}

fn config_phase_labels_for_running_entry(
    config: &ServiceConfig,
    entry: &RunningEntry,
) -> Vec<String> {
    let selected = match config.select_template_for_issue(&entry.issue) {
        Ok(selected) => selected,
        Err(_) => return Vec::new(),
    };
    let events = entry
        .native_run
        .as_ref()
        .map(|native_run| native_run.observed_events.as_slice())
        .unwrap_or(&[]);
    active_projection_phase_labels_for_template(selected.template, events)
}

fn projected_native_lane_for_observation(
    config: &ServiceConfig,
    issue: &Issue,
    status: MobRunStatus,
    events: &[MobEventSnapshot],
) -> Option<String> {
    if !native_run_status_is_active(status) {
        return None;
    }
    let selected = config.select_template_for_issue(issue).ok()?;
    evaluate_issue_projection(selected.template, status, events)
}

fn projected_native_lane_for_node(
    config: &ServiceConfig,
    issue: &Issue,
    node: &str,
) -> Option<String> {
    let selected = config.select_template_for_issue(issue).ok()?;
    projection_lane_for_template_node(selected.template, node)
}

fn projected_native_terminal_lane_for_status(
    config: &ServiceConfig,
    issue: &Issue,
    status: MobRunStatus,
) -> Option<String> {
    let selected = config.select_template_for_issue(issue).ok()?;
    projected_terminal_lane_for_template_status(selected.template, status)
}

fn recover_native_linear_evidence(
    issue: &Issue,
    workpad_body: Option<&str>,
) -> RecoveredLinearEvidence {
    RecoveredLinearEvidence {
        issue_state: issue.state.clone(),
        branch_name: issue.branch_name.clone().map(BranchName),
        workpad_native_run_id: workpad_body.and_then(parse_workpad_native_run_id),
        workpad_mentions_pr: workpad_body.is_some_and(workpad_mentions_pr),
        workpad_pr_number: workpad_body.and_then(parse_first_pr_number),
        unresolved_review_finding: workpad_body
            .and_then(crate::review_findings::extract_unresolved_review_findings)
            .map(|findings| findings.summary),
        reviewer_rework_requested: workpad_body.is_some_and(workpad_requests_reviewer_rework),
    }
}

fn native_run_requests_replan(native_run: &NativeRunHandle) -> bool {
    match latest_reviewer_gate_observation(&native_run.observed_events) {
        ReviewerGateObservation::Present(gate) => return gate.requests_replan(),
        ReviewerGateObservation::Invalid(_) | ReviewerGateObservation::Absent => {}
    }
    native_run
        .recovery
        .linear
        .as_ref()
        .is_some_and(|linear| linear.reviewer_rework_requested && !linear.workpad_mentions_pr)
}

fn native_pr_handoff_blocks_fresh_dispatch_for_evidence(
    config: &ServiceConfig,
    issue: &Issue,
    linear: &RecoveredLinearEvidence,
    durable_github_pr_exists: bool,
) -> bool {
    native_pr_handoff_waiting_state(config, issue)
        && !config.tracker.is_intake_state(&issue.state)
        && !is_native_ready_state(config, &issue.state)
        && !linear.reviewer_rework_requested
        && (linear.workpad_mentions_pr
            || linear.workpad_pr_number.is_some()
            || durable_github_pr_exists)
}

fn native_pr_handoff_missing_durable_pr_evidence(
    config: &ServiceConfig,
    issue: &Issue,
    linear: &RecoveredLinearEvidence,
) -> bool {
    if !native_pr_handoff_waiting_state(config, issue) {
        return false;
    }
    if config.tracker.is_intake_state(&issue.state)
        || is_native_ready_state(config, &issue.state)
        || linear.reviewer_rework_requested
    {
        return false;
    }
    if linear.workpad_mentions_pr || linear.workpad_pr_number.is_some() {
        return true;
    }
    false
}

fn native_pr_handoff_waiting_state(config: &ServiceConfig, issue: &Issue) -> bool {
    native_pr_handoff_waiting_lane(config, issue)
        .is_some_and(|lane| normalize_state(&issue.state) == normalize_state(&lane))
}

fn native_pr_handoff_waiting_lane(config: &ServiceConfig, issue: &Issue) -> Option<String> {
    config
        .linear
        .lanes
        .review_ready
        .clone()
        .or_else(|| projected_native_lane_for_node(config, issue, "reviewer"))
}

fn native_attention_lane(config: &ServiceConfig) -> String {
    config
        .linear
        .lanes
        .review_ready
        .clone()
        .or_else(|| config.linear.lanes.blocked.clone())
        .unwrap_or_else(|| config.linear.lanes.parked.clone())
}

fn native_liveness_blocked_lane(config: &ServiceConfig) -> String {
    config
        .linear
        .lanes
        .blocked
        .clone()
        .unwrap_or_else(|| config.linear.lanes.parked.clone())
}

fn is_native_ready_state(config: &ServiceConfig, state: &str) -> bool {
    config.tracker.is_intake_state(state)
        || (config.tracker.intake_states.is_empty() && is_todo_state(state))
}

fn native_execution_idempotency_key(identifier: &str, attempt: Option<u32>) -> IdempotencyKey {
    match attempt {
        Some(attempt) if attempt > 0 => {
            IdempotencyKey(format!("native-execution:{identifier}:attempt-{attempt}"))
        }
        _ => IdempotencyKey(format!("native-execution:{identifier}")),
    }
}

fn native_runtime_restore_keys(
    config: &ServiceConfig,
    issues: &[Issue],
    capacity: usize,
) -> Vec<String> {
    if capacity == 0 {
        return Vec::new();
    }

    let mut candidates = issues
        .iter()
        .filter(|issue| {
            matches!(
                dispatch_backend_for_issue(issue, config),
                DispatchBackend::NativeMob
            )
        })
        .filter_map(|issue| {
            let classification = classify_issue_lane(config, &issue.state);
            let priority = if is_native_ready_state(config, &issue.state) {
                1u8
            } else {
                match classification {
                    IssueLaneClassification::ProjectedActive
                    | IssueLaneClassification::ControlActive
                    | IssueLaneClassification::UnexpectedNonterminal => 0u8,
                    IssueLaneClassification::Intake => 1u8,
                    IssueLaneClassification::Parked
                    | IssueLaneClassification::AttentionOrTerminal => {
                        return None;
                    }
                }
            };
            let key = native_runtime_key_for_issue(config, issue)?;
            Some((priority, issue.updated_at, key))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    candidates.into_iter().map(|(_, _, key)| key).collect()
}

fn native_runtime_key_for_issue(config: &ServiceConfig, issue: &Issue) -> Option<String> {
    let selected = config.select_template_for_issue(issue).ok()?;
    let workspace_key = workspace_key_for_identifier(&issue.identifier);
    Some(sanitize_workspace_key(&format!(
        "{}-{}",
        workspace_key.0, selected.template_id.0
    )))
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn native_mcp_servers_for_issue(
    base: &BTreeMap<String, NativeMcpServerConfig>,
    workflow_dir: &Path,
    issue: &Issue,
    repository: Option<&GitHubRepoRef>,
    terminal_states: &[String],
    review_handoff_states: &[String],
    action_states: &[String],
    blocked_states: &[String],
    workgraph_state_path: &Path,
) -> BTreeMap<String, NativeMcpServerConfig> {
    let mut scoped = base.clone();
    let lane_state_path = issue_lane_state_path_for_workflow(workflow_dir)
        .display()
        .to_string();
    let terminal_states = terminal_states_env_value(terminal_states);
    let review_handoff_states = terminal_states_env_value(review_handoff_states);
    let action_states = terminal_states_env_value(action_states);
    let blocked_states = workgraph_blocked_states_env_value(blocked_states);
    let workgraph_state_path = workgraph_state_path.display().to_string();
    for (name, server) in &mut scoped {
        if is_native_linear_mcp_server(name, server) {
            server
                .env
                .insert("FUGUE_LINEAR_ISSUE_ID".to_string(), issue.id.clone());
            server.env.insert(
                "FUGUE_LINEAR_ISSUE_IDENTIFIER".to_string(),
                issue.identifier.clone(),
            );
            server.env.insert(
                lane_state_path_env_name().to_string(),
                lane_state_path.clone(),
            );
            server.env.insert(
                terminal_states_env_name().to_string(),
                terminal_states.clone(),
            );
            server.env.insert(
                LINEAR_REVIEW_HANDOFF_STATES_ENV.to_string(),
                review_handoff_states.clone(),
            );
            server
                .env
                .insert(LINEAR_ACTION_STATES_ENV.to_string(), action_states.clone());
            server.env.insert(
                WORKGRAPH_BLOCKED_STATES_ENV.to_string(),
                blocked_states.clone(),
            );
            server.env.insert(
                WORKGRAPH_STATE_PATH_ENV.to_string(),
                workgraph_state_path.clone(),
            );
            server
                .env
                .entry("FUGUE_LINEAR_MCP_AUTHORITY".to_string())
                .or_insert_with(|| "projection".to_string());
        }
    }
    if let Some(github) = scoped.get_mut("github") {
        if let Some(repository) = repository {
            if let Some(host) = &repository.host {
                github
                    .env
                    .insert("FUGUE_GITHUB_HOST".to_string(), host.to_string());
            }
            github
                .env
                .insert("FUGUE_GITHUB_OWNER".to_string(), repository.owner.0.clone());
            github
                .env
                .insert("FUGUE_GITHUB_REPO".to_string(), repository.name.0.clone());
        }
        if let Some(branch) = issue
            .branch_name
            .as_deref()
            .filter(|branch| !branch.trim().is_empty())
        {
            github
                .env
                .insert("FUGUE_GITHUB_HEAD_BRANCH".to_string(), branch.to_string());
        }
    }
    scoped
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn native_member_mcp_servers_for_issue(
    base: &BTreeMap<String, NativeMcpServerConfig>,
    workflow_dir: &Path,
    template: &MaterializedMobTemplate,
    issue: &Issue,
    repository: Option<&GitHubRepoRef>,
    terminal_states: &[String],
    review_handoff_states: &[String],
    action_states: &[String],
) -> BTreeMap<String, NativeMcpServerConfig> {
    let referenced = template
        .members
        .iter()
        .flat_map(|member| member.agent.tools.iter())
        .filter_map(|tool| tool.strip_prefix("mcp:"))
        .collect::<BTreeSet<_>>();
    let member_visible = base
        .iter()
        .filter(|(name, _)| referenced.contains(name.as_str()))
        .map(|(name, server)| (name.clone(), server.clone()))
        .chain(
            base.get("linear_projection")
                .map(|server| ("linear_projection".to_string(), server.clone())),
        )
        .collect::<BTreeMap<_, _>>();
    native_mcp_servers_for_issue(
        &member_visible,
        workflow_dir,
        issue,
        repository,
        terminal_states,
        review_handoff_states,
        action_states,
        &native_workgraph_blocked_states_from_tracker(terminal_states),
        &PathBuf::from(".fugue/meerkat/mobkit-runtime/workgraph.sqlite3"),
    )
}

fn native_member_direct_tool_servers(
    tracker: &TrackerConfig,
    template: &MaterializedMobTemplate,
) -> BTreeSet<String> {
    let referenced = template
        .members
        .iter()
        .flat_map(|member| member.agent.tools.iter())
        .filter_map(|tool| tool.strip_prefix("mcp:"));
    referenced
        .filter(|server| tracker.kind == "fugue-ims" && *server == "linear")
        .map(ToString::to_string)
        .collect()
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn native_repo_kapellmeister_mcp_servers(
    base: &BTreeMap<String, NativeMcpServerConfig>,
    workflow_dir: &Path,
    repository: Option<&GitHubRepoRef>,
    project_slug: Option<&str>,
    terminal_states: &[String],
    review_handoff_states: &[String],
    action_states: &[String],
    blocked_states: &[String],
    workgraph_state_path: &Path,
    fugue_host_api_url: Option<&str>,
) -> BTreeMap<String, NativeMcpServerConfig> {
    let mut scoped = base
        .iter()
        .filter(|(name, _)| name.as_str() != "fugue_host")
        .map(|(name, server)| (name.clone(), server.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, server) in &mut scoped {
        if is_native_linear_mcp_server(name, server) {
            server.env.insert(
                "FUGUE_LINEAR_MCP_AUTHORITY".to_string(),
                "projection".to_string(),
            );
            if let Some(project_slug) = project_slug.filter(|value| !value.is_empty()) {
                server
                    .env
                    .insert("LINEAR_PROJECT_SLUG".to_string(), project_slug.to_string());
            }
            server.env.insert(
                lane_state_path_env_name().to_string(),
                issue_lane_state_path_for_workflow(workflow_dir)
                    .display()
                    .to_string(),
            );
            server.env.insert(
                terminal_states_env_name().to_string(),
                terminal_states_env_value(terminal_states),
            );
            server.env.insert(
                LINEAR_REVIEW_HANDOFF_STATES_ENV.to_string(),
                terminal_states_env_value(review_handoff_states),
            );
            server.env.insert(
                LINEAR_ACTION_STATES_ENV.to_string(),
                terminal_states_env_value(action_states),
            );
            server.env.insert(
                WORKGRAPH_BLOCKED_STATES_ENV.to_string(),
                workgraph_blocked_states_env_value(blocked_states),
            );
            server.env.insert(
                WORKGRAPH_STATE_PATH_ENV.to_string(),
                workgraph_state_path.display().to_string(),
            );
        }
    }
    if let Some(github) = scoped.get_mut("github")
        && let Some(repository) = repository
    {
        if let Some(host) = &repository.host {
            github
                .env
                .insert("FUGUE_GITHUB_HOST".to_string(), host.to_string());
        }
        github
            .env
            .insert("FUGUE_GITHUB_OWNER".to_string(), repository.owner.0.clone());
        github
            .env
            .insert("FUGUE_GITHUB_REPO".to_string(), repository.name.0.clone());
    }
    if let Some(host) = base.get("fugue_host") {
        scoped.insert("fugue_host".to_string(), host.clone());
    } else {
        let host_command = native_host_mcp_command(base);
        let env = fugue_host_api_url
            .filter(|url| !url.trim().is_empty())
            .map(|url| BTreeMap::from([("FUGUE_HOST_API_URL".to_string(), url.trim().to_string())]))
            .unwrap_or_default();
        scoped.insert(
            "fugue_host".to_string(),
            NativeMcpServerConfig {
                command: host_command,
                url: None,
                env,
            },
        );
    }
    scoped
}

fn fugue_host_api_url_from_config(config: &ServiceConfig) -> Option<String> {
    config
        .server
        .as_ref()
        .and_then(|server| server.port)
        .map(|port| format!("http://127.0.0.1:{port}"))
}

fn fugue_host_api_auth_from_config(
    config: &ServiceConfig,
) -> Option<crate::config::MetaConsoleBasicAuthConfig> {
    config
        .server
        .as_ref()
        .and_then(|server| server.meta_console.basic_auth.clone())
}

fn terminal_states_env_value(terminal_states: &[String]) -> String {
    terminal_states.join(",")
}

fn native_repo_review_handoff_states(config: &ServiceConfig) -> Vec<String> {
    config
        .linear
        .lanes
        .review_ready
        .clone()
        .into_iter()
        .collect()
}

pub(crate) fn native_action_lanes(config: &ServiceConfig) -> Vec<String> {
    let mut states = Vec::new();
    if let Some(do_review) = &config.linear.lanes.do_review {
        add_unique_lane(&mut states, do_review);
    }
    if let Some(deploy) = &config.linear.lanes.deploy {
        add_unique_lane(&mut states, deploy);
    }
    if config.independent_review.enabled && config.independent_review_handoff_lane().is_none() {
        add_unique_lane(&mut states, &config.independent_review.lane);
    }
    if config.deploy.enabled {
        add_unique_lane(&mut states, &config.deploy.lane);
    }
    if config.final_review.enabled {
        add_unique_lane(&mut states, &config.final_review.lane);
    }
    states
}

fn add_unique_lane(states: &mut Vec<String>, lane: &str) {
    if lane.trim().is_empty() {
        return;
    }
    let normalized = normalize_state(lane);
    if !states
        .iter()
        .any(|existing| normalize_state(existing) == normalized)
    {
        states.push(lane.to_string());
    }
}

fn native_workgraph_blocked_states(config: &ServiceConfig) -> Vec<String> {
    let mut states = Vec::new();
    if let Some(blocked) = config.linear.lanes.blocked.clone() {
        states.push(blocked);
    }
    if !states
        .iter()
        .any(|state| normalize_state(state) == normalize_state(&config.final_review.blocked_lane))
    {
        states.push(config.final_review.blocked_lane.clone());
    }
    states
}

fn native_workgraph_blocked_states_from_tracker(terminal_states: &[String]) -> Vec<String> {
    terminal_states
        .iter()
        .filter(|state| normalize_state(state).contains("blocked"))
        .cloned()
        .collect()
}

fn native_issue_review_handoff_states(config: &ServiceConfig, issue: &Issue) -> Vec<String> {
    let mut states = native_repo_review_handoff_states(config);
    if let Some(reviewer_lane) = projected_native_lane_for_node(config, issue, "reviewer") {
        let normalized = normalize_state(&reviewer_lane);
        if !states
            .iter()
            .any(|state| normalize_state(state) == normalized)
        {
            states.push(reviewer_lane);
        }
    }
    states
}

fn linear_error_is_rate_limited(error: &FugueError) -> bool {
    matches!(
        error,
        FugueError::LinearGraphqlErrors(crate::error::LinearGraphqlFailure::RateLimited)
    )
}

fn native_host_mcp_command(base: &BTreeMap<String, NativeMcpServerConfig>) -> Vec<String> {
    base.values()
        .filter_map(|server| server.command.first())
        .find_map(|command| {
            let path = Path::new(command);
            let parent = path.parent()?;
            path.file_name()
                .and_then(|name| name.to_str())
                .filter(|name| matches!(*name, "fugue-linear-mcp" | "fugue-github-mcp"))?;
            Some(parent.join("fugue-host-mcp").display().to_string())
        })
        .map(|command| vec![command])
        .unwrap_or_else(|| vec!["fugue-host-mcp".to_string()])
}

fn next_native_terminal_attempt(entry: &RunningEntry) -> u32 {
    entry
        .retry_attempt
        .or_else(|| {
            entry
                .native_run
                .as_ref()
                .and_then(|native_run| native_run_attempt_from_run_id(&native_run.run_id))
        })
        .unwrap_or(0)
        .saturating_add(1)
}

fn native_run_attempt_from_run_id(run_id: &MobRunId) -> Option<u32> {
    run_id
        .0
        .rsplit_once(":attempt-")
        .and_then(|(_, attempt)| attempt.parse::<u32>().ok())
}

fn native_run_generation_sort_key(run_id: &MobRunId) -> u32 {
    native_run_attempt_from_run_id(run_id).unwrap_or(0)
}

fn next_native_run_generation_from_durable_runs(
    runs: &[MobRunSnapshot],
    requested_generation: Option<u32>,
) -> Option<u32> {
    let durable_next = runs
        .iter()
        .map(|run| native_run_generation_sort_key(&run.run_id))
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    Some(requested_generation.unwrap_or(0).max(durable_next))
}

fn next_native_run_generation_from_workpad(
    requested_generation: Option<u32>,
    workpad_generation: Option<u32>,
) -> Option<u32> {
    match workpad_generation {
        Some(workpad_generation) => Some(
            requested_generation
                .unwrap_or(0)
                .max(workpad_generation.saturating_add(1)),
        ),
        None => requested_generation,
    }
}

fn newest_native_runs_by_issue(runs: Vec<MobRunSnapshot>) -> Vec<MobRunSnapshot> {
    let mut newest = HashMap::<String, MobRunSnapshot>::new();
    for run in runs {
        let key = run
            .issue
            .as_ref()
            .map(|issue| format!("{issue:?}"))
            .unwrap_or_else(|| run.run_id.0.clone());
        match newest.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(run);
            }
            Entry::Occupied(mut entry) => {
                if native_run_generation_sort_key(&run.run_id)
                    >= native_run_generation_sort_key(&entry.get().run_id)
                {
                    entry.insert(run);
                }
            }
        }
    }
    newest.into_values().collect()
}

fn native_run_generation_for_dispatch(
    issue: &Issue,
    config: &ServiceConfig,
    requested_generation: Option<u32>,
) -> Option<u32> {
    let _ = (issue, config);
    requested_generation
}

fn refresh_native_run_linear_recovery_from_workpad(
    native_run: &mut NativeRunHandle,
    issue: &Issue,
    workpad_body: Option<&str>,
) {
    native_run.recovery.linear = Some(recover_native_linear_evidence(issue, workpad_body));
}

fn workpad_requests_reviewer_rework(body: &str) -> bool {
    let normalized = body.to_ascii_lowercase();
    normalized.contains("reviewer decision: **red")
        || normalized.contains("reviewer decision: red")
        || normalized.contains("return to planning")
        || normalized.contains("move the issue back to planning")
        || normalized.contains("reviewer findings requiring focused rework")
}

fn recover_native_workspace_evidence(
    inspection: &crate::workspace::WorkspaceInspection,
) -> RecoveredWorkspaceEvidence {
    RecoveredWorkspaceEvidence {
        path: inspection.path.display().to_string(),
        branch: inspection.current_branch.clone().map(BranchName),
        head_sha: inspection.head_sha.clone().map(GitSha),
        upstream_branch: inspection.upstream_branch.clone().map(BranchName),
        upstream_head_sha: inspection.upstream_head_sha.clone().map(GitSha),
        dirty: inspection.dirty,
    }
}

fn recover_existing_native_pr_handoff_for_issue(
    config: &ServiceConfig,
    issue: &Issue,
    linear: Option<&RecoveredLinearEvidence>,
) -> Option<RecoveredGitHubEvidence> {
    let repo = repository_scope_from_config(config)?;
    let workspace = WorkspaceManager::for_service(config);
    let workspace_key = workspace_key_for_identifier(&issue.identifier);
    let workspace_evidence = workspace
        .inspect_workspace_key(&workspace_key.0)
        .map_err(|err| {
            warn!(
                issue_identifier = %issue.identifier,
                error = %err,
                "native PR handoff dispatch guard workspace inspection failed"
            );
            err
        })
        .ok()
        .flatten()
        .map(|inspection| recover_native_workspace_evidence(&inspection));
    let github_host = GitHubToolHost::with_connections(config.meerkat.runtime_connections.clone());
    recover_native_github_evidence_from_durable_sources(
        &repo,
        linear,
        workspace_evidence.as_ref(),
        &[],
        &github_host,
    )
}

fn recover_recorded_native_pr_handoff(
    config: &ServiceConfig,
    issue: &Issue,
    linear: Option<&RecoveredLinearEvidence>,
    github: &dyn NativeStatusGitHubInspector,
) -> Result<Option<RecoveredGitHubEvidence>> {
    let Some(repo) = repository_scope_from_config(config) else {
        return Ok(None);
    };
    let Some(record) = crate::pr_handoff::load(config, issue, &repo)? else {
        return Ok(None);
    };
    if issue
        .branch_name
        .as_deref()
        .is_some_and(|branch| branch != record.head_branch.0)
    {
        return Err(FugueError::ResponseError(
            "Recorded PR association conflicts with the current issue branch".into(),
        ));
    }
    if linear
        .and_then(|evidence| evidence.workpad_pr_number)
        .is_some_and(|number| number != record.number)
    {
        return Err(FugueError::ResponseError(
            "Recorded PR association conflicts with current issue PR evidence".into(),
        ));
    }
    let workspace = WorkspaceManager::for_service(config)
        .inspect_workspace_key(&workspace_key_for_identifier(&issue.identifier).0)?
        .map(|inspection| recover_native_workspace_evidence(&inspection));
    if native_pr_lookup_branch(linear, workspace.as_ref())
        .is_some_and(|branch| *branch != record.head_branch)
    {
        return Err(FugueError::ResponseError(
            "Recorded PR association conflicts with the current workspace branch".into(),
        ));
    }
    let result = github.inspect_pr(crate::tools::InspectPrInput {
        repo: repo.clone(),
        pull_request: GitHubPullRequestRef::Number {
            number: record.number,
        },
    });
    match result {
        FugueToolResult::Ok {
            output: FugueToolOutput::GitHubInspectPr(output),
            ..
        } => {
            let pr = output.pull_request;
            if pr.repo != repo || pr.number != record.number || pr.head_branch != record.head_branch
            {
                return Err(FugueError::ResponseError(
                    "GitHub PR identity differs from the recorded issue association".into(),
                ));
            }
            // Never use a cached merge state, check result, or approval.
            Ok(Some(RecoveredGitHubEvidence {
                pull_request: pr,
                rejected_pull_requests: Vec::new(),
            }))
        }
        FugueToolResult::Err { error, .. } => Err(FugueError::ResponseError(error.message)),
        FugueToolResult::Ok { .. } => Err(FugueError::ResponseError(
            "GitHub PR inspection returned an unexpected result".into(),
        )),
    }
}

fn recover_existing_native_pr_monitor_handoff_for_issue(
    config: &ServiceConfig,
    issue: &Issue,
    linear: Option<&RecoveredLinearEvidence>,
    github: &dyn NativeStatusGitHubInspector,
) -> Option<RecoveredGitHubEvidence> {
    let repo = repository_scope_from_config(config)?;
    let workspace = WorkspaceManager::for_service(config);
    let workspace_key = workspace_key_for_identifier(&issue.identifier);
    let workspace_evidence = workspace
        .inspect_workspace_key(&workspace_key.0)
        .map_err(|err| {
            warn!(
                issue_identifier = %issue.identifier,
                error = %err,
                "native PR handoff monitor workspace inspection failed"
            );
            err
        })
        .ok()
        .flatten()
        .map(|inspection| recover_native_workspace_evidence(&inspection));
    recover_native_pr_monitor_github_evidence(&repo, linear, workspace_evidence.as_ref(), github)
}

fn recover_native_pr_monitor_github_evidence(
    repo: &GitHubRepoRef,
    linear: Option<&RecoveredLinearEvidence>,
    workspace: Option<&RecoveredWorkspaceEvidence>,
    github: &dyn NativeStatusGitHubInspector,
) -> Option<RecoveredGitHubEvidence> {
    let lookup_branch = native_pr_lookup_branch(linear, workspace);
    for pull_request_ref in native_pr_lookup_refs(linear, workspace, &[]) {
        let result = github.inspect_pr(crate::tools::InspectPrInput {
            repo: repo.clone(),
            pull_request: pull_request_ref,
        });
        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubInspectPr(output),
                ..
            } => {
                let mut rejected_pull_requests = Vec::new();
                if let Some(expected_branch) = lookup_branch
                    && output.pull_request.head_branch != *expected_branch
                {
                    rejected_pull_requests.push(RejectedPullRequestEvidence {
                        number: output.pull_request.number,
                        reason: format!(
                            "head branch `{}` does not match current issue/workspace branch `{}`",
                            output.pull_request.head_branch.0, expected_branch.0
                        ),
                    });
                }
                return Some(RecoveredGitHubEvidence {
                    pull_request: output.pull_request,
                    rejected_pull_requests,
                });
            }
            FugueToolResult::Err { error, .. } => {
                warn!(
                    message = %error.message,
                    "native PR handoff monitor GitHub PR evidence unavailable"
                );
            }
            FugueToolResult::Ok { output, .. } => {
                warn!(
                    ?output,
                    "native PR handoff monitor GitHub PR inspection returned unexpected output"
                );
            }
        }
    }
    None
}

#[cfg(test)]
fn recover_native_github_evidence(
    pull_request: GitHubPullRequestSnapshot,
) -> RecoveredGitHubEvidence {
    RecoveredGitHubEvidence {
        pull_request,
        rejected_pull_requests: Vec::new(),
    }
}

fn focused_pr_handoff_decision(
    candidates: Vec<GitHubPullRequestSnapshot>,
    expected_branch: Option<&BranchName>,
    expected_head_sha: Option<&GitSha>,
) -> Result<RecoveredGitHubEvidence> {
    let Some(expected_branch) = expected_branch else {
        return Err(FugueError::ResponseError(
            "native PR handoff cannot select a durable GitHub PR without current issue/workspace branch evidence"
                .to_string(),
        ));
    };
    let mut accepted = None;
    let mut rejected_pull_requests = Vec::new();
    for pull_request in candidates {
        let rejection = if pull_request.head_branch != *expected_branch {
            Some(format!(
                "head branch `{}` does not match current issue/workspace branch `{}`",
                pull_request.head_branch.0, expected_branch.0
            ))
        } else if pull_request.state != PullRequestState::Open {
            Some(format!(
                "state `{:?}` is not an open handoff PR",
                pull_request.state
            ))
        } else if let Some(expected_head_sha) = expected_head_sha
            && pull_request.head_sha != *expected_head_sha
        {
            Some(format!(
                "head SHA `{}` does not match current workspace head `{}`",
                pull_request.head_sha.0, expected_head_sha.0
            ))
        } else {
            None
        };
        if let Some(reason) = rejection {
            rejected_pull_requests.push(RejectedPullRequestEvidence {
                number: pull_request.number,
                reason,
            });
        } else if accepted.is_none() {
            accepted = Some(pull_request);
        } else {
            rejected_pull_requests.push(RejectedPullRequestEvidence {
                number: pull_request.number,
                reason: format!(
                    "duplicate open handoff PR for branch `{}`; PR #{} was selected",
                    expected_branch.0,
                    accepted
                        .as_ref()
                        .map(|pull_request: &GitHubPullRequestSnapshot| pull_request.number.0)
                        .unwrap_or_default()
                ),
            });
        }
    }
    let pull_request = accepted.ok_or_else(|| {
        FugueError::ResponseError(format!(
            "native PR handoff found no acceptable open PR for current issue/workspace branch `{}`",
            expected_branch.0
        ))
    })?;
    Ok(RecoveredGitHubEvidence {
        pull_request,
        rejected_pull_requests,
    })
}

fn parse_workpad_native_run_id(body: &str) -> Option<MobRunId> {
    body.lines()
        .filter_map(|line| {
            if line.contains("Native `mob/flow_run` run id:")
                && let Some(run_id) = parse_backticked_value_after(line, "Fugue run")
            {
                return Some(MobRunId(run_id));
            }
            let marker = "Native mob run:";
            parse_backticked_value_after(line, marker).map(MobRunId)
        })
        .next_back()
}

fn parse_backticked_value_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)? + marker.len();
    let rest = &line[start..];
    let open = rest.find('`')? + 1;
    let after_open = &rest[open..];
    let close = after_open.find('`')?;
    let value = after_open[..close].trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn workpad_mentions_pr(body: &str) -> bool {
    parse_first_pr_number(body).is_some()
}

fn parse_first_pr_number(body: &str) -> Option<GitHubPullRequestNumber> {
    workpad_pr_handoff_section_lines(body).find_map(parse_pr_number)
}

fn workpad_pr_handoff_section_lines(body: &str) -> impl Iterator<Item = &str> {
    let mut in_handoff_section = false;
    body.lines().filter(move |line| {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            let heading = trimmed.trim_start_matches('#').trim().to_ascii_lowercase();
            in_handoff_section = matches!(
                heading.as_str(),
                "pull request or handoff"
                    | "pull request"
                    | "pr handoff"
                    | "review / pr handoff evidence"
            );
            return false;
        }
        in_handoff_section
    })
}

fn infer_pull_request_number_from_events(
    events: &[MobEventSnapshot],
) -> Option<GitHubPullRequestNumber> {
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|event| event.sequence);
    ordered.into_iter().find_map(|event| {
        if !matches!(
            event.kind,
            crate::tools::MobEventKind::ToolCallCompleted {
                operation: FugueToolOperation::GitHubCreatePr
            }
        ) {
            return None;
        }
        event.message.as_deref().and_then(parse_pr_number)
    })
}

fn parse_pr_number(message: &str) -> Option<GitHubPullRequestNumber> {
    let bytes = message.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let start = if bytes[index] == b'#' {
            Some(index + 1)
        } else if bytes[index..].starts_with(b"/pull/") {
            Some(index + b"/pull/".len())
        } else {
            None
        };
        if let Some(start) = start {
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(digits) = std::str::from_utf8(&bytes[start..end])
                && let Ok(number) = digits.parse::<u64>()
            {
                return Some(GitHubPullRequestNumber(number));
            }
        }
        index += 1;
    }
    None
}

fn recover_native_github_evidence_from_durable_sources(
    repo: &GitHubRepoRef,
    linear: Option<&RecoveredLinearEvidence>,
    workspace: Option<&RecoveredWorkspaceEvidence>,
    events: &[MobEventSnapshot],
    github: &dyn NativeStatusGitHubInspector,
) -> Option<RecoveredGitHubEvidence> {
    let lookup_branch = native_pr_lookup_branch(linear, workspace);
    for pull_request_ref in native_pr_lookup_refs(linear, workspace, events) {
        let result = github.inspect_pr(crate::tools::InspectPrInput {
            repo: repo.clone(),
            pull_request: pull_request_ref,
        });
        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubInspectPr(output),
                ..
            } => {
                let expected_branch = lookup_branch;
                let expected_head_sha = workspace.and_then(|workspace| {
                    if workspace.branch.as_ref() == expected_branch {
                        workspace.head_sha.as_ref()
                    } else {
                        None
                    }
                });
                match focused_pr_handoff_decision(
                    vec![output.pull_request],
                    expected_branch,
                    expected_head_sha,
                ) {
                    Ok(evidence) => return Some(evidence),
                    Err(err) => {
                        warn!(
                            message = %err,
                            "native durable GitHub PR evidence rejected"
                        );
                    }
                }
            }
            FugueToolResult::Err { error, .. } => {
                warn!(
                    message = %error.message,
                    "native durable GitHub PR evidence unavailable"
                );
            }
            FugueToolResult::Ok { output, .. } => {
                warn!(
                    ?output,
                    "native durable GitHub PR inspection returned unexpected output"
                );
            }
        }
    }
    None
}

fn native_pr_lookup_refs(
    linear: Option<&RecoveredLinearEvidence>,
    workspace: Option<&RecoveredWorkspaceEvidence>,
    events: &[MobEventSnapshot],
) -> Vec<GitHubPullRequestRef> {
    let mut refs = Vec::new();
    if let Some(head_branch) = native_pr_lookup_branch(linear, workspace) {
        refs.push(GitHubPullRequestRef::Branch {
            head_branch: head_branch.clone(),
        });
    }
    if let Some(number) = linear.and_then(|linear| linear.workpad_pr_number) {
        push_unique_pr_ref(&mut refs, GitHubPullRequestRef::Number { number });
    }
    if let Some(number) = infer_pull_request_number_from_events(events) {
        push_unique_pr_ref(&mut refs, GitHubPullRequestRef::Number { number });
    }
    refs
}

fn push_unique_pr_ref(refs: &mut Vec<GitHubPullRequestRef>, next: GitHubPullRequestRef) {
    if !refs.iter().any(|existing| existing == &next) {
        refs.push(next);
    }
}

fn native_pr_lookup_branch<'a>(
    linear: Option<&'a RecoveredLinearEvidence>,
    workspace: Option<&'a RecoveredWorkspaceEvidence>,
) -> Option<&'a BranchName> {
    workspace
        .and_then(|workspace| workspace.branch.as_ref())
        .filter(|branch| !matches!(branch.0.as_str(), "" | "main" | "master"))
        .or_else(|| {
            linear
                .filter(|linear| linear.workpad_mentions_pr || linear.workpad_pr_number.is_some())
                .and_then(|linear| linear.branch_name.as_ref())
        })
}

fn repository_scope_from_recovery_inputs(
    config: &ServiceConfig,
    github: Option<&RecoveredGitHubEvidence>,
) -> Option<GitHubRepoRef> {
    github
        .map(|github| github.pull_request.repo.clone())
        .or_else(|| repository_scope_from_config(config))
}

fn native_status_recovery_from_durable_evidence(
    run: &MobRunSnapshot,
    evidence: NativeDurableEvidence,
    max_review_cycles: Option<u64>,
) -> NativeStatusRecovery {
    let NativeDurableEvidence {
        run: durable_run,
        linear,
        github,
        workspace,
        events,
    } = evidence;
    let source_run = durable_run.as_ref().unwrap_or(run);
    native_status_recovery(
        source_run,
        linear,
        github,
        workspace,
        &events,
        max_review_cycles,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn refresh_native_status_recovery(
    run: &MobRunSnapshot,
    issue: &Issue,
    config: &ServiceConfig,
    workspace: &WorkspaceManager,
    events: &[MobEventSnapshot],
    existing: &NativeStatusRecovery,
    workpad_body: Option<&str>,
    github_host: Option<&dyn NativeStatusGitHubInspector>,
) -> NativeStatusRecovery {
    let workspace_evidence = run
        .workspace_key
        .as_ref()
        .and_then(|workspace_key| {
            workspace
                .inspect_workspace_key(&workspace_key.0)
                .ok()
                .flatten()
        })
        .map(|inspection| recover_native_workspace_evidence(&inspection))
        .or_else(|| existing.workspace.clone());
    let linear_evidence = workpad_body
        .map(|body| {
            retain_existing_unresolved_review_finding(
                recover_native_linear_evidence(issue, Some(body)),
                existing.linear.as_ref(),
            )
        })
        .or_else(|| existing.linear.clone())
        .or_else(|| Some(recover_native_linear_evidence(issue, None)));
    let github_evidence = (|| {
        let repo = repository_scope_from_recovery_inputs(config, existing.github.as_ref())?;
        recover_native_github_evidence_from_durable_sources(
            &repo,
            linear_evidence.as_ref(),
            workspace_evidence.as_ref(),
            events,
            github_host?,
        )
    })()
    .or_else(|| existing.github.clone());
    let evidence = NativeDurableEvidence {
        run: Some(run.clone()),
        linear: linear_evidence,
        github: github_evidence,
        workspace: workspace_evidence,
        events: events.to_vec(),
    };
    native_status_recovery_from_durable_evidence(
        run,
        evidence,
        config
            .select_template_for_issue(issue)
            .ok()
            .and_then(|selected| selected.template.max_review_cycles),
    )
}

fn retain_existing_unresolved_review_finding(
    mut refreshed: RecoveredLinearEvidence,
    existing: Option<&RecoveredLinearEvidence>,
) -> RecoveredLinearEvidence {
    if refreshed.unresolved_review_finding.is_none()
        && refreshed.issue_state.eq_ignore_ascii_case("Rework")
        && let Some(existing_finding) = existing.and_then(|linear| {
            linear
                .unresolved_review_finding
                .as_ref()
                .filter(|finding| !finding.trim().is_empty())
        })
    {
        refreshed.unresolved_review_finding = Some(existing_finding.clone());
    }
    refreshed
}

fn infer_pull_request_ref_from_events(events: &[MobEventSnapshot]) -> Option<GitHubPullRequestRef> {
    infer_pull_request_number_from_events(events)
        .map(|number| GitHubPullRequestRef::Number { number })
}

fn native_status_review_reason(
    run: &MobRunSnapshot,
    linear: Option<&RecoveredLinearEvidence>,
    github: Option<&RecoveredGitHubEvidence>,
    workspace: Option<&RecoveredWorkspaceEvidence>,
    events: &[MobEventSnapshot],
    max_review_cycles: Option<u64>,
) -> Option<NativeStatusReviewEvidence> {
    if let Some(linear) = linear {
        if let Some(workpad_run_id) = &linear.workpad_native_run_id
            && workpad_run_id != &run.run_id
        {
            return Some(NativeStatusReviewEvidence {
                reason: format!(
                    "Linear workpad native run id {} disagrees with Meta run id {}",
                    workpad_run_id.0, run.run_id.0
                ),
            });
        }
        if let Some(github) = github {
            if !github.rejected_pull_requests.is_empty() {
                return Some(NativeStatusReviewEvidence {
                    reason: format_rejected_pr_review_reason(&github.rejected_pull_requests),
                });
            }
            if !native_pr_state_is_accepted(github.pull_request.state) {
                return Some(NativeStatusReviewEvidence {
                    reason: format!(
                        "durable GitHub PR #{} is `{:?}`, not an open or merged handoff PR",
                        github.pull_request.number.0, github.pull_request.state
                    ),
                });
            }
            if github.pull_request.draft && !native_run_status_is_active(run.status) {
                return Some(NativeStatusReviewEvidence {
                    reason: format!(
                        "durable GitHub PR #{} is draft and still requires the reviewer/repo gate before completion",
                        github.pull_request.number.0
                    ),
                });
            }
        }
        if linear.issue_state.eq_ignore_ascii_case("Rework")
            && let Some(finding) = &linear.unresolved_review_finding
        {
            return Some(NativeStatusReviewEvidence {
                reason: format!(
                    "Linear issue is in Rework with unresolved operator review finding: {finding}"
                ),
            });
        }
    }
    if let (Some(linear), Some(github)) = (linear, github) {
        if let Some(workpad_pr_number) = linear.workpad_pr_number
            && github.pull_request.number != workpad_pr_number
        {
            return Some(NativeStatusReviewEvidence {
                reason: format!(
                    "Linear workpad PR #{} disagrees with durable GitHub PR #{}",
                    workpad_pr_number.0, github.pull_request.number.0
                ),
            });
        }
        if let Some(linear_branch) = &linear.branch_name
            && github.pull_request.head_branch != *linear_branch
            && !workspace_branch_matches_github_head(workspace, github)
        {
            return Some(NativeStatusReviewEvidence {
                reason: format!(
                    "Linear branch {} disagrees with GitHub PR head branch {}",
                    linear_branch.0, github.pull_request.head_branch.0
                ),
            });
        }
    }
    if let (Some(workspace), Some(github)) = (workspace, github)
        && let Some(head_sha) = &workspace.head_sha
        && github.pull_request.head_sha != *head_sha
    {
        return Some(NativeStatusReviewEvidence {
            reason: format!(
                "workspace head SHA {} disagrees with GitHub PR head SHA {}",
                head_sha.0, github.pull_request.head_sha.0
            ),
        });
    }
    if let Some(ref_from_events) = infer_pull_request_ref_from_events(events)
        && let (
            Some(GitHubPullRequestRef::Number {
                number: event_number,
            }),
            Some(github),
        ) = (Some(ref_from_events), github)
        && github.pull_request.number != event_number
    {
        return Some(NativeStatusReviewEvidence {
            reason: format!(
                "Meta GitHub PR event #{} disagrees with durable GitHub PR #{}",
                event_number.0, github.pull_request.number.0
            ),
        });
    }
    let pr_handoff_claimed = infer_pull_request_number_from_events(events).is_some()
        || linear.is_some_and(|linear| linear.workpad_mentions_pr);
    if pr_handoff_claimed && github.is_none() {
        return Some(NativeStatusReviewEvidence {
            reason: "Meta/Linear report PR handoff, but durable GitHub PR evidence is unavailable"
                .to_string(),
        });
    }
    let evidence = inspect_native_mob_e2e_contract(events, max_review_cycles);
    if run.status == MobRunStatus::Succeeded && evidence.completed_successfully {
        if let Some(linear) = linear
            && !matches!(linear.issue_state.as_str(), "Done")
            && github.is_none()
            && !linear.workpad_mentions_pr
        {
            return Some(NativeStatusReviewEvidence {
                reason: format!(
                    "Meta reports success but Linear issue remains in state {} without durable PR handoff evidence",
                    linear.issue_state
                ),
            });
        }
        if evidence.opened_pr_count == 1 && github.is_none() {
            let workpad_mentions_pr = linear.is_some_and(|linear| linear.workpad_mentions_pr);
            if !workpad_mentions_pr {
                return Some(NativeStatusReviewEvidence {
                    reason: "Meta reports a successful native run with one PR, but durable GitHub/workpad PR evidence is missing".to_string(),
                });
            }
        }
        if !evidence.review_rework_loop_is_bounded_and_routed() {
            return Some(NativeStatusReviewEvidence {
                reason: native_retry_reason(run.status, Some(&evidence), events),
            });
        }
    }
    None
}

fn workspace_branch_matches_github_head(
    workspace: Option<&RecoveredWorkspaceEvidence>,
    github: &RecoveredGitHubEvidence,
) -> bool {
    workspace
        .and_then(|workspace| workspace.branch.as_ref())
        .is_some_and(|workspace_branch| workspace_branch == &github.pull_request.head_branch)
}

fn native_pr_state_is_accepted(state: PullRequestState) -> bool {
    matches!(state, PullRequestState::Open | PullRequestState::Merged)
}

fn format_rejected_pr_review_reason(rejected: &[RejectedPullRequestEvidence]) -> String {
    let mut reason = "native PR handoff rejected conflicting pull request evidence".to_string();
    for rejected in rejected.iter().take(5) {
        reason.push_str(&format!(
            "; rejected PR #{}: {}",
            rejected.number.0, rejected.reason
        ));
    }
    reason
}

fn native_status_recovery(
    run: &MobRunSnapshot,
    linear: Option<RecoveredLinearEvidence>,
    github: Option<RecoveredGitHubEvidence>,
    workspace: Option<RecoveredWorkspaceEvidence>,
    events: &[MobEventSnapshot],
    max_review_cycles: Option<u64>,
) -> NativeStatusRecovery {
    let review = native_status_review_reason(
        run,
        linear.as_ref(),
        github.as_ref(),
        workspace.as_ref(),
        events,
        max_review_cycles,
    );
    NativeStatusRecovery {
        workspace,
        linear,
        github,
        review,
    }
}

fn issue_identifier_from_ref(issue: &LinearIssueRef) -> Option<String> {
    match issue {
        LinearIssueRef::Identifier { identifier } => Some(identifier.0.clone()),
        LinearIssueRef::Id { .. } => None,
    }
}

fn issue_ref_for_identifier(identifier: &str) -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier(identifier.to_string()),
    }
}

fn workspace_key_for_identifier(identifier: &str) -> WorkspaceKey {
    WorkspaceKey(sanitize_workspace_key(identifier))
}

async fn materialize_issue_attachments_for_native_launch(
    tracker: &TrackerConfig,
    issue: &Issue,
    workspace_path: &Path,
) -> Result<Issue> {
    if issue.attachments.is_empty() {
        return Ok(issue.clone());
    }
    IssueAdapter::from_config(tracker)?
        .materialize_issue_attachments(issue, workspace_path)
        .await
}

fn native_issue_observation_attempt(_issue: &Issue) -> u32 {
    1
}

fn worker_cursor(
    identifier: &str,
    event: &str,
    attempt: u32,
    observed_at: DateTime<Utc>,
) -> DurableCursorPosition {
    DurableCursorPosition {
        key: DurableCursorKey {
            source: DurableCursorSource::WorkerEvents,
            partition: CursorPartition(sanitize_workspace_key(identifier)),
        },
        cursor: Cursor(format!("{identifier}:{event}:{attempt}")),
        observed_at,
    }
}

fn worker_finished_signal(
    identifier: &str,
    exit: WorkerExit,
    error: Option<String>,
    attempt: u32,
    terminal_issue_state: bool,
    observed_at: DateTime<Utc>,
) -> KapellmeisterShadowSignal {
    let issue = issue_ref_for_identifier(identifier);
    let workspace_key = workspace_key_for_identifier(identifier);
    let source_cursor = Some(worker_cursor(identifier, "finished", attempt, observed_at));
    match exit {
        WorkerExit::Normal => {
            let outcome = if terminal_issue_state {
                ShadowWorkerOutcome::CompletedWithTerminalIssueState
            } else {
                ShadowWorkerOutcome::Succeeded
            };
            KapellmeisterShadowSignal::worker_finished(
                observed_at,
                source_cursor,
                issue,
                workspace_key,
                outcome,
            )
        }
        WorkerExit::Failed
        | WorkerExit::TimedOut
        | WorkerExit::Stalled
        | WorkerExit::CanceledByReconciliation => KapellmeisterShadowSignal::worker_failed(
            observed_at,
            source_cursor,
            issue,
            workspace_key,
            ShadowWorkerFailure {
                reason: worker_failure_reason(exit),
                message: error,
                retry_scheduled: !terminal_issue_state,
            },
        ),
    }
}

fn native_liveness_blocked_signal(
    identifier: &str,
    reason: NativeRunParkReason,
    detail: Option<&str>,
    attempt: u32,
    observed_at: DateTime<Utc>,
) -> KapellmeisterShadowSignal {
    KapellmeisterShadowSignal::worker_failed(
        observed_at,
        Some(worker_cursor(
            identifier,
            "native_liveness_blocked",
            attempt,
            observed_at,
        )),
        issue_ref_for_identifier(identifier),
        workspace_key_for_identifier(identifier),
        ShadowWorkerFailure {
            reason: ShadowWorkerFailureReason::InfrastructureUnavailable,
            message: Some(native_liveness_signal_message(reason, detail)),
            retry_scheduled: false,
        },
    )
}

fn native_liveness_signal_message(reason: NativeRunParkReason, detail: Option<&str>) -> String {
    let mut message = native_run_park_reason(reason).to_string();
    if let Some(detail) = detail {
        message.push_str("; detail=");
        message.push_str(&native_liveness_detail_for_workpad(detail));
    }
    message
}

fn native_pr_handoff_missing_pr_signal(
    identifier: &str,
    observed_at: DateTime<Utc>,
) -> KapellmeisterShadowSignal {
    KapellmeisterShadowSignal::worker_failed(
        observed_at,
        Some(worker_cursor(
            identifier,
            "native_pr_handoff_missing_pr_evidence",
            0,
            observed_at,
        )),
        issue_ref_for_identifier(identifier),
        workspace_key_for_identifier(identifier),
        ShadowWorkerFailure {
            reason: ShadowWorkerFailureReason::InfrastructureUnavailable,
            message: Some(
                "Workpad/Linear claims a PR handoff, but durable GitHub PR evidence is missing; host did not move the Linear lane"
                    .to_string(),
            ),
            retry_scheduled: false,
        },
    )
}

fn worker_failure_reason(exit: WorkerExit) -> ShadowWorkerFailureReason {
    match exit {
        WorkerExit::Normal => ShadowWorkerFailureReason::WorkerError,
        WorkerExit::Failed => ShadowWorkerFailureReason::WorkerError,
        WorkerExit::TimedOut => ShadowWorkerFailureReason::TimedOut,
        WorkerExit::Stalled => ShadowWorkerFailureReason::Stalled,
        WorkerExit::CanceledByReconciliation => ShadowWorkerFailureReason::ReconciliationCancelled,
    }
}

fn classify_issue_policy_mode(issue: &Issue, config: &ServiceConfig) -> IssuePolicyMode {
    if native_kapellmeister_prompt_control_enabled(config) {
        return IssuePolicyMode::Kapellmeister;
    }
    classify_policy_mode(
        &config.kapellmeister.shadow_mode.authority,
        &kapellmeister_context_for_issue(issue, repository_scope_from_config(config)),
    )
}

fn dispatch_backend_for_issue(issue: &Issue, config: &ServiceConfig) -> DispatchBackend {
    match config.select_template_for_issue(issue) {
        Ok(selected) if selected.template.backend == MobExecutionBackend::NativeExecution => {
            DispatchBackend::NativeMob
        }
        _ => DispatchBackend::NativeMob,
    }
}

fn recover_native_running_entry(
    run: &MobRunSnapshot,
    issues: &[Issue],
    config: &ServiceConfig,
    workspace: &WorkspaceManager,
    workpad_body: Option<&str>,
) -> Option<(Issue, RunningEntry)> {
    if !native_run_status_is_active(run.status) {
        return None;
    }
    let issue_ref = run.issue.as_ref()?;
    let issue = match issue_ref {
        LinearIssueRef::Identifier { identifier } => issues
            .iter()
            .find(|issue| issue.identifier == identifier.0)?,
        LinearIssueRef::Id { id } => issues.iter().find(|issue| issue.id == id.0)?,
    };
    if config.tracker.is_terminal_state(&issue.state) {
        return None;
    }
    if !matches!(
        dispatch_backend_for_issue(issue, config),
        DispatchBackend::NativeMob
    ) {
        return None;
    }
    let linear_evidence = recover_native_linear_evidence(issue, workpad_body);
    if native_pr_handoff_blocks_fresh_dispatch_for_evidence(config, issue, &linear_evidence, false)
    {
        return None;
    }
    let workspace_key = run
        .workspace_key
        .clone()
        .unwrap_or_else(|| workspace_key_for_identifier(&issue.identifier));
    let inspection = workspace
        .inspect_workspace_key(&workspace_key.0)
        .ok()
        .flatten()?;
    let workspace_evidence = recover_native_workspace_evidence(&inspection);
    let session = LiveSession {
        session_id: Some(run.run_id.0.clone()),
        last_meerkat_event: Some("native_mob_recovered_after_restart".to_string()),
        last_meerkat_timestamp: Some(Utc::now()),
        last_meerkat_message: Some(format!(
            "recovered native execution mob {} for {} after restart",
            run.run_id.0, issue.identifier
        )),
        ..LiveSession::default()
    };
    Some((
        issue.clone(),
        RunningEntry {
            issue: issue.clone(),
            identifier: issue.identifier.clone(),
            policy_mode: classify_issue_policy_mode(issue, config),
            workspace_path: inspection.path.display().to_string(),
            session,
            started_at: run.started_at.unwrap_or(run.updated_at),
            retry_attempt: None,
            cancel: CancellationToken::new(),
            handle: None,
            termination: None,
            native_run: Some(NativeRunHandle {
                run_id: run.run_id.clone(),
                observed_events: Vec::new(),
                recovery: native_status_recovery(
                    run,
                    Some(linear_evidence),
                    None,
                    Some(workspace_evidence),
                    &[],
                    config
                        .select_template_for_issue(issue)
                        .ok()
                        .and_then(|selected| selected.template.max_review_cycles),
                ),
            }),
        },
    ))
}

fn runtime_recovered_signal(
    config: &ServiceConfig,
    issue: &Issue,
    run: &MobRunSnapshot,
    native_run: Option<&NativeRunHandle>,
) -> KapellmeisterShadowSignal {
    let observed_at = run.updated_at;
    let projected_lane = native_run.and_then(|native_run| {
        projected_native_lane(config, issue, native_run, run.status)
            .map(|name| LinearStateRef::Name { name })
    });
    KapellmeisterShadowSignal::runtime_recovered(
        observed_at,
        Some(worker_cursor(
            &issue.identifier,
            "runtime_recovered",
            native_issue_observation_attempt(issue),
            observed_at,
        )),
        issue_ref_for_identifier(&issue.identifier),
        run.run_id.clone(),
        LinearStateRef::Name {
            name: issue.state.clone(),
        },
        projected_lane,
        native_run.and_then(|native_run| {
            native_run
                .recovery
                .workspace
                .as_ref()
                .map(kapellmeister_workspace_evidence)
        }),
    )
}

fn kapellmeister_workspace_evidence(
    workspace: &RecoveredWorkspaceEvidence,
) -> KapellmeisterWorkspaceEvidence {
    KapellmeisterWorkspaceEvidence {
        path: workspace.path.clone(),
        branch: workspace.branch.clone(),
        head_sha: workspace.head_sha.clone(),
        upstream_branch: workspace.upstream_branch.clone(),
        upstream_head_sha: workspace.upstream_head_sha.clone(),
        dirty: workspace.dirty,
    }
}

fn projected_native_lane(
    config: &ServiceConfig,
    issue: &Issue,
    native_run: &NativeRunHandle,
    status: MobRunStatus,
) -> Option<String> {
    if config.tracker.is_terminal_state(&issue.state) {
        return None;
    }
    let selected = config.select_template_for_issue(issue).ok()?;
    evaluate_issue_projection(selected.template, status, &native_run.observed_events)
}

fn classify_policy_mode(
    _authority: &KapellmeisterToolAuthority,
    _context: &KapellmeisterDecisionContext,
) -> IssuePolicyMode {
    IssuePolicyMode::Kapellmeister
}

fn kapellmeister_context_for_issue(
    issue: &Issue,
    repository: Option<GitHubRepoRef>,
) -> KapellmeisterDecisionContext {
    let issue_ref = issue_ref_for_identifier(&issue.identifier);
    KapellmeisterDecisionContext {
        issue: Some(issue_ref.clone()),
        repository,
        linear_labels: issue.labels.clone(),
        workspace_key: Some(workspace_key_for_identifier(&issue.identifier)),
        observations: vec![KapellmeisterHostObservation::LinearIssueObserved {
            issue: issue_ref,
            state: LinearStateRef::Name {
                name: issue.state.clone(),
            },
            labels: issue.labels.clone(),
        }],
    }
}

fn repository_scope_from_config(config: &ServiceConfig) -> Option<GitHubRepoRef> {
    config
        .kapellmeister
        .shadow_mode
        .authority
        .safe_selectors
        .repositories
        .first()
        .cloned()
}

fn interpret_human_lane_move(
    tracker: &crate::config::TrackerConfig,
    state: &str,
) -> HumanLaneAction {
    if tracker.is_terminal_state(state) {
        HumanLaneAction::StopAndCleanup
    } else if tracker.is_active_state(state) {
        HumanLaneAction::Continue
    } else {
        HumanLaneAction::StopNoRetry
    }
}

fn interpret_native_prompt_control_lane(
    tracker: &crate::config::TrackerConfig,
    state: &str,
) -> HumanLaneAction {
    if tracker.is_terminal_state(state) {
        HumanLaneAction::StopAndCleanup
    } else {
        HumanLaneAction::Continue
    }
}

fn native_compact_lane_repair_target(
    config: &ServiceConfig,
    issue: &Issue,
    native_run: &NativeRunHandle,
    status: MobRunStatus,
) -> Option<String> {
    if config.tracker.is_terminal_state(&issue.state) {
        return None;
    }
    let selected = config.select_template_for_issue(issue).ok()?;
    let projected =
        evaluate_issue_projection(selected.template, status, &native_run.observed_events)?;
    (normalize_state(&issue.state) != normalize_state(&projected)).then_some(projected)
}

fn should_dispatch_locked(
    issue: &Issue,
    state: &OrchestratorState,
    config: &ServiceConfig,
    allow_claimed: bool,
) -> bool {
    if issue.id.is_empty()
        || issue.identifier.is_empty()
        || issue.title.is_empty()
        || issue.state.is_empty()
    {
        return false;
    }
    let dispatch_state_allowed = match dispatch_backend_for_issue(issue, config) {
        DispatchBackend::NativeMob => {
            config.tracker.is_intake_state(&issue.state)
                || config.tracker.is_active_state(&issue.state)
        }
    };
    if !dispatch_state_allowed
        || config.tracker.is_terminal_state(&issue.state)
        || state.running.contains_key(&issue.id)
        || (!allow_claimed && state.claimed.contains(&issue.id))
    {
        return false;
    }
    if is_todo_state(&issue.state)
        && issue.blocked_by.iter().any(|blocker| {
            blocker
                .state
                .as_deref()
                .is_none_or(|state| !config.tracker.is_terminal_state(state))
        })
    {
        return false;
    }
    has_available_slot_locked(issue, state, config, allow_claimed)
}

fn kapellmeister_observation_suppresses_dispatch(
    policy_mode: IssuePolicyMode,
    _backend: DispatchBackend,
) -> bool {
    matches!(policy_mode, IssuePolicyMode::Kapellmeister)
}

fn native_kapellmeister_prompt_control_enabled(config: &ServiceConfig) -> bool {
    if !config.mob.native.enabled || config.mob.templates.is_empty() {
        return false;
    }
    if matches!(
        config.kapellmeister.shadow_mode.authority.mode,
        KapellmeisterAuthorityMode::GuardedLabeled
    ) {
        return true;
    }
    config.mob.templates.values().any(|template| {
        matches!(template.backend, MobExecutionBackend::NativeExecution) && template.flow.is_none()
    })
}

fn has_available_slot_locked(
    issue: &Issue,
    state: &OrchestratorState,
    config: &ServiceConfig,
    _allow_claimed: bool,
) -> bool {
    let global_available = config
        .agent
        .max_concurrent_agents
        .saturating_sub(state.running.len());
    if global_available == 0 {
        return false;
    }
    let normalized = normalize_state(&issue.state);
    let state_limit = config
        .agent
        .max_concurrent_agents_by_state
        .get(&normalized)
        .copied()
        .unwrap_or(config.agent.max_concurrent_agents);
    let running_in_state = state
        .running
        .values()
        .filter(|entry| normalize_state(&entry.issue.state) == normalized)
        .count();
    running_in_state < state_limit
}

pub fn sort_for_dispatch(issues: &mut [Issue]) {
    issues.sort_by(|a, b| {
        let priority = a
            .priority
            .unwrap_or(i64::MAX)
            .cmp(&b.priority.unwrap_or(i64::MAX));
        if priority != Ordering::Equal {
            return priority;
        }
        let created = match (a.created_at, b.created_at) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if created != Ordering::Equal {
            return created;
        }
        a.identifier.cmp(&b.identifier)
    });
}

fn merge_candidate_issues<I>(issues: &mut Vec<Issue>, extras: I)
where
    I: IntoIterator<Item = Issue>,
{
    let mut seen = issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect::<HashSet<_>>();
    for issue in extras {
        if seen.insert(issue.id.clone()) {
            issues.push(issue);
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RuntimeSnapshot {
    pub generated_at: DateTime<Utc>,
    pub counts: SnapshotCounts,
    pub kapellmeister_shadow: KapellmeisterShadowInspection,
    pub running: Vec<RunningRow>,
    pub retrying: Vec<RetryRow>,
    pub meerkat_totals: SnapshotTotals,
    pub rate_limits: Option<Value>,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotCounts {
    pub running: usize,
    pub retrying: usize,
}

#[derive(Debug, Serialize)]
pub struct RunningRow {
    pub issue_id: String,
    pub issue_identifier: String,
    pub issue: RunningIssueRow,
    pub workspace_key: String,
    pub lifecycle: RunningLifecycleRow,
    pub policy_mode: IssuePolicyMode,
    pub backend: StatusBackend,
    pub state: String,
    pub session_id: Option<String>,
    pub session_ref: Option<String>,
    pub stream_id: Option<String>,
    pub turn_count: u64,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub tokens: TokenTotals,
    pub native_recovery: Option<NativeRecoveryStatusRow>,
}

#[derive(Debug, Serialize)]
pub struct RunningIssueRow {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RunningLifecycleRow {
    pub lane: String,
    pub status: RunningLifecycleStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunningLifecycleStatus {
    Running,
    Terminating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusBackend {
    NativeMob,
}

#[derive(Debug, Serialize)]
pub struct NativeRecoveryStatusRow {
    pub run_id: String,
    pub workspace_path: Option<String>,
    pub workspace_branch: Option<String>,
    pub workspace_head_sha: Option<String>,
    pub workspace_dirty: Option<bool>,
    pub linear_branch: Option<String>,
    pub workpad_run_id: Option<String>,
    pub workpad_mentions_pr: bool,
    pub workpad_pr_number: Option<u64>,
    pub unresolved_review_finding: Option<String>,
    pub github_pr_url: Option<String>,
    pub github_pr_state: Option<PullRequestState>,
    pub github_head_sha: Option<String>,
    pub rejected_prs: Vec<NativeRejectedPrRow>,
    pub review_required: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NativeRejectedPrRow {
    pub number: u64,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct RetryRow {
    pub issue_id: String,
    pub issue_identifier: String,
    pub policy_mode: IssuePolicyMode,
    pub attempt: u32,
    pub due_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub seconds_running: f64,
}

#[derive(Debug, Serialize)]
pub struct IssueDebugSnapshot {
    pub issue_identifier: String,
    pub issue_id: String,
    pub status: String,
    pub workspace: IssueWorkspaceSnapshot,
    pub attempts: IssueAttemptsSnapshot,
    pub running: Option<RunningRow>,
    pub retry: Option<RetryRow>,
    pub recent_events: Vec<RecentEvent>,
    pub native_events: Vec<MobEventSnapshot>,
    pub last_error: Option<String>,
    pub tracked: Value,
}

#[derive(Debug, Serialize)]
pub struct IssueWorkspaceSnapshot {
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct IssueAttemptsSnapshot {
    pub restart_count: u32,
    pub current_retry_attempt: Option<u32>,
}

pub async fn snapshot(shared: &SharedState, config: &ServiceConfig) -> RuntimeSnapshot {
    let shadow = inspect_shadow_state(&config.kapellmeister.shadow_mode).unwrap_or_else(|err| {
        let mut inspection = KapellmeisterShadowInspection::no_state(
            config.kapellmeister.shadow_mode.state_path.clone(),
            config.kapellmeister.shadow_mode.authority.clone(),
        );
        inspection.session_ref = Some(format!("inspection_error: {err}"));
        inspection
    });
    let Ok(state) = shared.try_read() else {
        return RuntimeSnapshot {
            generated_at: Utc::now(),
            counts: SnapshotCounts {
                running: 0,
                retrying: 0,
            },
            kapellmeister_shadow: shadow,
            running: Vec::new(),
            retrying: Vec::new(),
            meerkat_totals: SnapshotTotals {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                seconds_running: 0.0,
            },
            rate_limits: None,
            last_error: Some(
                "runtime state snapshot is temporarily unavailable because the orchestrator lock is busy"
                    .to_string(),
            ),
        };
    };
    let now = Utc::now();
    let mut running = state
        .running
        .iter()
        .map(|(issue_id, entry)| running_row(issue_id, entry))
        .collect::<Vec<_>>();
    sort_running_rows(&mut running);
    let mut retrying = state
        .retry_attempts
        .iter()
        .map(|(issue_id, entry)| retry_row(issue_id, entry))
        .collect::<Vec<_>>();
    sort_retry_rows(&mut retrying);
    let active_seconds: f64 = state
        .running
        .values()
        .map(|entry| (now - entry.started_at).num_milliseconds().max(0) as f64 / 1000.0)
        .sum();
    RuntimeSnapshot {
        generated_at: now,
        counts: SnapshotCounts {
            running: running.len(),
            retrying: retrying.len(),
        },
        kapellmeister_shadow: shadow,
        running,
        retrying,
        meerkat_totals: SnapshotTotals {
            input_tokens: state.meerkat_totals.input_tokens,
            output_tokens: state.meerkat_totals.output_tokens,
            total_tokens: state.meerkat_totals.total_tokens,
            seconds_running: state.ended_runtime_seconds + active_seconds,
        },
        rate_limits: state.meerkat_rate_limits.clone(),
        last_error: state.last_error.clone(),
    }
}

pub async fn issue_snapshot(
    shared: &SharedState,
    issue_identifier: &str,
) -> Option<IssueDebugSnapshot> {
    let state = shared.read().await;
    let running = state
        .running
        .iter()
        .find(|(_, entry)| entry.identifier == issue_identifier)
        .map(|(issue_id, entry)| running_row(issue_id, entry));
    let retry_entry = state
        .retry_attempts
        .iter()
        .find(|(_, entry)| entry.identifier == issue_identifier);
    let retry = retry_entry.map(|(issue_id, entry)| retry_row(issue_id, entry));
    let (issue_id, workspace, status) = if let Some(row) = &running {
        (
            row.issue_id.clone(),
            state.running[&row.issue_id].workspace_path.clone(),
            "running".to_string(),
        )
    } else if let Some(row) = &retry {
        (
            row.issue_id.clone(),
            retry_entry
                .map(|(_, entry)| entry.workspace_path.clone())
                .unwrap_or_default(),
            "retrying".to_string(),
        )
    } else {
        return None;
    };
    let recent_events = state
        .recent_events
        .get(&issue_id)
        .cloned()
        .unwrap_or_default();
    let native_events = state
        .running
        .get(&issue_id)
        .and_then(|entry| entry.native_run.as_ref())
        .map(|native_run| native_run.observed_events.clone())
        .unwrap_or_default();
    Some(IssueDebugSnapshot {
        issue_identifier: issue_identifier.to_string(),
        issue_id,
        status,
        workspace: IssueWorkspaceSnapshot { path: workspace },
        attempts: IssueAttemptsSnapshot {
            restart_count: retry
                .as_ref()
                .map(|row| row.attempt.saturating_sub(1))
                .unwrap_or(0),
            current_retry_attempt: retry.as_ref().map(|row| row.attempt),
        },
        running,
        retry,
        recent_events,
        native_events,
        last_error: state.last_error.clone(),
        tracked: serde_json::json!({}),
    })
}

fn running_row(issue_id: &str, entry: &RunningEntry) -> RunningRow {
    RunningRow {
        issue_id: issue_id.to_string(),
        issue_identifier: entry.identifier.clone(),
        issue: RunningIssueRow {
            id: entry.issue.id.clone(),
            identifier: entry.issue.identifier.clone(),
            title: entry.issue.title.clone(),
            url: entry.issue.url.clone(),
        },
        workspace_key: workspace_key_for_identifier(&entry.identifier).0,
        lifecycle: RunningLifecycleRow {
            lane: entry.issue.state.clone(),
            status: if entry.termination.is_some() {
                RunningLifecycleStatus::Terminating
            } else {
                RunningLifecycleStatus::Running
            },
        },
        policy_mode: entry.policy_mode,
        backend: StatusBackend::NativeMob,
        state: entry.issue.state.clone(),
        session_id: entry.session.session_id.clone(),
        session_ref: entry.session.session_ref.clone(),
        stream_id: entry.session.stream_id.clone(),
        turn_count: entry.session.turn_count,
        last_event: entry.session.last_meerkat_event.clone(),
        last_message: running_row_last_message(entry),
        started_at: entry.started_at,
        last_event_at: entry.session.last_meerkat_timestamp,
        tokens: entry.session.meerkat_tokens.clone(),
        native_recovery: entry.native_run.as_ref().map(native_recovery_status_row),
    }
}

fn running_row_last_message(entry: &RunningEntry) -> Option<String> {
    match entry.session.last_meerkat_message.as_deref() {
        Some(message) if !is_stale_native_startup_delivery_message(message) => {
            Some(message.to_string())
        }
        _ => entry
            .native_run
            .as_ref()
            .and_then(native_recovery_status_message),
    }
}

fn is_stale_native_startup_delivery_message(message: &str) -> bool {
    message == "issue lead task delivery timed out after 15s"
        || message.starts_with("issue lead task delivery failed:")
}

fn native_recovery_status_message(native_run: &NativeRunHandle) -> Option<String> {
    if let Some(github) = &native_run.recovery.github {
        let pr = &github.pull_request;
        let head = short_sha(&pr.head_sha.0);
        return Some(format!(
            "native run {} tracks PR #{} ({}) at {}",
            native_run.run_id.0,
            pr.number.0,
            pull_request_state_label(pr.state),
            head
        ));
    }
    if let Some(review) = &native_run.recovery.review {
        return Some(format!(
            "native run {} needs review: {}",
            native_run.run_id.0, review.reason
        ));
    }
    if let Some(workspace) = &native_run.recovery.workspace {
        return Some(format!(
            "native run {} workspace {} ({})",
            native_run.run_id.0,
            workspace.path,
            workspace_cleanliness_label(workspace.dirty)
        ));
    }
    Some(format!("native run {} is active", native_run.run_id.0))
}

fn short_sha(value: &str) -> &str {
    value.get(..12).unwrap_or(value)
}

fn pull_request_state_label(state: PullRequestState) -> &'static str {
    match state {
        PullRequestState::Open => "open",
        PullRequestState::Merged => "merged",
        PullRequestState::Closed => "closed",
    }
}

fn workspace_cleanliness_label(dirty: bool) -> &'static str {
    if dirty { "dirty" } else { "clean" }
}

fn native_recovery_status_row(native_run: &NativeRunHandle) -> NativeRecoveryStatusRow {
    let recovery = &native_run.recovery;
    NativeRecoveryStatusRow {
        run_id: native_run.run_id.0.clone(),
        workspace_path: recovery
            .workspace
            .as_ref()
            .map(|workspace| workspace.path.clone()),
        workspace_branch: recovery
            .workspace
            .as_ref()
            .and_then(|workspace| workspace.branch.as_ref().map(|branch| branch.0.clone())),
        workspace_head_sha: recovery
            .workspace
            .as_ref()
            .and_then(|workspace| workspace.head_sha.as_ref().map(|sha| sha.0.clone())),
        workspace_dirty: recovery.workspace.as_ref().map(|workspace| workspace.dirty),
        linear_branch: recovery
            .linear
            .as_ref()
            .and_then(|linear| linear.branch_name.as_ref().map(|branch| branch.0.clone())),
        workpad_run_id: recovery.linear.as_ref().and_then(|linear| {
            linear
                .workpad_native_run_id
                .as_ref()
                .map(|run_id| run_id.0.clone())
        }),
        workpad_mentions_pr: recovery
            .linear
            .as_ref()
            .is_some_and(|linear| linear.workpad_mentions_pr),
        workpad_pr_number: recovery
            .linear
            .as_ref()
            .and_then(|linear| linear.workpad_pr_number.map(|number| number.0)),
        unresolved_review_finding: recovery
            .linear
            .as_ref()
            .and_then(|linear| linear.unresolved_review_finding.clone()),
        github_pr_url: recovery
            .github
            .as_ref()
            .map(|github| github.pull_request.url.clone()),
        github_pr_state: recovery
            .github
            .as_ref()
            .map(|github| github.pull_request.state),
        github_head_sha: recovery
            .github
            .as_ref()
            .map(|github| github.pull_request.head_sha.0.clone()),
        rejected_prs: recovery
            .github
            .as_ref()
            .map(|github| {
                github
                    .rejected_pull_requests
                    .iter()
                    .map(|rejected| NativeRejectedPrRow {
                        number: rejected.number.0,
                        reason: rejected.reason.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        review_required: recovery.review.as_ref().map(|review| review.reason.clone()),
    }
}

fn retry_row(issue_id: &str, entry: &RetryEntry) -> RetryRow {
    RetryRow {
        issue_id: issue_id.to_string(),
        issue_identifier: entry.identifier.clone(),
        policy_mode: entry.policy_mode,
        attempt: entry.attempt,
        due_at: entry.due_at,
        error: entry.error.clone(),
    }
}

fn sort_running_rows(rows: &mut [RunningRow]) {
    rows.sort_by(|a, b| {
        a.issue_identifier
            .cmp(&b.issue_identifier)
            .then_with(|| a.issue_id.cmp(&b.issue_id))
    });
}

fn sort_retry_rows(rows: &mut [RetryRow]) {
    rows.sort_by(|a, b| {
        a.due_at
            .cmp(&b.due_at)
            .then_with(|| a.issue_identifier.cmp(&b.issue_identifier))
            .then_with(|| a.issue_id.cmp(&b.issue_id))
    });
}

#[cfg(test)]
fn repo_kapellmeister_signal_message(
    signals: &[KapellmeisterShadowSignal],
) -> std::result::Result<String, serde_json::Error> {
    repo_kapellmeister_signal_message_with_lanes(signals, "Done", None, Some("Review"))
}

fn repo_kapellmeister_signal_message_with_lanes(
    signals: &[KapellmeisterShadowSignal],
    success: &str,
    review_ready: Option<&str>,
    independent_review_lane: Option<&str>,
) -> std::result::Result<String, serde_json::Error> {
    let payload = serde_json::to_string_pretty(signals)?;
    let handoff_instruction = match review_ready {
        Some(lane)
            if independent_review_lane.is_some_and(|review| same_issue_lane(review, lane)) =>
        {
            format!(
                "When implementation and its issue-team review are ready, record the PR evidence and move the issue to {lane} for independent specialist review. This shared holding status is a review request, never proof of approval. On first-seen, recovered or changed observations in {lane}, inspect the PR and current native review: admit or resume the required independent review, and retain an already completed current review instead of starting it again. Do not start a new implementation mob from this observation. Only an explicit merge action with verified review and merge evidence may project to {success}."
            )
        }
        Some(lane) => format!(
            "When reviewer and WorkGraph obligations are satisfied, move the issue to {lane} to await operator review. This is a holding lane, not a review action trigger. Keep it there until an explicit merge action; only verified merge completion projects to {success}."
        ),
        None => format!(
            "When reviewer and WorkGraph obligations are satisfied, move the issue to {success}. In this workspace that lane records accepted development; merging still requires a separate explicit action."
        ),
    };
    let review_instruction = if independent_review_lane.is_some() {
        "For a configured Review action or shared review holding status, inspect the issue/PR and current native review evidence, then use fugue_start_independent_review with a typed IndependentReviewStartRequest when review is required. The review coordinator must select the configured quorum from existing repository specialists through native admission; never create substitute reviewers. Reuse the same request identity after an uncertain response."
    } else {
        "The independent Review action is disabled in this workspace. Review is a holding lane only; do not start an independent review orchestrator or create three-reviewer quorum obligations. Ordinary issue-mob review still applies."
    };
    Ok(format!(
        "Fugue host signal batch for the repo-scoped Kapellmeister.\n\
         Treat this JSON as authoritative host-observed lifecycle input. \
         Use it to coordinate issue mobs, PR handoff, and operator guidance. \
         Do not depend on Linear lanes as lifecycle authority; Linear remains a projection. \
         Use one tool call at a time; do not issue parallel or batched tool calls in this signal turn. \
         If a linear_issue_first_seen or open_issue_without_runtime observation reports an intake, \
         control_active, or unexpected_nonterminal lane with no running_mob, call \
         fugue_start_issue_mob for that issue immediately unless the payload is a configured \
         Review action lane, Deploy lane, terminal, blocked, duplicate, or explicit pause reason. \
         If a lane observation is classified as attention_or_terminal, do not start an issue mob \
         from that observation alone. For a configured final-review/merge lane, wait for or act on \
         a final_pr_gate_requested signal and use fugue_start_final_pr_gate with its exact typed \
         request; do not substitute fugue_start_issue_mob. \
         {review_instruction} \
         For a configured Deploy lane, do not start the implementation mob and do not try to \
         orchestrate merge/OTA yourself; Fugue host handles Deploy as a guarded mechanical \
         transaction after verifying durable PR evidence, CI, WorkGraph blockers, and deploy \
         policy. \
         If a pr_handoff_monitor_changed signal reports ready_for_done, the PR is eligible for handoff. Inspect reviewer/WorkGraph evidence. {handoff_instruction} Report CI evidence literally: an empty check list means no checks were reported, not that checks ran successfully. \
         Do not wait for an external flow-step feeder. If an active issue reports missing concrete handoff, stale worker_started signals, or offline issue peers, actively inspect/ask the issue mob for status or decide resume/replan through Kapellmeister-owned coordination. \
         A linear_issue_moved signal means Fugue observed external projection drift from the \
         issue lane map. It is not proof that lifecycle work completed, but it is an \
         operator-visible intent/change that must be reconciled through Kapellmeister and \
         the issue lead rather than ignored as mere projection noise. \
         If a linear_issue_commented or linear_issue_comment_changed signal includes \
         operator_review_finding, treat that field as concrete issue rework input from an \
         operator/repo reviewer. Tell the issue lead to replan and drive worker changes from \
         that finding; do not classify it as a passive final-gate wait, acceptance hold, or \
         standby condition. \
         When writing Linear Workpad/status evidence from repo scope, pass the explicit issue \
         identifier from the signal payload to the Linear tool call. \
         You do not have shell/worktree authority. If reviewer-green evidence has no pushed \
         branch/head SHA yet, ask the issue lead to have the coder commit and push the reviewed \
         scoped changes before you call github_create_pr. If a runtime_recovered signal carries \
         workspace.branch, workspace.head_sha, workspace.upstream_branch, matching \
         workspace.upstream_head_sha, and dirty=false, treat that as durable pushed-branch \
         evidence. When that pushed-branch evidence is present and no PR exists for the branch, \
         create or update the PR yourself with github_create_pr; do not ask the issue lead for an \
         extra PR creation request. If github_create_pr returns an open PR, newly created or \
         already existing, tell the issue lead the PR number/URL/head branch and ask it to record \
         PR handoff evidence while keeping ordinary issue-mob work on the active projection lane. \
         Do not ask issue agents to move issues into configured Review, Deploy, or \
         final-review action lanes; those lanes are operator or host-observed control inputs. \
         Merge is not implicit repo authority: accepted review and CI evidence supports a handoff, \
         but it is not merge authorization. Do not merge a PR unless Deploy policy or an operator \
         explicitly approves that specific PR merge in the current session. \
         If a Meerkat mob operator tool such as list_members returns access_denied, treat that as a scope boundary and continue from these signals and comms peers.\n\n\
         ```json\n{payload}\n```"
    ))
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    #[test]
    fn repo_signal_prompt_uses_configured_success_lane() {
        let body = super::repo_kapellmeister_signal_message_with_lanes(
            &[],
            "Accepted",
            None,
            Some("Review"),
        )
        .expect("prompt");
        assert!(body.contains("move the issue to Accepted"));
        assert!(!body.contains("move the issue to Done"));
    }

    #[test]
    fn repo_signal_prompt_holds_review_before_configured_completion() {
        let body = super::repo_kapellmeister_signal_message_with_lanes(
            &[],
            "Closed",
            Some("Resolved"),
            None,
        )
        .expect("prompt");
        assert!(body.contains("move the issue to Resolved to await operator review"));
        assert!(body.contains("only verified merge completion projects to Closed"));
        assert!(!body.contains("move the issue to Closed"));
        assert!(!body.contains("means accepted development work, not merged PR"));
    }

    #[test]
    fn shared_review_handoff_requests_native_review_without_claiming_approval() {
        let body = super::repo_kapellmeister_signal_message_with_lanes(
            &[],
            "Closed",
            Some("Resolved"),
            Some("Resolved"),
        )
        .unwrap();
        assert!(body.contains("move the issue to Resolved for independent specialist review"));
        assert!(body.contains("first-seen, recovered or changed observations"));
        assert!(body.contains("retain an already completed current review"));
        assert!(body.contains("never proof of approval"));
        assert!(body.contains("never create substitute reviewers"));
        assert!(!body.contains("holding lane, not a review action trigger"));
        assert!(!body.contains("spawn them"));
    }

    #[test]
    fn repo_signal_prompt_does_not_request_disabled_review_action() {
        let body = super::repo_kapellmeister_signal_message_with_lanes(&[], "Done", None, None)
            .expect("prompt");
        assert!(body.contains("Review is a holding lane only"));
        assert!(!body.contains("fugue_start_independent_review"));
    }

    use super::*;
    use crate::final_pr_gate::{FinalPrGateDecision, FinalReviewVerdictStatus};
    use crate::kapellmeister::KapellmeisterShadowSignalKind;
    use crate::linear_adapter::{
        IssueCommentChange, IssueLaneObservation, issue_comment_control_plane_signals,
        issue_lane_control_plane_signals,
    };
    use crate::tools::{
        FugueToolError, GitHubCheckSnapshot, GitHubOwner, GitHubRepoName, IdempotencyKey,
        LinearCommentId, LinearIssueId, LinearIssueSnapshot, LinearStateId, LinearStateSnapshot,
        LinearStateType, LinearTeamId, ToolExecutionMetadata,
    };
    use async_trait::async_trait;
    use chrono::TimeZone;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::config::{
        LinearRequiredState, WorkflowConfigManager, WorkflowDefinition, parse_native_workflow,
    };

    fn issue(id: &str, identifier: &str, priority: Option<i64>, created: i64) -> Issue {
        Issue {
            id: id.to_string(),
            identifier: identifier.to_string(),
            title: "Title".to_string(),
            description: None,
            priority,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: vec![],
            blocked_by: vec![],
            attachments: vec![],
            created_at: Some(Utc.timestamp_opt(created, 0).unwrap()),
            updated_at: None,
        }
    }

    fn issue_with_labels(labels: &[&str]) -> Issue {
        let mut issue = issue("issue-1", "LUC-168", Some(1), 10);
        issue.labels = labels.iter().map(|label| (*label).to_string()).collect();
        issue
    }

    #[test]
    fn orchestrator_uses_issue_adapter_for_linear_transport_boundaries() {
        let source = include_str!("orchestrator.rs");
        let banned = [
            format!("{}::from_config", "LinearClient"),
            format!("{}::new", "LinearToolExecutor"),
            format!("{}::from_config", "FugueImsStore"),
            concat!("use crate::linear_", "lane_state").to_string(),
            concat!("use crate::linear_", "projection").to_string(),
            concat!("LinearLane", "State").to_string(),
            concat!("LinearComment", "State").to_string(),
            concat!("classify_linear", "_lane").to_string(),
            concat!("evaluate_linear", "_projection").to_string(),
            concat!("projection_lane", "_for_node").to_string(),
            concat!("state_ref", "_for_lane").to_string(),
        ];
        for banned_pattern in banned {
            assert!(
                !source.contains(&banned_pattern),
                "orchestrator.rs must use IssueAdapter/facade instead of {banned_pattern}"
            );
        }
        assert!(source.contains("IssueAdapter::from_config"));
    }

    #[test]
    fn kapellmeister_pr_attention_monitor_does_not_project_lanes() {
        let source = include_str!("orchestrator.rs");
        let function_start = source
            .find("async fn reconcile_native_kapellmeister_pr_attention_handoffs")
            .expect("function exists");
        let function_end = source[function_start..]
            .find("async fn ensure_linear_projection_lanes")
            .map(|offset| function_start + offset)
            .expect("next function exists");
        let body = &source[function_start..function_end];

        assert!(
            body.contains("claimed_without_running"),
            "Kapellmeister PR attention monitor may observe already-running issue mobs but must skip launch-only claims"
        );
        assert!(
            !body.contains(
                "state.running.contains_key(&issue.id) || state.claimed.contains(&issue.id)"
            ),
            "running issue mobs must not suppress PR/check readiness observations"
        );
        assert!(
            body.contains("deliver_native_pr_handoff_monitor_signal_to_kapellmeister"),
            "PR monitor must notify Kapellmeister through a host signal"
        );
        assert!(
            !body.contains("project_issue_to_native_lane"),
            "Kapellmeister-mode PR monitor must not project Linear lanes directly"
        );
    }

    fn issue_in_lane(lane: &str) -> Issue {
        let mut issue = issue("issue-1", "LUC-168", Some(1), 10);
        issue.state = lane.to_string();
        issue.updated_at = Some(Utc.timestamp_opt(20, 0).unwrap());
        issue
    }

    fn active_issue_with_labels(labels: &[&str]) -> Issue {
        let mut issue = issue_with_labels(labels);
        issue.state = "In Progress".to_string();
        issue.branch_name = Some("codex/LUC-168".to_string());
        issue
    }

    #[test]
    fn native_mcp_servers_are_scoped_to_current_issue_and_repo() {
        let mut issue = issue("issue-362", "LUC-362", Some(3), 10);
        issue.branch_name = Some("luka/luc-362-docs".to_string());
        let mut servers = BTreeMap::new();
        servers.insert(
            "linear".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "linear_projection".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["/opt/fugue/bin/fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::from([(
                    "FUGUE_LINEAR_MCP_AUTHORITY".to_string(),
                    "projection".to_string(),
                )]),
            },
        );
        servers.insert(
            "github".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-github-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        let repo = GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        };

        let scoped = native_mcp_servers_for_issue(
            &servers,
            Path::new("/tmp/fugue-workflow"),
            &issue,
            Some(&repo),
            &["Done".to_string(), "Canceled".to_string()],
            &["Ready for review".to_string()],
            &["Review".to_string(), "Deploy".to_string()],
            &["Blocked".to_string()],
            Path::new("/tmp/fugue-workflow/workgraph.sqlite3"),
        );

        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_ISSUE_IDENTIFIER"))
                .map(String::as_str),
            Some("LUC-362")
        );
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_ISSUE_ID"))
                .map(String::as_str),
            Some("issue-362")
        );
        assert_eq!(
            scoped
                .get("linear_projection")
                .and_then(|server| server.env.get("FUGUE_LINEAR_ISSUE_IDENTIFIER"))
                .map(String::as_str),
            Some("LUC-362")
        );
        assert_eq!(
            scoped
                .get("linear_projection")
                .and_then(|server| server.env.get("FUGUE_LINEAR_MCP_AUTHORITY"))
                .map(String::as_str),
            Some("projection")
        );
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get(LINEAR_ACTION_STATES_ENV))
                .map(String::as_str),
            Some("Review,Deploy")
        );
        assert_eq!(
            scoped
                .get("github")
                .and_then(|server| server.env.get("FUGUE_GITHUB_OWNER"))
                .map(String::as_str),
            Some("lukacf")
        );
        assert_eq!(
            scoped
                .get("github")
                .and_then(|server| server.env.get("FUGUE_GITHUB_REPO"))
                .map(String::as_str),
            Some("meerkat-fugue")
        );
        assert_eq!(
            scoped
                .get("github")
                .and_then(|server| server.env.get("FUGUE_GITHUB_HEAD_BRANCH"))
                .map(String::as_str),
            Some("luka/luc-362-docs")
        );
    }

    #[test]
    fn repo_kapellmeister_mcp_servers_enable_projection_and_merge_authority() {
        let mut servers = BTreeMap::new();
        servers.insert(
            "linear".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "github".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-github-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "fugue_host".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["custom-fugue-host-mcp".to_string()],
                url: None,
                env: BTreeMap::from([(
                    "FUGUE_HOST_API_URL".to_string(),
                    "http://127.0.0.1:4114".to_string(),
                )]),
            },
        );
        servers.insert(
            "playwright".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec![
                    "npx".to_string(),
                    "-y".to_string(),
                    "@playwright/mcp@latest".to_string(),
                    "--headless".to_string(),
                ],
                url: None,
                env: BTreeMap::new(),
            },
        );
        let repo = GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        };

        let scoped = native_repo_kapellmeister_mcp_servers(
            &servers,
            Path::new("/tmp/fugue-workflow"),
            Some(&repo),
            Some("meerkat-fugue-queue"),
            &["Done".to_string(), "Canceled".to_string()],
            &["Ready for review".to_string()],
            &["Review".to_string(), "Deploy".to_string()],
            &["Blocked".to_string()],
            Path::new("/tmp/fugue-workflow/workgraph.sqlite3"),
            Some("http://127.0.0.1:4007"),
        );

        assert_eq!(scoped.len(), 4);
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_MCP_AUTHORITY"))
                .map(String::as_str),
            Some("projection")
        );
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("LINEAR_PROJECT_SLUG"))
                .map(String::as_str),
            Some("meerkat-fugue-queue")
        );
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get(LINEAR_ACTION_STATES_ENV))
                .map(String::as_str),
            Some("Review,Deploy")
        );
        assert!(
            scoped
                .get("github")
                .is_none_or(|server| !server.env.contains_key("FUGUE_GITHUB_ALLOW_MERGE"))
        );
        assert_eq!(
            scoped
                .get("github")
                .and_then(|server| server.env.get("FUGUE_GITHUB_OWNER"))
                .map(String::as_str),
            Some("lukacf")
        );
        assert_eq!(
            scoped
                .get("fugue_host")
                .and_then(|server| server.command.first())
                .map(String::as_str),
            Some("custom-fugue-host-mcp")
        );
        assert_eq!(
            scoped
                .get("fugue_host")
                .and_then(|server| server.env.get("FUGUE_HOST_API_URL"))
                .map(String::as_str),
            Some("http://127.0.0.1:4114")
        );
        assert_eq!(
            scoped
                .get("playwright")
                .map(|server| server.command.as_slice()),
            Some(
                &[
                    "npx".to_string(),
                    "-y".to_string(),
                    "@playwright/mcp@latest".to_string(),
                    "--headless".to_string(),
                ][..]
            )
        );
    }

    #[test]
    fn repo_kapellmeister_default_host_mcp_uses_configured_server_port() {
        let scoped = native_repo_kapellmeister_mcp_servers(
            &BTreeMap::new(),
            Path::new("/tmp/fugue-workflow"),
            None,
            None,
            &[],
            &[],
            &[],
            &[],
            Path::new("/tmp/fugue-workflow/workgraph.sqlite3"),
            Some("http://127.0.0.1:4017"),
        );

        assert_eq!(
            scoped
                .get("fugue_host")
                .and_then(|server| server.env.get("FUGUE_HOST_API_URL"))
                .map(String::as_str),
            Some("http://127.0.0.1:4017")
        );
    }

    #[test]
    fn final_and_independent_launch_plans_include_configured_mcp_except_host() {
        let mut issue = issue("issue-641", "LUC-641", Some(3), 10);
        issue.branch_name = Some("luka/luc-641-add-playwright-mcp-support".to_string());
        let repo = repo_ref();
        let mut servers = BTreeMap::new();
        servers.insert(
            "linear".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "github".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-github-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "playwright".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec![
                    "npx".to_string(),
                    "-y".to_string(),
                    "@playwright/mcp@latest".to_string(),
                    "--headless".to_string(),
                ],
                url: None,
                env: BTreeMap::new(),
            },
        );
        servers.insert(
            "fugue_host".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-host-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        let mcp_tools = agent_visible_mcp_tool_names(&servers);
        assert_eq!(
            mcp_tools,
            vec![
                "mcp:github".to_string(),
                "mcp:linear".to_string(),
                "mcp:playwright".to_string()
            ]
        );

        let pr = pr_snapshot(222, "luka/luc-641-add-playwright-mcp-support", "sha-641");
        let final_request = FinalPrGateStartRequest {
            issue: issue_ref_for_identifier("LUC-641"),
            pr: pr.clone(),
            trigger_mode: crate::config::FinalReviewMode::Manual,
            reviewers: vec![crate::final_pr_gate::FinalReviewerMemberSpec {
                id: "architecture".to_string(),
                profile: "final_architecture_reviewer".to_string(),
                required: true,
                merge_tool_allowed: false,
            }],
            spawn_input: SpawnOrResumeExecutionInput {
                issue: issue_ref_for_identifier("LUC-641"),
                workspace_key: workspace_key_for_identifier("LUC-641"),
                template_id: crate::tools::MobTemplateId("final-pr-gate-222".to_string()),
                worktree_path: Some("/tmp/LUC-641".to_string()),
                instructions: Some("final review".to_string()),
                idempotency_key: IdempotencyKey("final-pr-gate-test".to_string()),
            },
            mob_definition: json!({}),
        };
        let final_template = final_pr_gate_native_launch_template(&final_request, &mcp_tools);
        assert_eq!(final_template.members.len(), 1);
        assert_eq!(
            final_template.members[0].name, "review_orchestrator",
            "final review must select existing specialists through one coordinator"
        );
        let final_tools = &final_template.members[0].agent.tools;
        assert!(final_tools.contains(&"mcp:github".to_string()));
        assert!(final_tools.contains(&"mcp:linear".to_string()));
        assert!(final_tools.contains(&"mcp:playwright".to_string()));
        assert!(!final_tools.contains(&"mcp:fugue_host".to_string()));

        let final_plan = NativeMobLaunchPlan::new(
            final_request.spawn_input.clone(),
            final_template.clone(),
            native_member_mcp_servers_for_issue(
                &servers,
                Path::new("/tmp/fugue-workflow"),
                &final_template,
                &issue,
                Some(&repo),
                &["Done".to_string(), "Canceled".to_string()],
                &["Ready for review".to_string()],
                &["Final Review & Merge".to_string()],
            ),
            native_member_direct_tool_servers(
                &TrackerConfig {
                    jira: None,
                    kind: "linear".to_string(),
                    endpoint: String::new(),
                    api_key: None,
                    project_slug: None,
                    intake_states: Vec::new(),
                    in_flight_states: Vec::new(),
                    active_states: Vec::new(),
                    attachment_root: None,
                    max_attachment_files: 0,
                    max_attachment_bytes: 0,
                    max_attachment_total_bytes: 0,
                    terminal_states: Vec::new(),
                },
                &final_template,
            ),
            None,
            Some(repo.clone()),
        )
        .unwrap_or_else(|error| panic!("final plan should validate configured MCPs: {error:?}"));
        assert!(final_plan.mcp_servers.contains_key("playwright"));
        assert!(!final_plan.mcp_servers.contains_key("fugue_host"));

        let independent_request = IndependentReviewStartRequest {
            issue: issue_ref_for_identifier("LUC-641"),
            spawn_input: SpawnOrResumeExecutionInput {
                issue: issue_ref_for_identifier("LUC-641"),
                workspace_key: workspace_key_for_identifier("LUC-641"),
                template_id: crate::tools::MobTemplateId("independent-review-222".to_string()),
                worktree_path: Some("/tmp/LUC-641".to_string()),
                instructions: Some("independent review".to_string()),
                idempotency_key: IdempotencyKey("independent-review-test".to_string()),
            },
            pull_request: pr,
            orchestrator_profile: "review_orchestrator".to_string(),
        };
        let independent_template =
            independent_review_native_launch_template(&independent_request, &mcp_tools);
        let independent_tools = &independent_template.members[0].agent.tools;
        assert!(independent_tools.contains(&"mcp:github".to_string()));
        assert!(independent_tools.contains(&"mcp:linear".to_string()));
        assert!(independent_tools.contains(&"mcp:playwright".to_string()));
        assert!(!independent_tools.contains(&"mcp:fugue_host".to_string()));
        let independent_plan = NativeMobLaunchPlan::new(
            independent_request.spawn_input.clone(),
            independent_template.clone(),
            native_member_mcp_servers_for_issue(
                &servers,
                Path::new("/tmp/fugue-workflow"),
                &independent_template,
                &issue,
                Some(&repo),
                &["Done".to_string(), "Canceled".to_string()],
                &["Ready for review".to_string()],
                &["Review".to_string()],
            ),
            BTreeSet::new(),
            None,
            Some(repo),
        )
        .unwrap_or_else(|error| {
            panic!("independent plan should validate configured MCPs: {error:?}")
        });
        assert!(independent_plan.mcp_servers.contains_key("playwright"));
        assert!(!independent_plan.mcp_servers.contains_key("fugue_host"));
    }

    #[test]
    fn native_member_mcp_servers_include_issue_lead_projection_authority() {
        let mut issue = issue("issue-362", "LUC-362", Some(3), 10);
        issue.branch_name = Some("luka/luc-362-docs".to_string());
        let mut servers = BTreeMap::new();
        servers.insert(
            "linear".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::from([(
                    "FUGUE_LINEAR_MCP_AUTHORITY".to_string(),
                    "workpad".to_string(),
                )]),
            },
        );
        servers.insert(
            "linear_projection".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-linear-mcp".to_string()],
                url: None,
                env: BTreeMap::from([(
                    "FUGUE_LINEAR_MCP_AUTHORITY".to_string(),
                    "projection".to_string(),
                )]),
            },
        );
        servers.insert(
            "github".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-github-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );
        let repo = GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        };

        let scoped = native_member_mcp_servers_for_issue(
            &servers,
            Path::new("/tmp/fugue-workflow"),
            &test_planner_coder_reviewer_template(),
            &issue,
            Some(&repo),
            &["Done".to_string(), "Canceled".to_string()],
            &["Ready for review".to_string()],
            &["Review".to_string(), "Deploy".to_string()],
        );

        assert!(scoped.contains_key("linear"));
        assert!(scoped.contains_key("github"));
        assert!(scoped.contains_key("linear_projection"));
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_MCP_AUTHORITY"))
                .map(String::as_str),
            Some("workpad")
        );
        assert_eq!(
            scoped
                .get("linear_projection")
                .and_then(|server| server.env.get("FUGUE_LINEAR_MCP_AUTHORITY"))
                .map(String::as_str),
            Some("projection")
        );
        assert_eq!(
            scoped
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_ISSUE_IDENTIFIER"))
                .map(String::as_str),
            Some("LUC-362")
        );
        assert_eq!(
            scoped
                .get("github")
                .and_then(|server| server.env.get("FUGUE_GITHUB_HEAD_BRANCH"))
                .map(String::as_str),
            Some("luka/luc-362-docs")
        );
    }

    fn linear_issue_snapshot_in_state(state_name: &str) -> LinearIssueSnapshot {
        LinearIssueSnapshot {
            id: LinearIssueId("issue-1".to_string()),
            identifier: LinearIssueIdentifier("LUC-277".to_string()),
            title: "Title".to_string(),
            description: None,
            url: None,
            state: LinearStateSnapshot {
                id: LinearStateId(format!("state-{state_name}")),
                name: state_name.to_string(),
                state_type: LinearStateType::Started,
            },
            assignee: None,
            branch_name: None,
            labels: vec![],
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            team_id: LinearTeamId("team-1".to_string()),
        }
    }

    fn repo_ref() -> GitHubRepoRef {
        GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }
    }

    fn test_planner_coder_reviewer_template() -> MaterializedMobTemplate {
        let member = |name: &str, tools: Vec<&str>| crate::config::MaterializedMobMember {
            name: name.to_string(),
            profile: name.to_string(),
            agent: crate::config::NativeAgentProfile {
                model: Some("gpt-5.5".to_string()),
                provider: None,
                auth_binding: None,
                max_tokens: None,
                system_prompt: None,
                output_schema: None,
                structured_output_retries: None,
                provider_params: None,
                tools: tools.into_iter().map(str::to_string).collect(),
                skills: Vec::new(),
                additional_instructions: None,
            },
        };
        MaterializedMobTemplate {
            backend: MobExecutionBackend::NativeExecution,
            members: vec![
                member("planner", vec!["mob", "mcp:linear", "mcp:github"]),
                member("coder", vec!["mob", "shell", "mcp:linear", "mcp:github"]),
                member("reviewer", vec!["mob", "shell", "mcp:linear", "mcp:github"]),
            ],
            flow: Some(crate::config::NativeMobFlow::PlannerCoderReviewer),
            linear_projection: None,
            max_review_cycles: Some(3),
        }
    }

    fn pr_snapshot(number: u64, head_branch: &str, head_sha: &str) -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: repo_ref(),
            number: GitHubPullRequestNumber(number),
            title: format!("PR #{number}"),
            url: format!("https://github.com/lukacf/meerkat-fugue/pull/{number}"),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName(head_branch.to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha(head_sha.to_string()),
        }
    }

    fn run_git(workspace: &std::path::Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(workspace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_head_sha(workspace: &std::path::Path) -> String {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(workspace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git rev-parse HEAD failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn workflow_definition(kapellmeister: serde_json::Value) -> WorkflowDefinition {
        let mut root = json!({
            "tracker": {
                "kind": "linear",
                "api_key": "token",
                "project_slug": "project",
                "active_states": ["Todo", "In Progress"],
                "terminal_states": ["Done", "Canceled"]
            },
            "workspace": { "root": "/tmp/fugue-test-workspaces" },
            "agent": {
                "max_concurrent_agents": 2,
                "max_turns": 1,
                "max_retry_backoff_ms": 10
            },
            "meerkat": { "command": "true" }
        });
        if let Some(object) = root.as_object_mut() {
            object.insert("kapellmeister".to_string(), kapellmeister);
        }
        WorkflowDefinition {
            config: root.as_object().unwrap().clone(),
            prompt_template: "hi".to_string(),
        }
    }

    fn native_workflow_definition() -> WorkflowDefinition {
        let root = json!({
            "kapellmeister": {
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            },
            "tracker": {
                "kind": "linear",
                "api_key": "token",
                "project_slug": "project",
                "active_states": ["Todo", "In Progress"],
                "terminal_states": ["Done", "Canceled"]
            },
            "workspace": { "root": "/tmp/fugue-test-workspaces" },
            "agent": {
                "max_concurrent_agents": 2,
                "max_turns": 1,
                "max_retry_backoff_ms": 10
            },
            "meerkat": { "command": "true" },
            "worker_policy": {
                "native_launch_context_template": "Native launch context from config for {{ issue.identifier }} in {{ worktree }} using {{ template_id }} with {{ members }}. Preserve the configured handoff."
            },
            "mob": {
                "backend": "native-execution",
                "native": {"enabled": true, "shadow": false},
                "templates": {
                    "implementation_mob": {
                        "backend": "native-execution",
                        "flow": "planner-coder-reviewer",
                        "max_review_cycles": 2,
                        "linear_projection": {
                            "id": "planner_coder_reviewer",
                            "node_lanes": {
                                "planner": "Planning",
                                "coder": "Implementing",
                                "reviewer": "Reviewing"
                            },
                            "aggregate": {
                                "precedence": ["Reviewing", "Implementing", "Planning"],
                                "fallback": "Reviewing"
                            },
                            "terminal_lanes": {
                                "succeeded": "Done",
                                "failed": "Reviewing",
                                "cancelled": "Canceled"
                            }
                        },
                        "members": [
                            {"name": "planner", "profile": "planner", "agent": {"system_prompt": "Plan", "tools": ["mob", "mcp:linear", "mcp:github"]}},
                            {"name": "coder", "profile": "coder", "agent": {"tools": ["mob", "shell", "git", "mcp:linear", "mcp:github"]}},
                            {"name": "reviewer", "profile": "reviewer", "agent": {"system_prompt": "Review", "tools": ["mob", "shell", "git", "mcp:linear", "mcp:github"]}}
                        ]
                    }
                },
                "mcp_servers": {
                    "linear": {"command": ["fugue-linear-mcp"]},
                    "github": {"command": ["fugue-github-mcp"]}
                },
                "selection": {
                    "default_template": "implementation_mob",
                    "by_label": {"native": "implementation_mob"}
                }
            }
        });
        WorkflowDefinition {
            config: root.as_object().unwrap().clone(),
            prompt_template: "Work on {{ issue.identifier }}".to_string(),
        }
    }

    fn service_config(kapellmeister: serde_json::Value) -> ServiceConfig {
        ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &workflow_definition(kapellmeister),
        )
        .unwrap()
    }

    fn guarded_config() -> ServiceConfig {
        service_config(json!({
            "authority_mode": "guarded_labeled",
            "safe_selectors": {
                "linear_labels": ["kapellmeister-safe"]
            }
        }))
    }

    #[derive(Default)]
    struct RecordingWorkpadExecutor {
        inputs: Vec<crate::tools::UpsertWorkpadInput>,
    }

    impl RecordingWorkpadExecutor {
        fn last_body(&self) -> Option<&str> {
            self.inputs.last().map(|input| input.body_markdown.as_str())
        }
    }

    #[async_trait]
    impl KapellmeisterHostToolExecutor for RecordingWorkpadExecutor {
        async fn claim_issue(&mut self, input: crate::tools::ClaimIssueInput) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::LinearClaimIssue,
                Some(input.idempotency_key),
            )
        }

        async fn move_issue(&mut self, input: crate::tools::MoveIssueInput) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::LinearMoveIssue,
                Some(input.idempotency_key),
            )
        }

        async fn upsert_workpad(
            &mut self,
            input: crate::tools::UpsertWorkpadInput,
        ) -> FugueToolResult {
            self.inputs.push(input);
            FugueToolResult::Ok {
                output: FugueToolOutput::LinearUpsertWorkpad(crate::tools::UpsertWorkpadOutput {
                    comment_id: LinearCommentId("comment-1".to_string()),
                    url: Some("https://linear.example/LUC-168#comment-1".to_string()),
                    created: false,
                    updated: true,
                }),
                metadata: ToolExecutionMetadata::default(),
            }
        }

        async fn spawn_or_resume_execution(
            &mut self,
            input: SpawnOrResumeExecutionInput,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(input.idempotency_key),
            )
        }

        async fn spawn_or_resume_final_pr_gate(
            &mut self,
            request: FinalPrGateStartRequest,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(request.spawn_input.idempotency_key),
            )
        }

        async fn spawn_or_resume_independent_review(
            &mut self,
            request: IndependentReviewStartRequest,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(request.spawn_input.idempotency_key),
            )
        }

        async fn merge_pr(&mut self, input: crate::tools::MergePrInput) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::GitHubMergePr,
                Some(input.idempotency_key),
            )
        }
    }

    #[derive(Default)]
    struct RecordingFinalGateProjectionExecutor {
        workpads: Vec<UpsertWorkpadInput>,
        moves: Vec<MoveIssueInput>,
        merges: Vec<MergePrInput>,
        operations: Vec<&'static str>,
        fail_merge: bool,
    }

    #[async_trait]
    impl KapellmeisterHostToolExecutor for RecordingFinalGateProjectionExecutor {
        async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::LinearClaimIssue,
                Some(input.idempotency_key),
            )
        }

        async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult {
            self.operations.push("move");
            self.moves.push(input.clone());
            FugueToolResult::Ok {
                output: FugueToolOutput::LinearMoveIssue(crate::tools::MoveIssueOutput {
                    issue: linear_issue_snapshot_in_state(match &input.state {
                        LinearStateRef::Name { name } => name.as_str(),
                        LinearStateRef::Id { .. } => "Projected",
                    }),
                    previous_state: None,
                    changed: true,
                }),
                metadata: ToolExecutionMetadata::default(),
            }
        }

        async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult {
            self.operations.push("workpad");
            self.workpads.push(input);
            FugueToolResult::Ok {
                output: FugueToolOutput::LinearUpsertWorkpad(crate::tools::UpsertWorkpadOutput {
                    comment_id: LinearCommentId("comment-1".to_string()),
                    url: Some("https://linear.example/LUC-623#comment-1".to_string()),
                    created: false,
                    updated: true,
                }),
                metadata: ToolExecutionMetadata::default(),
            }
        }

        async fn spawn_or_resume_execution(
            &mut self,
            input: SpawnOrResumeExecutionInput,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(input.idempotency_key),
            )
        }

        async fn spawn_or_resume_final_pr_gate(
            &mut self,
            request: FinalPrGateStartRequest,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(request.spawn_input.idempotency_key),
            )
        }

        async fn spawn_or_resume_independent_review(
            &mut self,
            request: IndependentReviewStartRequest,
        ) -> FugueToolResult {
            recorded_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                Some(request.spawn_input.idempotency_key),
            )
        }

        async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult {
            self.operations.push("merge");
            if self.fail_merge {
                return recorded_error(
                    FugueToolOperation::GitHubMergePr,
                    Some(input.idempotency_key),
                );
            }
            self.merges.push(input.clone());
            FugueToolResult::Ok {
                output: FugueToolOutput::GitHubMergePr(crate::tools::MergePrOutput {
                    pull_request: pr_snapshot(
                        187,
                        "luka/luc-623",
                        input.expected_head_sha.as_ref().unwrap().0.as_str(),
                    ),
                    merged: true,
                    merge_commit_sha: input.expected_head_sha.clone(),
                }),
                metadata: ToolExecutionMetadata::default(),
            }
        }
    }

    #[tokio::test]
    async fn recorded_pr_handoff_survives_workpad_rewrite_and_workspace_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let (orchestrator, _) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let mut config = orchestrator.config.effective().clone();
        config.meerkat.state_root = Some(directory.path().join("runtime"));
        config.workspace.root = directory.path().join("removed-workspaces");
        config
            .kapellmeister
            .shadow_mode
            .authority
            .safe_selectors
            .repositories = vec![repo_ref()];
        let issue = issue("immutable-issue-1", "TEST-1", None, 10);
        let pr = pr_snapshot(42, "feature/test-1", "reviewed-head");
        crate::pr_handoff::record(&config, &issue, &pr).unwrap();
        let notes = recover_native_linear_evidence(
            &issue,
            Some("## Fugue Workpad\n\nRewritten notes without a PR reference."),
        );
        assert_eq!(
            parse_first_pr_number(
                "## Fugue Workpad\n\n### Review / PR Handoff Evidence\nPR: https://github.com/example/application/pull/42"
            ),
            Some(GitHubPullRequestNumber(42))
        );
        assert!(
            recover_existing_native_pr_monitor_handoff_for_issue(
                &config,
                &issue,
                Some(&notes),
                &StaticGitHubInspector::ok(pr.clone())
            )
            .is_none()
        );

        // A fresh process reads the durable association, then obtains current
        // state and head from GitHub rather than replaying a stored snapshot.
        let mut current = pr.clone();
        current.state = PullRequestState::Merged;
        current.head_sha = GitSha("current-head".into());
        let recovered = recover_recorded_native_pr_handoff(
            &config,
            &issue,
            None,
            &StaticGitHubInspector::ok(current.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(recovered.pull_request, current);
        assert!(
            recover_recorded_native_pr_handoff(
                &config,
                &issue,
                None,
                &StaticGitHubInspector::err("GitHub unavailable")
            )
            .is_err()
        );
        let mut wrong = current;
        wrong.head_branch = BranchName("unrelated-branch".into());
        assert!(
            recover_recorded_native_pr_handoff(
                &config,
                &issue,
                None,
                &StaticGitHubInspector::ok(wrong)
            )
            .is_err()
        );
        let mut reworked = issue.clone();
        reworked.branch_name = Some("feature/replacement".into());
        assert!(
            recover_recorded_native_pr_handoff(
                &config,
                &reworked,
                None,
                &StaticGitHubInspector::ok(pr.clone())
            )
            .is_err(),
            "a durable association must not override the current issue branch"
        );
        config.tracker.project_slug = Some("another-project".into());
        assert!(
            recover_recorded_native_pr_handoff(
                &config,
                &issue,
                None,
                &StaticGitHubInspector::ok(pr)
            )
            .unwrap()
            .is_none()
        );
    }

    #[derive(Clone)]
    struct StaticGitHubInspector {
        result: FugueToolResult,
        ci: Option<CheckCiOutput>,
    }

    impl StaticGitHubInspector {
        fn ok(pull_request: GitHubPullRequestSnapshot) -> Self {
            Self {
                ci: Some(ci_output(CiStatus::Success)),
                result: FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubInspectPr(crate::tools::InspectPrOutput {
                        pull_request,
                    }),
                    metadata: ToolExecutionMetadata::default(),
                },
            }
        }

        fn err(message: &str) -> Self {
            Self {
                ci: Some(ci_output(CiStatus::Success)),
                result: FugueToolResult::Err {
                    error: FugueToolError {
                        operation: FugueToolOperation::GitHubInspectPr,
                        code: crate::tools::FugueToolErrorCode::ExecutionFailed,
                        message: message.to_string(),
                        retry: crate::tools::RetryAdvice::DoNotRetry,
                        idempotency_key: None,
                    },
                    metadata: ToolExecutionMetadata::default(),
                },
            }
        }
    }

    impl NativeStatusGitHubInspector for StaticGitHubInspector {
        fn inspect_pr(&self, _input: crate::tools::InspectPrInput) -> FugueToolResult {
            self.result.clone()
        }

        fn check_pr_ci(&self, _pull_request: &GitHubPullRequestSnapshot) -> FugueToolResult {
            match self.ci.clone() {
                Some(output) => FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubCheckCi(output),
                    metadata: ToolExecutionMetadata::default(),
                },
                None => recorded_error(FugueToolOperation::GitHubCheckCi, None),
            }
        }
    }

    fn unavailable_github_inspector() -> StaticGitHubInspector {
        StaticGitHubInspector {
            ci: None,
            result: FugueToolResult::Err {
                error: FugueToolError {
                    operation: FugueToolOperation::GitHubInspectPr,
                    code: crate::tools::FugueToolErrorCode::BackendUnavailable,
                    message: "gh unavailable".to_string(),
                    retry: crate::tools::RetryAdvice::SafeToRetry,
                    idempotency_key: None,
                },
                metadata: ToolExecutionMetadata::default(),
            },
        }
    }

    #[derive(Default)]
    struct RecordingNativeMobExecutionHost {
        inner: InMemoryNativeMobExecutionHost,
        spawned_inputs: std::sync::Mutex<Vec<SpawnOrResumeExecutionInput>>,
        plan_inputs: std::sync::Mutex<Vec<SpawnOrResumeExecutionInput>>,
        status_override: std::sync::Mutex<Option<MobRunSnapshot>>,
        cleanup_calls: std::sync::Mutex<Vec<(MobRunId, String)>>,
    }

    impl RecordingNativeMobExecutionHost {
        fn append_structural_event(
            &self,
            run_id: &MobRunId,
            kind: crate::tools::MobEventKind,
            message: Option<String>,
        ) -> std::result::Result<MobEventSnapshot, FugueToolError> {
            self.inner.append_structural_event(run_id, kind, message)
        }

        fn raw_spawn_call_count(&self) -> usize {
            self.spawned_inputs
                .lock()
                .map(|inputs| inputs.len())
                .unwrap_or_default()
        }

        fn plan_call_count(&self) -> usize {
            self.plan_inputs
                .lock()
                .map(|inputs| inputs.len())
                .unwrap_or_default()
        }

        fn spawn_call_count(&self) -> usize {
            self.raw_spawn_call_count() + self.plan_call_count()
        }

        fn spawned_inputs(&self) -> Vec<SpawnOrResumeExecutionInput> {
            let mut inputs = self
                .spawned_inputs
                .lock()
                .map(|inputs| inputs.clone())
                .unwrap_or_default();
            inputs.extend(
                self.plan_inputs
                    .lock()
                    .map(|inputs| inputs.clone())
                    .unwrap_or_default(),
            );
            inputs
        }

        fn set_status_override(&self, run: MobRunSnapshot) {
            if let Ok(mut override_run) = self.status_override.lock() {
                *override_run = Some(run);
            }
        }

        fn running_count_for_issue(&self, issue: LinearIssueRef) -> usize {
            match self.list_runs(crate::tools::ListRunsInput {
                issue: Some(issue),
                status: Some(MobRunStatus::Running),
                cursor: None,
                limit: 100,
            }) {
                FugueToolResult::Ok {
                    output: FugueToolOutput::MobListRuns(output),
                    ..
                } => output.runs.len(),
                _ => 0,
            }
        }

        fn cleanup_calls(&self) -> Vec<(MobRunId, String)> {
            self.cleanup_calls
                .lock()
                .map(|calls| calls.clone())
                .unwrap_or_default()
        }
    }

    impl NativeMobExecutionHost for RecordingNativeMobExecutionHost {
        fn list_runs(&self, input: crate::tools::ListRunsInput) -> FugueToolResult {
            self.inner.list_runs(input)
        }

        fn status(&self, input: crate::tools::InspectRunInput) -> FugueToolResult {
            if let Ok(override_run) = self.status_override.lock()
                && let Some(run) = override_run.as_ref()
                && run.run_id == input.run_id
            {
                return FugueToolResult::Ok {
                    output: FugueToolOutput::MobInspectRun(crate::tools::InspectRunOutput {
                        run: run.clone(),
                    }),
                    metadata: ToolExecutionMetadata::default(),
                };
            }
            self.inner.status(input)
        }

        fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult {
            if let Ok(mut inputs) = self.spawned_inputs.lock() {
                inputs.push(input.clone());
            }
            self.inner.spawn_or_resume_execution(input)
        }

        fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
            let input = plan.input.clone();
            if let Ok(mut inputs) = self.plan_inputs.lock() {
                inputs.push(input.clone());
            }
            self.inner.spawn_or_resume_execution(input)
        }
        fn cancel_execution(&self, input: crate::tools::CancelExecutionInput) -> FugueToolResult {
            self.inner.cancel_execution(input)
        }

        fn cleanup_execution(
            &self,
            run_id: &MobRunId,
            reason: &str,
        ) -> std::result::Result<usize, String> {
            if let Ok(mut calls) = self.cleanup_calls.lock() {
                calls.push((run_id.clone(), reason.to_string()));
            }
            Ok(1)
        }

        fn observe_structural_events(
            &self,
            input: crate::mob_tools::ObserveStructuralEventsInput,
        ) -> FugueToolResult {
            self.inner.observe_structural_events(input)
        }
    }

    fn native_event(
        sequence: u64,
        kind: crate::tools::MobEventKind,
        message: Option<&str>,
    ) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence,
            occurred_at: Utc::now(),
            kind,
            message: message.map(str::to_string),
        }
    }

    #[test]
    fn native_retry_reason_includes_terminal_event_detail() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            Some("Meta flow `planner-coder-reviewer` failed: planner member exited"),
        )];

        let reason = native_retry_reason(MobRunStatus::Failed, None, &events);

        assert!(reason.contains("native mob completed with status Failed"));
        assert!(
            reason.contains(
                "detail=Meta flow `planner-coder-reviewer` failed: planner member exited"
            )
        );
    }

    #[test]
    fn native_terminal_status_prefers_structural_event_over_stale_snapshot() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            Some("Meta RPC mob/create failed: cannot resume flow run 'flow-1'"),
        )];

        assert_eq!(
            native_terminal_status_from_events(&events),
            Some(MobRunStatus::Failed)
        );
    }

    #[test]
    fn turn_limit_events_select_issue_lead_recovery_observations() {
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::MemberKickoffUpdated {
                    member: "LUC-642/coder".to_string(),
                    phase: "turn_limit_reached".to_string(),
                },
                Some("Terminal failure: Failed (TurnLimitReached): turn limit reached"),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::MemberKickoffUpdated {
                    member: "LUC-642/reviewer".to_string(),
                    phase: "started".to_string(),
                },
                Some("started"),
            ),
        ];

        let selected = turn_limit_events(&events).collect::<Vec<_>>();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].member, "LUC-642/coder");
        assert!(selected[0].detail.contains("TurnLimitReached"));
    }

    #[test]
    fn member_terminal_failure_events_select_issue_lead_recovery_observations() {
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::MemberKickoffUpdated {
                    member: "LUC-654/coder".to_string(),
                    phase: "member_terminal_failure".to_string(),
                },
                Some(
                    "LUC-654/coder had a terminal runtime turn failure: Terminal failure: Failed (LlmFailure): LLM failure terminal turn",
                ),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::MemberKickoffUpdated {
                    member: "LUC-654/planner".to_string(),
                    phase: "started".to_string(),
                },
                Some("started"),
            ),
        ];

        let selected = member_terminal_failure_events(&events).collect::<Vec<_>>();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].member, "LUC-654/coder");
        assert!(selected[0].detail.contains("LlmFailure"));
    }

    #[test]
    fn native_resource_exhaustion_terminal_failure_parks_instead_of_retrying() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            Some(
                "Meta RPC mob/spawn_many did not start all required members: Internal error: Max sessions reached (64/64)",
            ),
        )];

        assert_eq!(
            native_terminal_park_reason(&events),
            Some(NativeRunParkReason::ResourceExhausted)
        );
        assert_eq!(
            native_run_terminal_action(MobRunStatus::Failed, None),
            NativeRunTerminalAction::Retry
        );
    }

    #[test]
    fn native_meta_restore_corruption_parks_instead_of_retrying() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            Some(
                "Meta RPC method mob/create failed: internal error: cannot resume flow run \
                 'flow-1': flow authority projection mismatch",
            ),
        )];

        assert_eq!(
            native_terminal_park_reason(&events),
            Some(NativeRunParkReason::HostMechanics)
        );
        assert!(native_mechanism_failure_is_nonretryable(
            "internal error: duplicate persisted mob id during restore: mob-1"
        ));
    }

    #[test]
    fn native_reviewer_llm_terminal_failure_parks_instead_of_blind_retrying() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            Some(
                "Meta flow `planner-coder-reviewer` failed: reviewer failed: internal error: \
                 turn abandoned: apply failed: Terminal failure: Failed (LlmFailure): \
                 LLM failure terminal turn",
            ),
        )];

        assert_eq!(
            native_terminal_park_reason(&events),
            Some(NativeRunParkReason::HostMechanics)
        );
    }

    #[test]
    fn member_level_terminal_failure_masks_sealed_flow_failure_for_parking() {
        let member_failure = "Meta flow step `reviewer` failed: internal error: \
            turn abandoned: apply failed: Terminal failure: Failed (LlmFailure): \
            LLM failure terminal turn";
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::MemberCompleted {
                    member: "reviewer".to_string(),
                },
                Some(member_failure),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Failed,
                    structured_output: None,
                },
                Some("Meta flow `planner-coder-reviewer` failed: root frame sealed failed"),
            ),
        ];

        assert_eq!(
            native_terminal_park_reason(&events),
            Some(NativeRunParkReason::HostMechanics)
        );
        assert_eq!(
            native_terminal_park_detail(&events).as_deref(),
            Some(member_failure)
        );
    }

    #[test]
    fn structured_green_gate_can_handoff_before_later_terminal_llm_failure() {
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: Some(json!({
                        "steps": {
                            "reviewer": {
                                "schema_version": crate::native_reviewer_gate::REVIEWER_GATE_SCHEMA_VERSION,
                                "status": "green",
                                "next_action": "pr_handoff",
                                "summary": "Reviewer gate is green.",
                                "findings": []
                            }
                        }
                    })),
                },
                Some("reviewer produced a durable green gate"),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::MemberCompleted {
                    member: "reviewer".to_string(),
                },
                Some(
                    "Meta flow step `reviewer` failed: internal error: \
                     turn abandoned: apply failed: Terminal failure: Failed (LlmFailure): \
                     LLM failure terminal turn",
                ),
            ),
            native_event(
                3,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Failed,
                    structured_output: None,
                },
                Some("Meta flow `planner-coder-reviewer` failed: root frame sealed failed"),
            ),
        ];
        let reviewer_gate = latest_reviewer_gate_observation(&events);

        assert_eq!(
            select_native_run_terminal_action(
                MobRunStatus::Failed,
                None,
                &events,
                &reviewer_gate,
                false,
                false,
                true,
            ),
            NativeRunTerminalAction::CreatePrHandoff
        );
    }

    #[test]
    fn role_wrapped_structured_green_gate_can_handoff() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Succeeded,
                structured_output: Some(json!({
                    "steps": {
                        "reviewer": {
                            "reviewer": {
                                "schema_version": crate::native_reviewer_gate::REVIEWER_GATE_SCHEMA_VERSION,
                                "status": "green",
                                "next_action": "pr_handoff",
                                "summary": "Meta role-wrapped reviewer gate is green.",
                                "findings": []
                            }
                        }
                    }
                })),
            },
            Some("flow completed with role-wrapped reviewer gate"),
        )];
        let reviewer_gate = latest_reviewer_gate_observation(&events);

        assert_eq!(
            select_native_run_terminal_action(
                MobRunStatus::Succeeded,
                None,
                &events,
                &reviewer_gate,
                false,
                false,
                true,
            ),
            NativeRunTerminalAction::CreatePrHandoff
        );
    }

    #[test]
    fn terminal_llm_failure_without_structured_gate_still_parks() {
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::MemberCompleted {
                    member: "reviewer".to_string(),
                },
                Some(
                    "Meta flow step `reviewer` failed: internal error: \
                     turn abandoned: apply failed: Terminal failure: Failed (LlmFailure): \
                     LLM failure terminal turn",
                ),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Failed,
                    structured_output: None,
                },
                Some("Meta flow `planner-coder-reviewer` failed: root frame sealed failed"),
            ),
        ];
        let reviewer_gate = latest_reviewer_gate_observation(&events);

        assert_eq!(
            select_native_run_terminal_action(
                MobRunStatus::Failed,
                None,
                &events,
                &reviewer_gate,
                false,
                false,
                true,
            ),
            NativeRunTerminalAction::Park(NativeRunParkReason::HostMechanics)
        );
    }

    #[test]
    fn native_liveness_monitor_workpad_section_is_idempotent() {
        let body = "## Fugue Workpad\n\n### Progress\n- [x] Meta run started.";
        let detail = "Internal error: Max sessions reached (64/64)";

        let once = append_native_liveness_monitor_evidence(
            body,
            NativeRunParkReason::ResourceExhausted,
            Some(detail),
        );
        let twice = append_native_liveness_monitor_evidence(
            &once,
            NativeRunParkReason::ResourceExhausted,
            Some(detail),
        );

        assert_eq!(once, twice);
        assert!(once.contains("### Fugue Liveness Monitor"));
        assert!(once.contains("parked issue"));
        assert!(once.contains("Max sessions reached"));
        assert!(once.contains("will not start another mob attempt"));
    }

    #[test]
    fn native_liveness_blocked_signal_notifies_repo_mob_without_retry() {
        let observed_at = Utc.timestamp_opt(42, 0).unwrap();
        let signal = native_liveness_blocked_signal(
            "LUC-401",
            NativeRunParkReason::ResourceExhausted,
            Some("Internal error: Max sessions reached (64/64)"),
            23,
            observed_at,
        );

        assert!(matches!(
            signal.kind,
            KapellmeisterShadowSignalKind::WorkerFailed { .. }
        ));
        let KapellmeisterShadowSignalKind::WorkerFailed {
            issue,
            workspace_key,
            failure,
        } = signal.kind
        else {
            unreachable!("signal kind checked above");
        };
        assert_eq!(issue, issue_ref_for_identifier("LUC-401"));
        assert_eq!(workspace_key.0, "LUC-401");
        assert_eq!(
            failure.reason,
            ShadowWorkerFailureReason::InfrastructureUnavailable
        );
        assert!(!failure.retry_scheduled);
        assert!(failure.message.unwrap().contains("Max sessions reached"));
    }

    #[test]
    fn native_missing_pr_handoff_evidence_signal_does_not_encode_lane_move() {
        let observed_at = Utc.timestamp_opt(42, 0).unwrap();
        let signal = native_pr_handoff_missing_pr_signal("LUC-519", observed_at);

        let KapellmeisterShadowSignalKind::WorkerFailed {
            issue,
            workspace_key,
            failure,
        } = signal.kind
        else {
            panic!("missing PR evidence should notify Kapellmeister as worker failure evidence");
        };
        assert_eq!(issue, issue_ref_for_identifier("LUC-519"));
        assert_eq!(workspace_key.0, "LUC-519");
        assert_eq!(
            failure.reason,
            ShadowWorkerFailureReason::InfrastructureUnavailable
        );
        assert!(!failure.retry_scheduled);
        assert!(
            failure
                .message
                .as_deref()
                .unwrap()
                .contains("host did not move the Linear lane")
        );
    }

    #[test]
    fn repo_kapellmeister_signal_message_carries_typed_signal_payload() {
        let signal = KapellmeisterShadowSignal::linear_issue_moved(
            Utc.timestamp_opt(42, 0).unwrap(),
            None,
            issue_ref_for_identifier("LUC-472"),
            None,
            LinearStateRef::Name {
                name: "Planning".to_string(),
            },
            None,
        );

        let message = repo_kapellmeister_signal_message(&[signal])
            .expect("signal batch should encode as JSON");

        assert!(message.contains("Fugue host signal batch"));
        assert!(message.contains("\"kind\": \"linear_issue_moved\""));
        assert!(message.contains("\"identifier\": \"LUC-472\""));
        assert!(message.contains("list_members returns access_denied"));
        assert!(message.contains("Linear remains a projection"));
        assert!(message.contains("Use one tool call at a time"));
        assert!(message.contains("do not issue parallel or batched tool calls"));
        assert!(message.contains("external projection drift"));
        assert!(message.contains("operator_review_finding"));
        assert!(message.contains("concrete issue rework input"));
        assert!(message.contains("do not classify it as a passive final-gate wait"));
        assert!(message.contains("pass the explicit issue"));
        assert!(message.contains("You do not have shell/worktree authority"));
        assert!(message.contains("coder commit and push the reviewed"));
        assert!(message.contains("treat that as durable pushed-branch evidence"));
        assert!(message.contains("do not ask the issue lead for an"));
        assert!(message.contains("github_create_pr returns an open PR"));
        assert!(message.contains("PR handoff evidence while keeping ordinary issue-mob work"));
        assert!(message.contains("Do not ask issue agents to move issues into configured Review"));
        assert!(message.contains("Merge is not implicit repo authority"));
        assert!(message.contains("operator explicitly approves"));
        assert!(message.contains("specific PR merge"));
    }

    fn orchestrator_for_definition(definition: WorkflowDefinition) -> Orchestrator {
        let (refresh_tx, refresh_rx) = mpsc::channel(1);
        drop(refresh_tx);
        let native_execution = definition
            .config
            .get("mob")
            .and_then(|mob| mob.get("backend"))
            .and_then(|backend| backend.as_str())
            == Some("native-execution");
        let workflow_path = PathBuf::from("/tmp/fugue.yaml");
        let manager =
            WorkflowConfigManager::from_definition_for_test(workflow_path, definition).unwrap();
        if native_execution {
            Orchestrator::new_with_native_execution_host(
                manager,
                refresh_rx,
                Arc::new(InMemoryNativeMobExecutionHost::new()),
            )
            .without_native_worker_execution()
        } else {
            Orchestrator::new(manager, refresh_rx).without_native_worker_execution()
        }
    }

    fn native_kapellmeister_workflow_definition() -> WorkflowDefinition {
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "linear_labels": ["kapellmeister-safe"]
                }
            }),
        );
        definition
    }

    fn native_control_plane_config() -> ServiceConfig {
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "linear",
                "api_key": "token",
                "project_slug": "project",
                "intake_states": ["Todo"],
                "active_states": ["Rework"],
                "terminal_states": ["Ready for review", "Blocked", "Done", "Canceled", "Duplicate"]
            }),
        );
        definition.config.insert(
            "linear_runtime".to_string(),
            json!({
                "provisioning": {"mode": "validate"},
                "lanes": {
                    "parked": "Backlog",
                    "ready": "Todo",
                    "review_ready": "Ready for review",
                    "rework": "Rework",
                    "blocked": "Blocked",
                    "success": "Done",
                    "canceled": "Canceled",
                    "duplicate": "Duplicate"
                },
                "required_states": []
            }),
        );
        definition.config.insert(
            "final_review".to_string(),
            json!({
                "enabled": true,
                "mode": "manual",
                "lane": "Final Review & Merge",
                "rework_lane": "Planning",
                "blocked_lane": "Blocked",
                "success_lane": "Done",
                "reviewers": [
                    {"id": "architecture", "profile": "final_architecture_reviewer", "required": true}
                ]
            }),
        );
        ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap()
    }

    fn native_orchestrator_for_definition(
        definition: WorkflowDefinition,
    ) -> (Orchestrator, Arc<RecordingNativeMobExecutionHost>) {
        let (refresh_tx, refresh_rx) = mpsc::channel(1);
        drop(refresh_tx);
        let manager = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap();
        let native_host = Arc::new(RecordingNativeMobExecutionHost::default());
        (
            Orchestrator::new_with_native_execution_host(manager, refresh_rx, native_host.clone()),
            native_host,
        )
    }

    #[tokio::test]
    async fn disabled_independent_review_cannot_start_or_create_runtime_work() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_workflow_definition());
        let request = IndependentReviewStartRequest {
            issue: issue_ref_for_identifier("LUC-631"),
            spawn_input: SpawnOrResumeExecutionInput {
                issue: issue_ref_for_identifier("LUC-631"),
                workspace_key: workspace_key_for_identifier("LUC-631"),
                template_id: crate::tools::MobTemplateId("independent_review".to_string()),
                worktree_path: None,
                instructions: None,
                idempotency_key: IdempotencyKey("disabled-review".to_string()),
            },
            pull_request: pr_snapshot(206, "luka/luc-631", "head-a"),
            orchestrator_profile: "review_orchestrator".to_string(),
        };
        let result = orchestrator
            .handle_kapellmeister_start_independent_review(request)
            .await;
        assert!(!result.accepted);
        assert!(result.message.contains("disabled by configuration"));
        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 0);
    }

    #[tokio::test]
    async fn independent_review_start_derives_worktree_path_and_uses_plan_launch() {
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            }),
        );
        definition.config.insert(
            "independent_review".to_string(),
            json!({
                "enabled": true,
                "lane": "Review",
                "orchestrator_profile": "review_orchestrator"
            }),
        );
        let (orchestrator, native_host) = native_orchestrator_for_definition(definition);
        let request = IndependentReviewStartRequest {
            issue: issue_ref_for_identifier("LUC-631"),
            spawn_input: SpawnOrResumeExecutionInput {
                issue: issue_ref_for_identifier("LUC-631"),
                workspace_key: workspace_key_for_identifier("LUC-631"),
                template_id: crate::tools::MobTemplateId("independent_review".to_string()),
                worktree_path: None,
                instructions: Some("Run independent review".to_string()),
                idempotency_key: IdempotencyKey(
                    "independent-review:meerkat-fugue:206:head-a".to_string(),
                ),
            },
            pull_request: pr_snapshot(206, "luka/luc-631", "head-a"),
            orchestrator_profile: "independent_review".to_string(),
        };

        let result = orchestrator
            .handle_kapellmeister_start_independent_review(request)
            .await;

        assert!(result.accepted, "{}", result.message);
        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 1);
        assert_eq!(
            native_host.spawned_inputs()[0].worktree_path.as_deref(),
            Some("/tmp/fugue-test-workspaces/LUC-631")
        );
    }

    #[tokio::test]
    async fn fugue_ims_kapellmeister_start_issue_uses_sqlite_issue_source() {
        let temp = tempfile::tempdir().unwrap();
        let ims_path = temp.path().join("fugue-ims.sqlite");
        let mut tracker = crate::fugue_ims::fugue_ims_tracker_config(&ims_path);
        tracker.project_slug = Some("lukacf/meerkat-fugue".into());
        crate::fugue_ims::create_issue_for_config(
            &tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("FUG-100".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Run without Linear".to_string(),
                description: Some("SQLite-backed IMS issue".to_string()),
                stage: crate::ims::ImsStageName("Todo".to_string()),
                labels: vec!["kapellmeister-safe".to_string()],
            },
        )
        .unwrap();

        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "fugue-ims",
                "endpoint": ims_path.display().to_string(),
                "project_slug": "lukacf/meerkat-fugue",
                "intake_states": ["Todo"],
                "active_states": ["Todo", "Rework", "Planning", "Implementing", "Reviewing"],
                "terminal_states": ["Done", "Canceled", "Duplicate"]
            }),
        );
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": temp.path().join("workspaces").display().to_string() }),
        );
        let (orchestrator, native_host) = native_orchestrator_for_definition(definition);

        let result = orchestrator
            .handle_kapellmeister_start_issue("FUG-100".to_string())
            .await;

        assert!(result.accepted, "{}", result.message);
        assert_eq!(native_host.spawn_call_count(), 1);
    }

    #[tokio::test]
    async fn kapellmeister_start_issue_allows_configured_review_ready_rework_resume() {
        let temp = tempfile::tempdir().unwrap();
        let ims_path = temp.path().join("fugue-ims.sqlite");
        let mut tracker = crate::fugue_ims::fugue_ims_tracker_config(&ims_path);
        tracker.project_slug = Some("lukacf/meerkat-fugue".into());
        crate::fugue_ims::create_issue_for_config(
            &tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("FUG-101".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Resume review-ready issue for rework".to_string(),
                description: Some("SQLite-backed IMS issue".to_string()),
                stage: crate::ims::ImsStageName("Ready for review".to_string()),
                labels: vec!["kapellmeister-safe".to_string()],
            },
        )
        .unwrap();

        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "linear_runtime".to_string(),
            json!({
                "provisioning": {"mode": "validate"},
                "lanes": {
                    "parked": "Backlog",
                    "ready": "Todo",
                    "review_ready": "Ready for review",
                    "blocked": "Blocked",
                    "success": "Done",
                    "canceled": "Canceled",
                    "duplicate": "Duplicate"
                },
                "required_states": []
            }),
        );
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "fugue-ims",
                "endpoint": ims_path.display().to_string(),
                "project_slug": "lukacf/meerkat-fugue",
                "intake_states": ["Todo"],
                "active_states": ["Todo", "Rework", "Planning", "Implementing", "Reviewing"],
                "terminal_states": ["Ready for review", "Blocked", "Done", "Canceled", "Duplicate"]
            }),
        );
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": temp.path().join("workspaces").display().to_string() }),
        );
        let (orchestrator, native_host) = native_orchestrator_for_definition(definition);

        let result = orchestrator
            .handle_kapellmeister_start_issue("FUG-101".to_string())
            .await;

        assert!(result.accepted, "{}", result.message);
        assert_eq!(native_host.spawn_call_count(), 1);
    }

    fn recorded_error(
        operation: FugueToolOperation,
        idempotency_key: Option<IdempotencyKey>,
    ) -> FugueToolResult {
        FugueToolResult::Err {
            error: crate::tools::FugueToolError {
                operation,
                code: crate::tools::FugueToolErrorCode::UnsupportedOperation,
                message: "recorded".to_string(),
                retry: crate::tools::RetryAdvice::DoNotRetry,
                idempotency_key,
            },
            metadata: ToolExecutionMetadata::default(),
        }
    }

    #[test]
    fn shared_parked_and_intake_status_still_starts_native_work() {
        let mut config = native_control_plane_config();
        config.linear.lanes.parked = "Open".into();
        config.linear.lanes.ready = "Open".into();
        config.tracker.intake_states = vec!["Open".into()];
        config.tracker.active_states.push("Open".into());
        let issue = issue_in_lane("Open");
        assert_eq!(
            classify_issue_lane(&config, "Open"),
            IssueLaneClassification::Intake
        );
        assert!(!is_native_parked_state(&config, "Open"));
        assert!(kapellmeister_start_issue_block_reason(&config, &issue).is_none());
        assert!(
            !issue_lane_control_plane_signals(
                &config,
                &issue,
                IssueLaneObservation::FirstSeen {
                    identifier: issue.identifier.clone(),
                    current: "Open".into(),
                },
                None,
            )
            .is_empty()
        );
    }

    #[test]
    fn lane_classifier_covers_native_control_plane_lanes() {
        let config = native_control_plane_config();

        assert_eq!(
            classify_issue_lane(&config, "Todo"),
            IssueLaneClassification::Intake
        );
        assert_eq!(
            classify_issue_lane(&config, "Planning"),
            IssueLaneClassification::ProjectedActive
        );
        assert_eq!(
            classify_issue_lane(&config, "Implementing"),
            IssueLaneClassification::ProjectedActive
        );
        assert_eq!(
            classify_issue_lane(&config, "Reviewing"),
            IssueLaneClassification::ProjectedActive
        );
        assert_eq!(
            classify_issue_lane(&config, "Rework"),
            IssueLaneClassification::Intake
        );
        for lane in [
            "Ready for review",
            "Final Review & Merge",
            "Blocked",
            "Done",
            "Canceled",
            "Duplicate",
        ] {
            assert_eq!(
                classify_issue_lane(&config, lane),
                IssueLaneClassification::AttentionOrTerminal
            );
        }
        assert_eq!(
            classify_issue_lane(&config, "In Progress"),
            IssueLaneClassification::UnexpectedNonterminal
        );
    }

    #[test]
    fn recent_signal_deduper_accepts_once_per_signal_id() {
        let mut deduper = RecentSignalDeduper::new(2);
        let first = ShadowSignalId("sig-first".to_string());
        let second = ShadowSignalId("sig-second".to_string());
        let third = ShadowSignalId("sig-third".to_string());

        assert!(deduper.accept(&first));
        assert!(!deduper.accept(&first));
        assert!(deduper.accept(&second));
        assert!(deduper.accept(&third));
        assert!(deduper.accept(&first));
    }

    #[test]
    fn first_seen_intake_lanes_report_to_kapellmeister_without_runtime() {
        let config = native_control_plane_config();
        for (lane, expected) in [
            ("Todo", IssueLaneClassification::Intake),
            ("Rework", IssueLaneClassification::Intake),
            (
                "In Progress",
                IssueLaneClassification::UnexpectedNonterminal,
            ),
        ] {
            let issue = issue_in_lane(lane);
            let signals = issue_lane_control_plane_signals(
                &config,
                &issue,
                IssueLaneObservation::FirstSeen {
                    identifier: issue.identifier.clone(),
                    current: lane.to_string(),
                },
                None,
            );
            let has_repair = signals.iter().any(|signal| {
                matches!(
                    &signal.kind,
                    KapellmeisterShadowSignalKind::LegacyLinearLaneRepairSuggested { .. }
                )
            });
            let expects_repair =
                crate::linear_lane_state::legacy_linear_lane_repair(&config, lane).is_some();
            assert_eq!(signals.len(), if expects_repair { 2 } else { 1 });
            assert_eq!(has_repair, expects_repair);
            let classification = signals.iter().find_map(|signal| {
                if let KapellmeisterShadowSignalKind::LinearIssueFirstSeen {
                    classification, ..
                } = &signal.kind
                {
                    Some(*classification)
                } else {
                    None
                }
            });
            assert_eq!(classification, Some(expected));
        }

        let issue = issue_in_lane("Backlog");
        assert!(
            issue_lane_control_plane_signals(
                &config,
                &issue,
                IssueLaneObservation::FirstSeen {
                    identifier: issue.identifier.clone(),
                    current: issue.state.clone(),
                },
                None,
            )
            .is_empty()
        );
    }

    #[test]
    fn active_issue_moved_to_rework_reports_as_intake_signal_with_running_mob_context() {
        let config = native_control_plane_config();
        let issue = issue_in_lane("Rework");
        let running_mob = KapellmeisterRunningMobObservation {
            run_id: MobRunId("native-execution:LUC-168".to_string()),
            status: MobRunStatus::Running,
            phase_labels: vec!["fugue:phase:implementing".to_string()],
        };

        let signals = issue_lane_control_plane_signals(
            &config,
            &issue,
            IssueLaneObservation::Changed {
                identifier: issue.identifier.clone(),
                previous: "Implementing".to_string(),
                current: "Rework".to_string(),
            },
            Some(running_mob.clone()),
        );

        assert_eq!(signals.len(), 2);
        assert!(signals.iter().any(|signal| matches!(
            &signal.kind,
            KapellmeisterShadowSignalKind::LegacyLinearLaneRepairSuggested { .. }
        )));
        let (from_state, to_state, classification, observed_running_mob) = signals
            .iter()
            .find_map(|signal| {
                if let KapellmeisterShadowSignalKind::LinearLaneChanged {
                    from_state,
                    to_state,
                    classification,
                    running_mob,
                    ..
                } = &signal.kind
                {
                    Some((from_state, to_state, classification, running_mob))
                } else {
                    None
                }
            })
            .expect("expected lane-changed signal");
        assert_eq!(
            from_state,
            &LinearStateRef::Name {
                name: "Implementing".to_string()
            }
        );
        assert_eq!(
            to_state,
            &LinearStateRef::Name {
                name: "Rework".to_string()
            }
        );
        assert_eq!(*classification, IssueLaneClassification::Intake);
        assert_eq!(observed_running_mob.as_ref(), Some(&running_mob));
    }

    #[test]
    fn unchanged_open_issue_without_runtime_is_reported_periodically() {
        let config = native_control_plane_config();
        let mut issue = issue_in_lane("In Progress");
        issue.labels = vec!["native".to_string(), "kapellmeister-safe".to_string()];

        let signals = issue_lane_control_plane_signals(
            &config,
            &issue,
            IssueLaneObservation::Unchanged,
            None,
        );

        assert_eq!(signals.len(), 2);
        assert!(signals.iter().any(|signal| matches!(
            &signal.kind,
            KapellmeisterShadowSignalKind::LegacyLinearLaneRepairSuggested { .. }
        )));
        let (lane, classification) = signals
            .iter()
            .find_map(|signal| {
                if let KapellmeisterShadowSignalKind::OpenIssueWithoutRuntime {
                    lane,
                    classification,
                    ..
                } = &signal.kind
                {
                    Some((lane, classification))
                } else {
                    None
                }
            })
            .expect("expected open_issue_without_runtime signal");
        assert_eq!(
            lane,
            &LinearStateRef::Name {
                name: "In Progress".to_string()
            }
        );
        assert_eq!(
            *classification,
            IssueLaneClassification::UnexpectedNonterminal
        );
    }

    #[test]
    fn repo_signal_prompt_routes_review_and_deploy_to_kapellmeister_tools() {
        let config = native_control_plane_config();
        let issue = issue_in_lane("Deploy");
        let signals = issue_lane_control_plane_signals(
            &config,
            &issue,
            IssueLaneObservation::FirstSeen {
                identifier: issue.identifier.clone(),
                current: issue.state.clone(),
            },
            None,
        );

        let message = repo_kapellmeister_signal_message(&signals).expect("signal message");

        assert!(message.contains("For a configured Review action or shared review holding status"));
        assert!(message.contains("fugue_start_independent_review"));
        assert!(message.contains("For a configured Deploy lane"));
        assert!(message.contains("do not start the implementation mob"));
        assert!(message.contains("Fugue host handles Deploy as a guarded mechanical transaction"));
    }

    #[test]
    fn runtime_recovered_signal_carries_linear_and_projected_lanes() {
        let config = native_control_plane_config();
        let issue = issue_in_lane("In Progress");
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:LUC-168".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(40, 0).unwrap(),
            completed_at: None,
        };
        let native_run = NativeRunHandle {
            run_id: run.run_id.clone(),
            observed_events: vec![MobEventSnapshot {
                sequence: 1,
                occurred_at: Utc.timestamp_opt(40, 0).unwrap(),
                kind: crate::tools::MobEventKind::MemberStarted {
                    member: "reviewer".to_string(),
                },
                message: None,
            }],
            recovery: NativeStatusRecovery {
                workspace: Some(RecoveredWorkspaceEvidence {
                    path: "/tmp/LUC-168".to_string(),
                    branch: Some(BranchName("luc-168-branch".to_string())),
                    head_sha: Some(GitSha("abc123".to_string())),
                    upstream_branch: Some(BranchName("origin/luc-168-branch".to_string())),
                    upstream_head_sha: Some(GitSha("abc123".to_string())),
                    dirty: false,
                }),
                ..NativeStatusRecovery::default()
            },
        };

        let signal = runtime_recovered_signal(&config, &issue, &run, Some(&native_run));

        let KapellmeisterShadowSignalKind::RuntimeRecovered {
            run_id,
            linear_lane,
            projected_lane,
            workspace,
            ..
        } = signal.kind
        else {
            panic!("expected runtime recovered signal");
        };
        assert_eq!(run_id, MobRunId("native-execution:LUC-168".to_string()));
        assert_eq!(
            linear_lane,
            LinearStateRef::Name {
                name: "In Progress".to_string()
            }
        );
        assert_eq!(
            projected_lane,
            Some(LinearStateRef::Name {
                name: "Reviewing".to_string()
            })
        );
        let workspace = workspace.expect("runtime recovery should carry workspace evidence");
        assert_eq!(
            workspace.branch,
            Some(BranchName("luc-168-branch".to_string()))
        );
        assert_eq!(workspace.head_sha, Some(GitSha("abc123".to_string())));
        assert_eq!(
            workspace.upstream_branch,
            Some(BranchName("origin/luc-168-branch".to_string()))
        );
        assert_eq!(
            workspace.upstream_head_sha,
            Some(GitSha("abc123".to_string()))
        );
        assert!(!workspace.dirty);
    }

    #[test]
    fn comment_snapshot_changes_emit_added_changed_and_deleted_signals() {
        let issue = issue_in_lane("Todo");
        let observed_at = Utc.timestamp_opt(30, 0).unwrap();
        let pr_review_body = "Operator review for PR #187 after rework: not accepted yet.\n\nBlocking findings:\n\n1. `src/orchestrator.rs:757` only computes `native_final_pr_gate_start_request_from_handoff(...)` and passes it into Workpad text.\n\nRequired rework:\n\n- Wire the final gate into the real Kapellmeister/control-plane path.\n";
        let signals = issue_comment_control_plane_signals(
            &issue,
            vec![
                IssueCommentChange::Added {
                    comment_id: LinearCommentId("added".to_string()),
                    body_markdown: pr_review_body.to_string(),
                },
                IssueCommentChange::Changed {
                    comment_id: LinearCommentId("changed".to_string()),
                    body_markdown: "Operator review: requesting changes.".to_string(),
                },
                IssueCommentChange::Deleted {
                    comment_id: LinearCommentId("deleted".to_string()),
                },
            ],
            observed_at,
        );

        assert_eq!(signals.len(), 3);
        match &signals[0].kind {
            KapellmeisterShadowSignalKind::LinearIssueCommented {
                body_markdown,
                operator_review_finding,
                ..
            } => {
                assert_eq!(body_markdown.as_deref(), Some(pr_review_body));
                assert_eq!(
                    operator_review_finding.as_deref(),
                    Some("Operator review for PR #187 after rework: not accepted yet.")
                );
            }
            other => panic!("expected comment-added signal, got {other:?}"),
        }
        match &signals[1].kind {
            KapellmeisterShadowSignalKind::LinearIssueCommentChanged {
                body_markdown,
                operator_review_finding,
                ..
            } => {
                assert_eq!(
                    body_markdown.as_deref(),
                    Some("Operator review: requesting changes.")
                );
                assert_eq!(
                    operator_review_finding.as_deref(),
                    Some("Operator review: requesting changes.")
                );
            }
            other => panic!("expected comment-changed signal, got {other:?}"),
        }
        assert!(matches!(
            signals[2].kind,
            KapellmeisterShadowSignalKind::LinearIssueCommentDeleted { .. }
        ));
    }

    fn guarded_context(issue: &Issue) -> KapellmeisterDecisionContext {
        kapellmeister_context_for_issue(issue, None)
    }

    #[test]
    fn kapellmeister_context_reports_issue_state_as_observation() {
        let mut issue = active_issue_with_labels(&["kapellmeister-safe"]);
        issue.state = "Todo".to_string();

        let context = guarded_context(&issue);

        assert_eq!(context.issue, Some(issue_ref_for_identifier("LUC-168")));
        assert_eq!(
            context.observations,
            vec![KapellmeisterHostObservation::LinearIssueObserved {
                issue: issue_ref_for_identifier("LUC-168"),
                state: LinearStateRef::Name {
                    name: "Todo".to_string()
                },
                labels: vec!["kapellmeister-safe".to_string()],
            }]
        );
    }

    #[test]
    fn native_launch_refresh_stops_when_user_canceled_stale_candidate() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Canceled".to_string();

        assert!(matches!(
            native_launch_refresh_action(&config, issue),
            NativeLaunchRefreshAction::Stop { reason }
                if reason.contains("terminal before native launch")
        ));
    }

    #[test]
    fn native_launch_refresh_continues_for_fresh_rework_observation_lane() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  lanes:
    rework: Rework
linear_projections:
  standard_projection:
    node_lanes:
      planner: Rework
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Rework]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Rework
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: standard_projection
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Rework".to_string();

        assert!(matches!(
            native_launch_refresh_action(&config, issue),
            NativeLaunchRefreshAction::Continue(_)
        ));
    }

    #[test]
    fn labeled_guarded_issue_uses_kapellmeister_policy_mode() {
        let config = guarded_config();
        let issue = issue_with_labels(&["kapellmeister-safe"]);

        assert_eq!(
            classify_issue_policy_mode(&issue, &config),
            IssuePolicyMode::Kapellmeister
        );
    }

    #[test]
    fn unlabeled_issue_stays_on_kapellmeister_policy_mode() {
        let config = guarded_config();
        let issue = issue_with_labels(&[]);

        assert_eq!(
            classify_issue_policy_mode(&issue, &config),
            IssuePolicyMode::Kapellmeister
        );
    }

    #[test]
    fn native_prompt_control_uses_kapellmeister_policy_without_labels() {
        let config = native_control_plane_config();
        let issue = issue_in_lane("Todo");

        assert_eq!(
            classify_issue_policy_mode(&issue, &config),
            IssuePolicyMode::Kapellmeister
        );
    }

    #[test]
    fn native_backend_selection_uses_native_only_boundary() {
        let native_config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let issue = active_issue_with_labels(&["native"]);

        assert_eq!(
            dispatch_backend_for_issue(&issue, &native_config),
            DispatchBackend::NativeMob
        );

        let guarded_config_value = guarded_config();
        assert_eq!(
            dispatch_backend_for_issue(&issue, &guarded_config_value),
            DispatchBackend::NativeMob
        );
    }

    #[test]
    fn kapellmeister_observation_suppresses_all_host_dispatch() {
        assert!(kapellmeister_observation_suppresses_dispatch(
            IssuePolicyMode::Kapellmeister,
            DispatchBackend::NativeMob
        ));
    }

    #[test]
    fn native_guarded_kapellmeister_mode_uses_prompt_control_loop() {
        let mut definition = native_workflow_definition();
        definition
            .config
            .entry("kapellmeister".to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("kapellmeister config should be an object")
            .insert("authority_mode".to_string(), json!("guarded_labeled"));
        let native_config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();

        assert!(native_kapellmeister_prompt_control_enabled(&native_config));

        let guarded_config_value = guarded_config();
        assert!(!native_kapellmeister_prompt_control_enabled(
            &guarded_config_value
        ));
    }

    #[test]
    fn native_prompt_control_keeps_unexpected_nonterminal_lanes_running() {
        let config = native_control_plane_config();

        assert_eq!(
            interpret_native_prompt_control_lane(&config.tracker, "In Progress"),
            HumanLaneAction::Continue
        );
        assert_eq!(
            interpret_native_prompt_control_lane(&config.tracker, "Done"),
            HumanLaneAction::StopAndCleanup
        );
    }

    #[test]
    fn native_launch_refresh_continues_for_unexpected_nonterminal_prompt_control_lane() {
        let config = native_control_plane_config();
        let issue = issue_in_lane("In Progress");

        assert!(matches!(
            native_launch_refresh_action(&config, issue),
            NativeLaunchRefreshAction::Continue(_)
        ));
    }

    #[test]
    fn native_runtime_restore_keys_prioritize_active_lanes_before_intake() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let mut todo = issue("issue-1", "LUC-701", Some(1), 10);
        todo.state = "Todo".to_string();
        todo.updated_at = Some(Utc.timestamp_opt(30, 0).unwrap());
        let mut planning = issue("issue-2", "LUC-702", Some(1), 10);
        planning.state = "Planning".to_string();
        planning.updated_at = Some(Utc.timestamp_opt(20, 0).unwrap());
        let mut implementing = issue("issue-3", "LUC-703", Some(1), 10);
        implementing.state = "Implementing".to_string();
        implementing.updated_at = Some(Utc.timestamp_opt(10, 0).unwrap());

        let keys = native_runtime_restore_keys(&config, &[todo, planning, implementing], 2);

        assert_eq!(
            keys,
            vec![
                "LUC-702-implementation_mob".to_string(),
                "LUC-703-implementation_mob".to_string(),
                "LUC-701-implementation_mob".to_string(),
            ]
        );
        assert!(native_runtime_restore_keys(&config, &[issue_in_lane("Planning")], 0).is_empty());
    }

    #[test]
    fn native_runtime_restore_keys_skip_terminal_and_parked_lanes() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();

        assert_eq!(
            native_runtime_restore_keys(
                &config,
                &[issue_in_lane("Done"), issue_in_lane("Backlog")],
                2
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn native_runtime_restore_keys_include_more_candidates_than_capacity() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();

        assert_eq!(
            native_runtime_restore_keys(
                &config,
                &[
                    issue("issue-1", "LUC-701", Some(1), 10),
                    issue("issue-2", "LUC-702", Some(1), 10),
                ],
                1
            ),
            vec![
                "LUC-701-implementation_mob".to_string(),
                "LUC-702-implementation_mob".to_string(),
            ]
        );
    }

    #[test]
    fn native_mob_dispatch_accepts_active_compact_rework_lanes() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let state = OrchestratorState {
            poll_interval_ms: 30_000,
            max_concurrent_agents: 2,
            running: HashMap::new(),
            claimed: HashSet::new(),
            retry_attempts: HashMap::new(),
            completed: HashSet::new(),
            meerkat_totals: TokenTotals::default(),
            ended_runtime_seconds: 0.0,
            meerkat_rate_limits: None,
            last_error: None,
            recent_events: HashMap::new(),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Progress".to_string();

        assert!(should_dispatch_locked(&issue, &state, &config, false));
    }

    #[test]
    fn parked_native_issue_lane_stops_without_retry_or_replan() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();

        assert_eq!(
            interpret_human_lane_move(&config.tracker, "Backlog"),
            HumanLaneAction::StopNoRetry
        );
    }

    #[test]
    fn native_issue_instructions_use_configured_launch_context_policy() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let issue = active_issue_with_labels(&["native"]);
        let selected = config.select_template_for_issue(&issue).unwrap();
        let worktree = PathBuf::from("/tmp/fugue-test-workspaces/LUC-168");

        let instructions = config
            .render_native_execution_instructions(
                &issue,
                None,
                &worktree,
                &selected.template_id,
                selected.template,
            )
            .unwrap();

        assert!(instructions.contains("Work on LUC-168"));
        assert!(instructions.contains("Native launch context from config"));
        assert!(instructions.contains("/tmp/fugue-test-workspaces/LUC-168"));
        assert!(instructions.contains("implementation_mob"));
        assert!(instructions.contains("planner"));
        assert!(instructions.contains("coder"));
        assert!(instructions.contains("reviewer"));
        assert!(instructions.contains("configured handoff"));
        assert!(!instructions.contains("Native execution mob launch context:"));
    }

    #[test]
    fn restart_reconciliation_recovers_active_native_mob_state_from_run_issue_and_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let workspace_root = temp.path().join("workspaces");
        let workspace_path = workspace_root.join("LUC-168");
        std::fs::create_dir_all(&workspace_path).unwrap();
        run_git(&workspace_path, &["init"]);
        run_git(
            &workspace_path,
            &["config", "user.email", "fugue@example.invalid"],
        );
        run_git(&workspace_path, &["config", "user.name", "Fugue Test"]);
        std::fs::write(workspace_path.join("README.md"), "hi\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "initial"]);
        run_git(&workspace_path, &["checkout", "-b", "codex/LUC-168"]);
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": workspace_root.display().to_string() }),
        );
        let config = WorkflowConfigManager::from_definition_for_test(
            temp.path().join("fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let issue = active_issue_with_labels(&["native"]);
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-168".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::WaitingForInput,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let workspace = WorkspaceManager::for_service(&config);

        let (recovered_issue, entry) = recover_native_running_entry(
            &run,
            std::slice::from_ref(&issue),
            &config,
            &workspace,
            None,
        )
        .unwrap();

        assert_eq!(recovered_issue.identifier, "LUC-168");
        assert_eq!(entry.identifier, "LUC-168");
        assert_eq!(
            entry.workspace_path,
            workspace_root.join("LUC-168").display().to_string()
        );
        assert_eq!(
            entry.session.session_id.as_deref(),
            Some("native-execution:native-execution:LUC-168")
        );
        assert_eq!(
            entry.session.last_meerkat_event.as_deref(),
            Some("native_mob_recovered_after_restart")
        );
        assert!(entry.handle.is_none());
        assert!(entry.native_run.is_some());
        let row = running_row(&recovered_issue.id, &entry);
        assert_eq!(row.backend, StatusBackend::NativeMob);
        let native_recovery = row.native_recovery.as_ref().unwrap();
        assert_eq!(
            native_recovery.run_id,
            "native-execution:native-execution:LUC-168"
        );
        assert_eq!(
            native_recovery.workspace_path.as_deref(),
            Some(workspace_path.display().to_string().as_str())
        );
        assert_eq!(
            native_recovery.workspace_branch.as_deref(),
            Some("codex/LUC-168")
        );
        assert!(native_recovery.workspace_head_sha.is_some());
        assert_eq!(native_recovery.workspace_dirty, Some(false));
        assert_eq!(
            native_recovery.linear_branch.as_deref(),
            Some("codex/LUC-168")
        );
        assert!(native_recovery.review_required.is_none());
    }

    #[test]
    fn restart_reconciliation_keeps_nonterminal_native_mob_from_noncanonical_lane() {
        let temp = tempfile::tempdir().unwrap();
        let workspace_root = temp.path().join("workspaces");
        let workspace_path = workspace_root.join("LUC-168");
        std::fs::create_dir_all(&workspace_path).unwrap();
        run_git(&workspace_path, &["init"]);
        run_git(
            &workspace_path,
            &["config", "user.email", "fugue@example.invalid"],
        );
        run_git(&workspace_path, &["config", "user.name", "Fugue Test"]);
        std::fs::write(workspace_path.join("README.md"), "hi\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "initial"]);
        run_git(&workspace_path, &["checkout", "-b", "codex/LUC-168"]);
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "linear",
                "api_key": "token",
                "project_slug": "project",
                "intake_states": ["Todo"],
                "active_states": ["Todo", "Planning", "Implementing", "Reviewing", "Rework"],
                "terminal_states": ["Done", "Canceled", "Ready for review"]
            }),
        );
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": workspace_root.display().to_string() }),
        );
        let config = WorkflowConfigManager::from_definition_for_test(
            temp.path().join("fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Progress".to_string();
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-168".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let workspace = WorkspaceManager::for_service(&config);

        let (recovered_issue, entry) = recover_native_running_entry(
            &run,
            std::slice::from_ref(&issue),
            &config,
            &workspace,
            None,
        )
        .unwrap();

        assert_eq!(recovered_issue.identifier, "LUC-168");
        assert_eq!(entry.issue.state, "In Progress");
        assert!(
            entry.native_run.is_some(),
            "nonterminal noncanonical projection lanes must not hide a live native mob after restart"
        );
    }

    #[test]
    fn completed_native_status_reconstructs_handoff_from_durable_evidence() {
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Review".to_string();
        let events = vec![
            native_event(1, crate::tools::MobEventKind::RunStarted, None),
            native_event(
                2,
                crate::tools::MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCreatePr,
                },
                Some("opened https://github.com/lukacf/meerkat-fugue/pull/7"),
            ),
            native_event(
                3,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                Some("native mob done"),
            ),
        ];
        let workpad = "## Fugue Workpad\n\n- [x] Native mob run: `native-execution:native-execution:LUC-168` (Succeeded).\n\n### Pull request or Handoff\n- [x] PR #7 opened.";
        let github = recover_native_github_evidence(pr_snapshot(
            7,
            "codex/LUC-168",
            "abc123abc123abc123abc123abc123abc123abcd",
        ));
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
            branch: Some(BranchName("codex/LUC-168".to_string())),
            head_sha: Some(GitSha(
                "abc123abc123abc123abc123abc123abc123abcd".to_string(),
            )),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };

        let recovery = native_status_recovery(
            &run,
            Some(recover_native_linear_evidence(&issue, Some(workpad))),
            Some(github),
            Some(workspace),
            &events,
            Some(2),
        );

        assert!(recovery.review.is_none());
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: events,
            recovery,
        });
        assert_eq!(
            row.github_pr_url.as_deref(),
            Some("https://github.com/lukacf/meerkat-fugue/pull/7")
        );
        assert_eq!(row.github_pr_state, Some(PullRequestState::Open));
        assert_eq!(
            row.github_head_sha.as_deref(),
            Some("abc123abc123abc123abc123abc123abc123abcd")
        );
        assert_eq!(
            row.workpad_run_id.as_deref(),
            Some("native-execution:native-execution:LUC-168")
        );
        assert!(row.workpad_mentions_pr);
        assert!(row.review_required.is_none());
    }

    #[test]
    fn active_native_status_accepts_draft_pr_as_reviewer_handoff() {
        let run_id = MobRunId("native-execution:native-execution:LUC-401:attempt-7".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-401")),
            workspace_key: Some(workspace_key_for_identifier("LUC-401")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Implementing".to_string();
        let head_sha = "b6fbecbb800fd3eee922a228ae5b8aae2e9764e2";
        let workpad =
            "- [x] PR #118: open draft at correct head `b6fbecbb800fd3eee922a228ae5b8aae2e9764e2`.";
        let mut pull_request = pr_snapshot(118, "fugue/luc-401", head_sha);
        pull_request.draft = true;
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-401".to_string(),
            branch: Some(BranchName("fugue/luc-401".to_string())),
            head_sha: Some(GitSha(head_sha.to_string())),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: true,
        };

        let recovery = native_status_recovery(
            &run,
            Some(recover_native_linear_evidence(&issue, Some(workpad))),
            Some(recover_native_github_evidence(pull_request)),
            Some(workspace),
            &[],
            Some(2),
        );
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: Vec::new(),
            recovery,
        });

        assert_eq!(row.github_pr_state, Some(PullRequestState::Open));
        assert_eq!(row.github_head_sha.as_deref(), Some(head_sha));
        assert!(row.review_required.is_none());
    }

    #[test]
    fn native_status_evidence_disagreement_requires_operator_review() {
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let issue = active_issue_with_labels(&["native"]);
        let workpad =
            "- [x] Native mob run: `native-execution:other-run` (Succeeded).\n- [x] PR #7 opened.";
        let github = recover_native_github_evidence(pr_snapshot(7, "different-branch", "def456"));
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
            branch: Some(BranchName("codex/LUC-168".to_string())),
            head_sha: Some(GitSha("abc123".to_string())),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCreatePr,
                },
                Some("opened PR #8"),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                None,
            ),
        ];

        let recovery = native_status_recovery(
            &run,
            Some(recover_native_linear_evidence(&issue, Some(workpad))),
            Some(github),
            Some(workspace),
            &events,
            Some(2),
        );
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: events,
            recovery,
        });

        let reason = row.review_required.unwrap();
        assert!(
            reason.contains("disagrees"),
            "unexpected review reason: {reason}"
        );
    }

    #[test]
    fn rework_with_unresolved_operator_finding_requires_review_after_external_claude_rerun() {
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Rework".to_string();
        let head_sha = "a2aadbfbb405e8f46ce2c03a3cdffd329da1fe22";
        let workpad = "- [x] Native mob run: `native-execution:native-execution:LUC-168` (Succeeded).\n- [x] Pull request: https://github.com/lukacf/meerkat-fugue/pull/7\n\n### Review findings\n- [ ] Source: blocking operator Rework comment must be addressed before rerunning Claude checks.\n- [ ] Tests: add regression coverage for the Rework handoff.\n- [ ] Docs: preserve the operator handoff note.\n";
        let events = vec![
            native_event(1, crate::tools::MobEventKind::RunStarted, None),
            native_event(
                2,
                crate::tools::MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCheckCi,
                },
                Some(
                    "reran Claude Code Review; failure is external anthropics/claude-code-action symlink directory mismatch",
                ),
            ),
            native_event(
                3,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                Some("external-only rerun finished"),
            ),
        ];
        let github = recover_native_github_evidence(pr_snapshot(7, "codex/LUC-168", head_sha));
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
            branch: Some(BranchName("codex/LUC-168".to_string())),
            head_sha: Some(GitSha(head_sha.to_string())),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };

        let recovery = native_status_recovery(
            &run,
            Some(recover_native_linear_evidence(&issue, Some(workpad))),
            Some(github),
            Some(workspace),
            &events,
            Some(2),
        );

        assert!(recovery.review.is_some());
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: events,
            recovery,
        });
        let reason = row.review_required.unwrap();
        assert!(reason.contains("Rework"), "unexpected reason: {reason}");
        assert!(reason.contains("review"), "unexpected reason: {reason}");
        assert!(reason.contains("Source"), "unexpected reason: {reason}");
        assert_eq!(row.github_head_sha.as_deref(), Some(head_sha));
        assert!(
            row.unresolved_review_finding
                .as_deref()
                .unwrap()
                .contains("Source")
        );
    }

    #[test]
    fn reviewer_red_workpad_requests_replan_instead_of_operator_review() {
        let run_id = MobRunId("native-execution:native-execution:LUC-264".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-264")),
            workspace_key: Some(workspace_key_for_identifier("LUC-264")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Reviewing".to_string();
        let workpad = "## Fugue Workpad\n\n#### Reviewer gate\n- [x] Reviewer decision: **red / return to Planning**.\n\n#### Reviewer findings requiring focused rework\n- [ ] Rework: connect the native GitHub repository scope to the actual production config-generation path.\n";
        let recovery = native_status_recovery(
            &run,
            Some(recover_native_linear_evidence(&issue, Some(workpad))),
            None,
            None,
            &[native_event(
                1,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                Some("reviewer requested rework"),
            )],
            Some(3),
        );
        let native_run = NativeRunHandle {
            run_id,
            observed_events: Vec::new(),
            recovery,
        };

        assert!(native_run_requests_replan(&native_run));
    }

    #[test]
    fn native_lane_move_accepts_idempotent_already_in_target_lane() {
        let output = MoveIssueOutput {
            issue: linear_issue_snapshot_in_state("Planning"),
            changed: false,
            previous_state: None,
        };

        assert!(native_lane_move_satisfied(&output));
    }

    #[test]
    fn native_pr_handoff_evidence_is_idempotent() {
        let entry = RunningEntry {
            issue: active_issue_with_labels(&["native"]),
            identifier: "LUC-277".to_string(),
            policy_mode: IssuePolicyMode::Kapellmeister,
            workspace_path: "/tmp/fugue-test-workspaces/LUC-277".to_string(),
            session: LiveSession::default(),
            started_at: Utc::now(),
            retry_attempt: Some(5),
            cancel: CancellationToken::new(),
            handle: None,
            termination: None,
            native_run: Some(NativeRunHandle {
                run_id: MobRunId("native-execution:native-execution:LUC-277:attempt-5".to_string()),
                observed_events: Vec::new(),
                recovery: NativeStatusRecovery::default(),
            }),
        };
        let pull_request = pr_snapshot(67, "luka/luc-277", "abc123");

        let once = append_native_pr_handoff_evidence(
            "## Fugue Workpad\n\n### Pull request or Handoff\n- [ ] Pending native mob output and PR handoff.",
            &entry,
            &pull_request,
        );
        let twice = append_native_pr_handoff_evidence(&once, &entry, &pull_request);

        assert_eq!(once, twice);
        assert!(once.contains("https://github.com/lukacf/meerkat-fugue/pull/67"));
        assert!(once.contains("attempt-5"));
        assert!(once.contains("PR handoff validations"));
        assert!(!once.contains("Human Review"));
        assert!(!once.contains("In Review"));
        assert!(!once.contains("Merging"));
    }

    fn ci_output(status: CiStatus) -> CheckCiOutput {
        CheckCiOutput {
            status,
            checks: vec![GitHubCheckSnapshot {
                name: "required-ci".to_string(),
                status,
                details_url: Some("https://github.example/checks/required-ci".to_string()),
                completed_at: (status != CiStatus::Pending).then(Utc::now),
            }],
        }
    }

    #[test]
    fn native_pr_handoff_monitor_replans_draft_pr_instead_of_passive_wait() {
        let mut pull_request = pr_snapshot(118, "fugue/luc-401", "abc123");
        pull_request.draft = true;
        let github = recover_native_github_evidence(pull_request);

        assert_eq!(
            native_pr_handoff_monitor_action(&github, Some(&ci_output(CiStatus::Success))),
            NativePrHandoffMonitorAction::Replan(
                NativePrHandoffReplanReason::DraftRequiresReviewerGate
            )
        );
    }

    #[test]
    fn native_pr_handoff_monitor_replans_when_ci_evidence_is_unavailable() {
        let github = recover_native_github_evidence(pr_snapshot(118, "fugue/luc-401", "abc123"));

        assert_eq!(
            native_pr_handoff_monitor_action(&github, None),
            NativePrHandoffMonitorAction::Replan(NativePrHandoffReplanReason::CiUnavailable)
        );
        assert!(!native_pr_handoff_monitor_blocks_dispatch(
            native_pr_handoff_monitor_action(&github, None)
        ));
    }

    #[test]
    fn native_pr_handoff_monitor_waiting_blocks_dispatch() {
        let github = recover_native_github_evidence(pr_snapshot(118, "fugue/luc-401", "abc123"));

        assert!(native_pr_handoff_monitor_blocks_dispatch(
            native_pr_handoff_monitor_action(&github, Some(&ci_output(CiStatus::Pending)))
        ));
    }

    #[test]
    fn native_pr_handoff_monitor_treats_empty_check_set_as_no_configured_ci() {
        let github = recover_native_github_evidence(pr_snapshot(118, "fugue/luc-401", "abc123"));
        let ci = CheckCiOutput {
            status: CiStatus::Pending,
            checks: Vec::new(),
        };

        assert_eq!(
            native_pr_handoff_monitor_action(&github, Some(&ci)),
            NativePrHandoffMonitorAction::Waiting(
                NativePrHandoffWaitingReason::AwaitingReviewOrMerge
            )
        );
        assert!(native_pr_handoff_monitor_blocks_dispatch(
            native_pr_handoff_monitor_action(&github, Some(&ci))
        ));
        let (decision, detail) = native_pr_handoff_monitor_signal_decision(
            native_pr_handoff_monitor_action(&github, Some(&ci)),
        );
        assert_eq!(decision, PrHandoffMonitorDecision::ReadyForDone);
        assert!(detail.contains("no checks were reported"));
        assert!(!detail.contains("move the issue to Done"));
    }

    #[test]
    fn native_pr_handoff_monitor_scans_external_nonterminal_lane_drift() {
        let mut config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        config.deploy.enabled = true;
        config.deploy.lane = "Deploy".to_string();
        config.independent_review.enabled = true;
        config.independent_review.lane = "Review".to_string();
        let mut issue = issue("issue-401", "LUC-401", Some(2), 10);

        issue.state = "In Progress".to_string();
        assert!(native_pr_handoff_monitor_candidate(&config, &issue));

        issue.state = "Deploy".to_string();
        assert!(native_pr_handoff_monitor_candidate(&config, &issue));
        assert!(native_deploy_lane_candidate(&config, &issue));

        issue.state = "Review".to_string();
        assert!(native_pr_handoff_monitor_candidate(&config, &issue));
        assert!(!native_deploy_lane_candidate(&config, &issue));

        issue.state = "Todo".to_string();
        assert!(!native_pr_handoff_monitor_candidate(&config, &issue));
        assert!(!native_deploy_lane_candidate(&config, &issue));

        issue.state = "Backlog".to_string();
        assert!(!native_pr_handoff_monitor_candidate(&config, &issue));

        issue.state = "Done".to_string();
        assert!(!native_pr_handoff_monitor_candidate(&config, &issue));
    }

    #[test]
    fn native_pr_handoff_monitor_routes_red_ci_back_to_planning() {
        let github = recover_native_github_evidence(pr_snapshot(118, "fugue/luc-401", "abc123"));

        assert_eq!(
            native_pr_handoff_monitor_action(&github, Some(&ci_output(CiStatus::Failure))),
            NativePrHandoffMonitorAction::Replan(NativePrHandoffReplanReason::CiFailed)
        );
    }

    #[test]
    fn native_pr_handoff_ci_passes_exact_candidate_to_inspector() {
        struct Inspector(std::sync::Mutex<Option<GitHubPullRequestSnapshot>>);
        impl NativeStatusGitHubInspector for Inspector {
            fn inspect_pr(&self, _: crate::tools::InspectPrInput) -> FugueToolResult {
                panic!("the CI read already has a candidate");
            }
            fn check_pr_ci(&self, candidate: &GitHubPullRequestSnapshot) -> FugueToolResult {
                *self.0.lock().unwrap() = Some(candidate.clone());
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubCheckCi(ci_output(CiStatus::Success)),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }
        let candidate = pr_snapshot(187, "feature", "df54ff6bba8830904fb69815e95b454b94b2fbfe");
        let inspector = Inspector(std::sync::Mutex::new(None));
        assert_eq!(
            inspect_native_pr_handoff_ci(&inspector, &candidate)
                .unwrap()
                .status,
            CiStatus::Success
        );
        assert_eq!(inspector.0.into_inner().unwrap(), Some(candidate));
    }

    #[test]
    fn native_pr_handoff_monitor_completes_only_after_merge() {
        let mut pull_request = pr_snapshot(118, "fugue/luc-401", "abc123");
        pull_request.state = PullRequestState::Merged;
        let github = recover_native_github_evidence(pull_request);

        assert_eq!(
            native_pr_handoff_monitor_action(&github, None),
            NativePrHandoffMonitorAction::Complete
        );
    }

    #[test]
    fn native_pr_handoff_monitor_materializes_final_gate_trigger_evidence() {
        let mut config = service_config(json!({}));
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Repo Gate".to_string(),
            rework_lane: "Repo Rework".to_string(),
            blocked_lane: "Repo Blocked".to_string(),
            success_lane: "Repo Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![
                crate::config::FinalReviewReviewerConfig {
                    id: "architecture".to_string(),
                    profile: "final_architecture_reviewer".to_string(),
                    required: true,
                },
                crate::config::FinalReviewReviewerConfig {
                    id: "security".to_string(),
                    profile: "final_security_reviewer".to_string(),
                    required: false,
                },
            ],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut issue = issue("issue-1", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "sha-a"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto final gate start");
        assert_eq!(start.reviewers.len(), 2);
        assert_eq!(start.trigger_mode, crate::config::FinalReviewMode::Auto);
        assert!(
            start
                .reviewers
                .iter()
                .all(|reviewer| !reviewer.merge_tool_allowed)
        );

        let section = native_pr_handoff_monitor_section(
            &github,
            Some(&ci_output(CiStatus::Success)),
            &NativePrHandoffMonitorAction::Waiting(
                NativePrHandoffWaitingReason::AwaitingReviewOrMerge,
            ),
            Some(&start),
        );
        assert!(section.contains("Final PR gate trigger: Auto"));
        assert!(section.contains("Kapellmeister owns typed verdict aggregation"));
        assert!(section.contains("2 configured final reviewer"));
    }

    fn final_gate_completion_event(
        verdicts: Vec<crate::final_pr_gate::FinalReviewVerdict>,
    ) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc::now(),
            kind: crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Succeeded,
                structured_output: Some(json!({ "verdicts": verdicts })),
            },
            message: None,
        }
    }

    async fn project_completion_for_test(
        status: FinalReviewVerdictStatus,
    ) -> (FinalPrGateRuntimePlan, RecordingFinalGateProjectionExecutor) {
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            }),
        );
        let (orchestrator, _native_host) = native_orchestrator_for_definition(definition);
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![
                crate::config::FinalReviewReviewerConfig {
                    id: "architecture".to_string(),
                    profile: "final_architecture_reviewer".to_string(),
                    required: true,
                },
                crate::config::FinalReviewReviewerConfig {
                    id: "tests".to_string(),
                    profile: "final_test_reviewer".to_string(),
                    required: true,
                },
            ],
        };
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Custom Final Gate".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "sha-a"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should produce request");
        let test_status = if status == FinalReviewVerdictStatus::RequestChanges {
            FinalReviewVerdictStatus::RequestChanges
        } else {
            FinalReviewVerdictStatus::Approve
        };
        let events = vec![final_gate_completion_event(vec![
            crate::final_pr_gate::FinalReviewVerdict {
                reviewer: "architecture".to_string(),
                status: FinalReviewVerdictStatus::Approve,
                pr: pr_snapshot(187, "luka/luc-623", "sha-a"),
                head_sha: GitSha("sha-a".to_string()),
                findings: Vec::new(),
                required_checks: Vec::new(),
            },
            crate::final_pr_gate::FinalReviewVerdict {
                reviewer: "tests".to_string(),
                status: test_status,
                pr: pr_snapshot(187, "luka/luc-623", "sha-a"),
                head_sha: GitSha("sha-a".to_string()),
                findings: Vec::new(),
                required_checks: Vec::new(),
            },
        ])];
        let FinalPrGateCompletionPlan::Ready(plan) = final_pr_gate_completion_plan(
            Utc::now(),
            FinalPrGateCompletionInput {
                config: config.final_review.clone(),
                start_request: start.clone(),
                current_pr: pr_snapshot(187, "luka/luc-623", "sha-a"),
                ci: Some(ci_output(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate-complete:187:sha-a".to_string()),
                extra_blockers: Vec::new(),
                events,
            },
        ) else {
            panic!("completion should be ready");
        };
        let mut executor = RecordingFinalGateProjectionExecutor::default();
        execute_final_pr_gate_projection_with_executor(
            &orchestrator
                .config
                .effective()
                .kapellmeister
                .shadow_mode
                .authority,
            &kapellmeister_context_for_issue(&issue, Some(repo_ref())),
            plan.projection.clone(),
            &mut executor,
        )
        .await
        .expect("guarded projection");
        (plan, executor)
    }

    #[tokio::test]
    async fn production_final_gate_completion_events_drive_rework_projection() {
        let (plan, executor) =
            project_completion_for_test(FinalReviewVerdictStatus::RequestChanges).await;
        assert_eq!(plan.result.decision, FinalPrGateDecision::Rework);
        assert_eq!(executor.workpads.len(), 1);
        assert_eq!(executor.moves.len(), 1);
        assert!(executor.merges.is_empty());
        assert_eq!(executor.operations, vec!["workpad", "move"]);
    }

    #[tokio::test]
    async fn production_final_gate_completion_events_drive_blocked_projection() {
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            }),
        );
        let (orchestrator, _native_host) = native_orchestrator_for_definition(definition);
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![
                crate::config::FinalReviewReviewerConfig {
                    id: "architecture".to_string(),
                    profile: "final_architecture_reviewer".to_string(),
                    required: true,
                },
                crate::config::FinalReviewReviewerConfig {
                    id: "tests".to_string(),
                    profile: "final_test_reviewer".to_string(),
                    required: true,
                },
            ],
        };
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Custom Final Gate".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "sha-a"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("manual lane should produce request");
        let events = vec![final_gate_completion_event(vec![
            crate::final_pr_gate::FinalReviewVerdict {
                reviewer: "architecture".to_string(),
                status: FinalReviewVerdictStatus::Approve,
                pr: pr_snapshot(187, "luka/luc-623", "sha-a"),
                head_sha: GitSha("sha-a".to_string()),
                findings: Vec::new(),
                required_checks: Vec::new(),
            },
        ])];
        let FinalPrGateCompletionPlan::Ready(plan) = final_pr_gate_completion_plan(
            Utc::now(),
            FinalPrGateCompletionInput {
                config: config.final_review.clone(),
                start_request: start,
                current_pr: pr_snapshot(187, "luka/luc-623", "sha-a"),
                ci: Some(ci_output(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate-complete:187:sha-a".to_string()),
                extra_blockers: Vec::new(),
                events,
            },
        ) else {
            panic!("completion should be ready");
        };
        let mut executor = RecordingFinalGateProjectionExecutor::default();
        execute_final_pr_gate_projection_with_executor(
            &orchestrator
                .config
                .effective()
                .kapellmeister
                .shadow_mode
                .authority,
            &kapellmeister_context_for_issue(&issue, Some(repo_ref())),
            plan.projection.clone(),
            &mut executor,
        )
        .await
        .expect("guarded projection");
        assert_eq!(plan.result.decision, FinalPrGateDecision::Blocked);
        assert_eq!(executor.workpads.len(), 1);
        assert_eq!(executor.moves.len(), 1);
        assert!(executor.merges.is_empty());
    }

    #[tokio::test]
    async fn production_final_gate_completion_events_drive_merge_projection() {
        let (plan, executor) = project_completion_for_test(FinalReviewVerdictStatus::Approve).await;
        assert_eq!(plan.result.decision, FinalPrGateDecision::Merge);
        assert_eq!(executor.workpads.len(), 1);
        assert_eq!(executor.moves.len(), 1);
        assert_eq!(executor.merges.len(), 1);
        assert_eq!(executor.operations, vec!["workpad", "merge", "move"]);
        assert_eq!(
            executor.merges[0].expected_head_sha,
            Some(GitSha("sha-a".to_string()))
        );
    }

    #[tokio::test]
    async fn final_gate_merge_failure_does_not_project_success_lane() {
        let (plan, mut executor) =
            project_completion_for_test(FinalReviewVerdictStatus::Approve).await;
        executor.moves.clear();
        executor.merges.clear();
        executor.workpads.clear();
        executor.operations.clear();
        executor.fail_merge = true;

        let issue = issue("issue-623", "LUC-623", Some(2), 10);
        let err = execute_final_pr_gate_projection_with_executor(
            &crate::kapellmeister::KapellmeisterToolAuthority {
                mode: crate::kapellmeister::KapellmeisterAuthorityMode::GuardedLabeled,
                read_only_tools: false,
                safe_selectors: crate::kapellmeister::KapellmeisterSafeSelectors {
                    repositories: vec![repo_ref()],
                    linear_labels: Vec::new(),
                },
            },
            &kapellmeister_context_for_issue(&issue, Some(repo_ref())),
            plan.projection,
            &mut executor,
        )
        .await
        .expect_err("merge failure blocks Done projection");

        assert!(err.to_string().contains("recorded"));
        assert_eq!(executor.operations, vec!["workpad", "merge"]);
        assert!(executor.moves.is_empty());
    }

    #[tokio::test]
    async fn production_final_pr_gate_api_handler_uses_guarded_plan_path() {
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            }),
        );
        let (mut orchestrator, native_host) = native_orchestrator_for_definition(definition);
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "architecture".to_string(),
                profile: "final_architecture_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut enabled_definition = orchestrator.config.definition().clone();
        enabled_definition.config.insert(
            "final_review".into(),
            serde_json::to_value(&config.final_review).unwrap(),
        );
        orchestrator.config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            enabled_definition,
        )
        .unwrap();
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "api-head"));
        let request = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should produce request");

        let result = orchestrator
            .handle_kapellmeister_start_final_pr_gate(request.clone())
            .await;

        assert!(result.accepted, "{}", result.message);
        assert_eq!(result.issue_identifier, "LUC-623");
        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 1);
        assert_eq!(
            native_host.spawned_inputs()[0].idempotency_key,
            request.spawn_input.idempotency_key
        );
    }

    #[tokio::test]
    async fn production_final_pr_gate_rejects_tool_error_result() {
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "kapellmeister".to_string(),
            json!({
                "authority_mode": "guarded_labeled",
                "safe_selectors": {
                    "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}]
                }
            }),
        );
        let (mut orchestrator, _native_host) = native_orchestrator_for_definition(definition);
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "architecture".to_string(),
                profile: "final_architecture_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut enabled_definition = orchestrator.config.definition().clone();
        enabled_definition.config.insert(
            "final_review".into(),
            serde_json::to_value(&config.final_review).unwrap(),
        );
        orchestrator.config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            enabled_definition,
        )
        .unwrap();
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "api-head"));
        let mut request = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should produce request");
        request.spawn_input.worktree_path = None;

        let result = orchestrator
            .handle_kapellmeister_start_final_pr_gate(request)
            .await;

        assert!(!result.accepted);
        assert!(result.message.contains("final PR gate start tool failed"));
        assert!(result.message.contains("worktree_path"));
    }

    #[tokio::test]
    async fn production_final_pr_gate_does_not_self_authorize_label_only_selector() {
        let (mut orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "architecture".to_string(),
                profile: "final_architecture_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut enabled_definition = orchestrator.config.definition().clone();
        enabled_definition.config.insert(
            "final_review".into(),
            serde_json::to_value(&config.final_review).unwrap(),
        );
        orchestrator.config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            enabled_definition,
        )
        .unwrap();
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "api-head"));
        let request = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should produce request");

        let result = orchestrator
            .handle_kapellmeister_start_final_pr_gate(request)
            .await;

        assert!(!result.accepted);
        assert!(result.message.contains("did not execute"));
        assert_eq!(native_host.spawn_call_count(), 0);
    }

    #[tokio::test]
    async fn native_pr_handoff_auto_final_gate_invokes_production_start_resume() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![
                crate::config::FinalReviewReviewerConfig {
                    id: "architecture".to_string(),
                    profile: "final_architecture_reviewer".to_string(),
                    required: true,
                },
                crate::config::FinalReviewReviewerConfig {
                    id: "security".to_string(),
                    profile: "final_security_reviewer".to_string(),
                    required: false,
                },
            ],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        issue.labels = vec!["kapellmeister-safe".to_string()];
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "new-head"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should start from review-ready typed PR handoff");

        assert!(
            orchestrator
                .start_native_final_pr_gate_from_kapellmeister_decision(&issue, &start)
                .await
                .expect("start final gate")
        );

        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 1);
        let inputs = native_host.spawned_inputs();
        assert_eq!(inputs.len(), 1);
        let input = &inputs[0];
        assert_eq!(input.issue, issue_ref_for_identifier("LUC-623"));
        assert_eq!(input.workspace_key, workspace_key_for_identifier("LUC-623"));
        assert_eq!(
            input.template_id,
            crate::tools::MobTemplateId("final-pr-gate-187".to_string())
        );
        assert_eq!(
            input.idempotency_key,
            IdempotencyKey("final-pr-gate:meerkat-fugue:187:new-head".to_string())
        );
        assert_eq!(
            input.worktree_path.as_deref(),
            Some("/tmp/fugue-test-workspaces/LUC-623")
        );
        let instructions = input.instructions.as_deref().unwrap();
        assert!(instructions.contains("via Auto"));
        assert!(instructions.contains("Review exact head new-head"));
        assert!(instructions.contains("success=Custom Done"));
        assert_eq!(start.reviewers.len(), 2);
        assert!(
            start
                .reviewers
                .iter()
                .all(|reviewer| !reviewer.merge_tool_allowed)
        );
    }

    #[tokio::test]
    async fn guarded_final_pr_gate_decision_uses_launch_plan_without_raw_spawn() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Auto,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "architecture".to_string(),
                profile: "final_architecture_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut issue = issue("issue-623", "LUC-623", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        issue.labels = vec!["kapellmeister-safe".to_string()];
        let github = recover_native_github_evidence(pr_snapshot(187, "luka/luc-623", "new-head"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("auto mode should start from review-ready typed PR handoff");
        let mut executor = OrchestratorFinalPrGateStartExecutor {
            native_execution_host: Arc::clone(&orchestrator.native_execution_host),
            config: orchestrator.config.effective().clone(),
            issue: issue.clone(),
        };
        let response = KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId("final-pr-gate-start:test:new-head".to_string()),
            decided_at: Utc::now(),
            decision: KapellmeisterHostDecision::FinalPrGateStart {
                request: start.clone(),
            },
            rationale: Some("test Kapellmeister-authored final gate start".to_string()),
        };

        execute_guarded_kapellmeister_decision(
            &orchestrator
                .config
                .effective()
                .kapellmeister
                .shadow_mode
                .authority,
            &kapellmeister_context_for_issue(&issue, Some(start.pr.repo.clone())),
            response,
            &mut executor,
        )
        .await
        .expect("guarded final gate decision executes");

        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 1);
        assert_eq!(
            native_host.spawned_inputs()[0].idempotency_key,
            start.spawn_input.idempotency_key
        );
    }

    #[tokio::test]
    async fn native_pr_handoff_manual_final_gate_lane_uses_same_start_resume_path() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let mut config = orchestrator.config.effective().clone();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Manual,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "tests".to_string(),
                profile: "final_test_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut issue = issue("issue-624", "LUC-624", Some(1), 10);
        issue.state = "Custom Final Gate".to_string();
        issue.labels = vec!["kapellmeister-safe".to_string()];
        let github =
            recover_native_github_evidence(pr_snapshot(188, "luka/luc-624", "manual-head"));
        let start = native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
            .expect("start request construction")
            .expect("manual mode should start only from final gate lane");
        assert_eq!(start.trigger_mode, crate::config::FinalReviewMode::Manual);

        orchestrator
            .start_native_final_pr_gate_from_kapellmeister_decision(&issue, &start)
            .await
            .expect("start manual final gate");

        assert_eq!(native_host.raw_spawn_call_count(), 0);
        assert_eq!(native_host.plan_call_count(), 1);
        let inputs = native_host.spawned_inputs();
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            inputs[0].template_id,
            crate::tools::MobTemplateId("final-pr-gate-188".to_string())
        );
        assert_eq!(
            inputs[0].worktree_path.as_deref(),
            Some("/tmp/fugue-test-workspaces/LUC-624")
        );
        assert!(
            inputs[0]
                .instructions
                .as_deref()
                .unwrap()
                .contains("via Manual")
        );
    }

    #[test]
    fn native_pr_handoff_monitor_reports_final_gate_observation_without_host_decision() {
        let source = include_str!("orchestrator.rs");
        let start = source
            .find("async fn reconcile_native_pr_handoffs")
            .expect("reconcile function");
        let end = source[start..]
            .find("async fn ensure_linear_projection_lanes")
            .expect("next function")
            + start;
        let body = &source[start..end];

        assert!(body.contains("deliver_native_final_pr_gate_request_to_kapellmeister"));
        assert!(!body.contains("KapellmeisterHostDecision::FinalPrGateStart"));
        assert!(!body.contains("start_native_final_pr_gate_from_kapellmeister_decision"));
        assert!(!body.contains("native_execution_host.spawn_or_resume_execution"));
    }

    #[test]
    fn native_kapellmeister_pr_attention_repair_delivers_final_gate_observation() {
        let source = include_str!("orchestrator.rs");
        let start = source
            .find("async fn reconcile_native_kapellmeister_pr_attention_handoffs")
            .expect("Kapellmeister PR attention repair function");
        let end = source[start..]
            .find("async fn running_mob_observation_for_issue")
            .expect("next orchestrator function")
            + start;
        let body = &source[start..end];

        assert!(body.contains("native_final_pr_gate_start_request_from_handoff"));
        assert!(body.contains("deliver_native_final_pr_gate_request_to_kapellmeister"));
        assert!(body.contains("final_gate_start.as_ref()"));
        assert!(!body.contains("KapellmeisterHostDecision::FinalPrGateStart"));
        assert!(!body.contains("native_execution_host.spawn_or_resume_execution"));
    }

    #[test]
    fn native_kapellmeister_pr_attention_monitor_does_not_skip_running_mobs() {
        let source = include_str!("orchestrator.rs");
        let start = source
            .find("async fn reconcile_native_kapellmeister_pr_attention_handoffs")
            .expect("Kapellmeister PR attention repair function");
        let end = source[start..]
            .find("async fn running_mob_observation_for_issue")
            .expect("next orchestrator function")
            + start;
        let body = &source[start..end];

        assert!(body.contains("claimed_without_running"));
        assert!(!body.contains("state.running.contains_key(&issue.id) || state.claimed"));
        assert!(body.contains("deliver_native_pr_handoff_monitor_signal_to_kapellmeister"));
    }

    #[test]
    fn repo_kapellmeister_signal_message_blocks_attention_lane_issue_start() {
        let body = repo_kapellmeister_signal_message(&[]).expect("signal prompt");

        assert!(body.contains("attention_or_terminal"));
        assert!(body.contains("do not start an issue mob"));
        assert!(body.contains("fugue_start_final_pr_gate"));
        assert!(body.contains("do not substitute fugue_start_issue_mob"));
        assert!(body.contains("fugue_start_independent_review"));
        assert!(body.contains("Fugue host handles Deploy as a guarded mechanical transaction"));
    }

    #[test]
    fn native_pr_handoff_manual_review_ready_does_not_start_without_final_gate_lane() {
        let mut config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        config.final_review = crate::config::FinalReviewConfig {
            enabled: true,
            mode: crate::config::FinalReviewMode::Manual,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: crate::config::FinalReviewMergeConfig::default(),
            reviewers: vec![crate::config::FinalReviewReviewerConfig {
                id: "tests".to_string(),
                profile: "final_test_reviewer".to_string(),
                required: true,
            }],
        };
        config.linear.lanes.review_ready = Some("Repo Ready".to_string());
        let mut issue = issue("issue-625", "LUC-625", Some(1), 10);
        issue.state = "Repo Ready".to_string();
        let github = recover_native_github_evidence(pr_snapshot(189, "luka/luc-625", "ready-head"));

        assert!(
            native_final_pr_gate_start_request_from_handoff(&config, &issue, &github)
                .expect("start request construction")
                .is_none()
        );
    }

    #[test]
    fn kapellmeister_projection_repair_allows_only_pr_attention_move() {
        assert!(kapellmeister_host_projection_repair_allowed(
            "native-kapellmeister-pr-handoff-attention"
        ));
        assert!(!kapellmeister_host_projection_repair_allowed(
            "native-compact-lane-repair"
        ));
        assert!(!kapellmeister_host_projection_repair_allowed(
            "native-pr-handoff-monitor-replan"
        ));
    }

    #[test]
    fn native_pr_handoff_monitor_workpad_section_is_idempotent_and_marks_rework() {
        let github = recover_native_github_evidence(pr_snapshot(118, "fugue/luc-401", "abc123"));
        let body =
            "## Fugue Workpad\n\n### Pull request or Handoff\n- [x] Repo boundary PR handoff.";
        let action = NativePrHandoffMonitorAction::Replan(NativePrHandoffReplanReason::CiFailed);

        let once = append_native_pr_handoff_monitor_evidence(
            body,
            &github,
            Some(&ci_output(CiStatus::Failure)),
            &action,
            None,
        );
        let twice = append_native_pr_handoff_monitor_evidence(
            &once,
            &github,
            Some(&ci_output(CiStatus::Failure)),
            &action,
            None,
        );

        assert_eq!(once, twice);
        assert!(once.contains("### PR Review Monitor"));
        assert!(once.contains("Reviewer decision: red / return to Active focused remediation"));
        assert!(once.contains(
            "Reviewer findings requiring Active remediation with WorkGraph/Workpad/phase evidence"
        ));
        assert!(workpad_requests_reviewer_rework(&once));
    }

    #[test]
    fn native_host_blocked_workpad_section_is_idempotent_and_calls_for_human_intervention() {
        let body = "## Fugue Workpad\n\n### Current stage\n- [x] Implementing.";
        let detail = "native mob launch failed: allowed roots only include /tmp";

        let once = append_native_host_blocked_evidence(body, detail);
        let twice = append_native_host_blocked_evidence(&once, detail);

        assert_eq!(once, twice);
        assert!(once.contains("### Fugue Host Blocked"));
        assert!(once.contains("could not launch a viable native mob"));
        assert!(once.contains("Human intervention is required"));
        assert!(once.contains("/tmp"));
    }

    #[test]
    fn native_pr_handoff_blocks_fresh_dispatch_from_active_projection_lane() {
        let config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        let mut issue = issue("issue-401", "LUC-401", Some(2), 10);
        issue.state = "Reviewing".to_string();
        let linear = recover_native_linear_evidence(
            &issue,
            Some("## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #118 opened."),
        );

        assert!(native_pr_handoff_blocks_fresh_dispatch_for_evidence(
            &config, &issue, &linear, false
        ));
    }

    #[test]
    fn native_pr_handoff_missing_evidence_ignores_branch_without_pr_handoff_claim() {
        let config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        let mut issue = issue("issue-519", "LUC-519", Some(2), 10);
        issue.state = "Reviewing".to_string();
        issue.branch_name = Some("luka/luc-519-abstract-away-linear-from-core-fugue".to_string());
        let linear = recover_native_linear_evidence(&issue, Some("## Fugue Workpad"));

        assert!(!native_pr_handoff_missing_durable_pr_evidence(
            &config, &issue, &linear
        ));

        let linear = recover_native_linear_evidence(
            &issue,
            Some("## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #118 opened."),
        );
        assert!(native_pr_handoff_missing_durable_pr_evidence(
            &config, &issue, &linear
        ));
    }

    #[test]
    fn native_attention_lanes_prefer_configured_review_ready_and_blocked() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  lanes:
    ready: Todo
    review_ready: Ready for review
    blocked: Blocked
    success: Done
    canceled: Canceled
    duplicate: Duplicate
linear_projections:
  standard_projection:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: standard_projection
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let issue = issue("issue-401", "LUC-401", Some(2), 10);

        assert_eq!(
            native_pr_handoff_waiting_lane(&config, &issue).as_deref(),
            Some("Ready for review")
        );
        assert_eq!(native_attention_lane(&config), "Ready for review");
        assert_eq!(native_liveness_blocked_lane(&config), "Blocked");
    }

    #[test]
    fn native_attention_lane_falls_back_to_blocked_then_parked_not_active_reviewing() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  lanes:
    parked: Backlog
    ready: Todo
    blocked: Blocked
    success: Done
    canceled: Canceled
    duplicate: Duplicate
linear_projections:
  standard_projection:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: standard_projection
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(native_attention_lane(&config), "Blocked");

        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  lanes:
    parked: Backlog
    ready: Todo
    success: Done
    canceled: Canceled
    duplicate: Duplicate
linear_projections:
  standard_projection:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: standard_projection
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(native_attention_lane(&config), "Backlog");
    }

    #[test]
    fn native_pr_handoff_durable_github_branch_does_not_block_replan_lane() {
        let config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        let mut issue = issue("issue-401", "LUC-401", Some(2), 10);
        issue.state = "Planning".to_string();
        let linear =
            recover_native_linear_evidence(&issue, Some("## Fugue Workpad\n\nNo PR here."));

        assert!(!native_pr_handoff_blocks_fresh_dispatch_for_evidence(
            &config, &issue, &linear, true
        ));
    }

    #[test]
    fn native_pr_handoff_dispatch_guard_does_not_block_todo_or_rework() {
        let config = ServiceConfig::from_definition(
            PathBuf::from("/tmp/fugue.yaml"),
            &native_workflow_definition(),
        )
        .unwrap();
        let mut issue = issue("issue-401", "LUC-401", Some(2), 10);
        let todo_linear = recover_native_linear_evidence(
            &issue,
            Some("## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #118 opened."),
        );

        assert!(!native_pr_handoff_blocks_fresh_dispatch_for_evidence(
            &config,
            &issue,
            &todo_linear,
            true
        ));

        issue.state = "Planning".to_string();
        let rework_linear = recover_native_linear_evidence(
            &issue,
            Some(
                "## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #118 opened.\n\n#### Reviewer gate\n- [x] Reviewer decision: red / return to Planning.\n\n#### Reviewer findings requiring focused rework\n- [ ] Fix the failing console smoke.",
            ),
        );

        assert!(!native_pr_handoff_blocks_fresh_dispatch_for_evidence(
            &config,
            &issue,
            &rework_linear,
            true
        ));
    }

    #[test]
    fn native_pr_handoff_branch_prefers_current_workspace_branch() {
        let mut issue = active_issue_with_labels(&["native"]);
        issue.branch_name = Some("poisoned/historical-branch".to_string());
        let inspection = crate::workspace::WorkspaceInspection {
            path: PathBuf::from("/tmp/fugue-test-workspaces/LUC-168"),
            workspace_key: "LUC-168".to_string(),
            current_branch: Some("luka/luc-306-focused".to_string()),
            head_sha: Some("abc123".to_string()),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: true,
        };

        let branch = native_pr_handoff_branch(&issue, &inspection);

        assert_eq!(branch.0, "luka/luc-306-focused");
    }

    #[test]
    fn native_pr_handoff_runtime_gitignore_error_parks_for_operator() {
        let error = "process_exit: git [\"add\", \"-A\", \":!.rkat\"] failed: stdout= stderr=The following paths are ignored by one of your .gitignore files: .rkat";

        assert!(native_pr_handoff_error_requires_operator_park(error));
    }

    #[test]
    fn git_add_workspace_changes_excludes_runtime_dirs() {
        let temp = tempfile::tempdir().unwrap();
        git_run(temp.path(), &["init"]).unwrap();
        std::fs::write(temp.path().join(".gitignore"), ".rkat/\n").unwrap();
        std::fs::create_dir(temp.path().join(".rkat")).unwrap();
        std::fs::create_dir(temp.path().join(".fugue")).unwrap();
        std::fs::write(temp.path().join(".rkat").join("runtime.json"), "{}").unwrap();
        std::fs::write(temp.path().join(".fugue").join("runtime.json"), "{}").unwrap();
        std::fs::write(temp.path().join("meaningful.txt"), "real change\n").unwrap();

        git_add_workspace_changes_excluding_runtime(temp.path()).unwrap();
        let staged = git_output(temp.path(), &["diff", "--cached", "--name-only"]).unwrap();
        let status = git_status_excluding_rkat(temp.path()).unwrap();

        assert!(staged.contains("meaningful.txt"));
        assert!(!staged.contains(".rkat"));
        assert!(!staged.contains(".fugue"));
        assert!(status.contains("meaningful.txt"));
        assert!(!status.contains(".rkat"));
        assert!(!status.contains(".fugue"));
    }

    #[test]
    fn stale_or_wrong_head_pr_is_not_accepted_as_handoff_evidence() {
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Reviewing".to_string();
        issue.branch_name = Some("luka/luc-306-focused".to_string());
        let linear = recover_native_linear_evidence(
            &issue,
            Some("- [x] Pull request: https://github.com/lukacf/meerkat-fugue/pull/73"),
        );
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
            branch: Some(BranchName("luka/luc-306-focused".to_string())),
            head_sha: Some(GitSha("current-head".to_string())),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };
        let rejected = focused_pr_handoff_decision(
            vec![pr_snapshot(73, "historical/wrong-branch", "stale-head")],
            workspace.branch.as_ref(),
            workspace.head_sha.as_ref(),
        );

        assert!(rejected.is_err());
        let recovery = native_status_recovery(
            &run,
            Some(linear),
            Some(RecoveredGitHubEvidence {
                pull_request: pr_snapshot(73, "historical/wrong-branch", "stale-head"),
                rejected_pull_requests: vec![RejectedPullRequestEvidence {
                    number: GitHubPullRequestNumber(73),
                    reason: "head branch `historical/wrong-branch` does not match current issue/workspace branch `luka/luc-306-focused`".to_string(),
                }],
            }),
            Some(workspace),
            &[],
            Some(2),
        );

        let reason = recovery.review.unwrap().reason;
        assert!(
            reason.contains("rejected PR #73"),
            "unexpected reason: {reason}"
        );
        assert!(
            reason.contains("wrong-branch"),
            "unexpected reason: {reason}"
        );
    }

    #[test]
    fn multiple_prs_select_current_workspace_branch_and_record_rejections() {
        let expected_branch = BranchName("luka/luc-306-focused".to_string());
        let expected_sha = GitSha("focused-head".to_string());

        let evidence = focused_pr_handoff_decision(
            vec![
                pr_snapshot(73, "historical/wrong-branch", "old-head"),
                pr_snapshot(72, "luka/luc-306-focused", "focused-head"),
            ],
            Some(&expected_branch),
            Some(&expected_sha),
        )
        .expect("focused branch PR should be accepted");

        assert_eq!(evidence.pull_request.number, GitHubPullRequestNumber(72));
        assert_eq!(evidence.rejected_pull_requests.len(), 1);
        assert_eq!(
            evidence.rejected_pull_requests[0].number,
            GitHubPullRequestNumber(73)
        );
        assert!(evidence.rejected_pull_requests[0].reason.contains("branch"));
    }

    #[test]
    fn draft_pr_can_satisfy_native_handoff_for_reviewer_gate() {
        let expected_branch = BranchName("fugue/luc-401".to_string());
        let expected_sha = GitSha("b6fbecbb800fd3eee922a228ae5b8aae2e9764e2".to_string());
        let mut pull_request = pr_snapshot(
            118,
            "fugue/luc-401",
            "b6fbecbb800fd3eee922a228ae5b8aae2e9764e2",
        );
        pull_request.draft = true;

        let evidence = focused_pr_handoff_decision(
            vec![pull_request],
            Some(&expected_branch),
            Some(&expected_sha),
        )
        .expect("draft PRs are valid handoff evidence before the reviewer gate");

        assert_eq!(evidence.pull_request.number, GitHubPullRequestNumber(118));
        assert!(evidence.pull_request.draft);
        assert!(evidence.rejected_pull_requests.is_empty());
    }

    #[test]
    fn durable_github_recovery_prefers_workspace_branch_over_linear_generated_branch() {
        struct RecordingInspector {
            seen: std::sync::Mutex<Vec<GitHubPullRequestRef>>,
            result: GitHubPullRequestSnapshot,
        }

        impl NativeStatusGitHubInspector for RecordingInspector {
            fn inspect_pr(&self, input: crate::tools::InspectPrInput) -> FugueToolResult {
                self.seen.lock().unwrap().push(input.pull_request);
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubInspectPr(crate::tools::InspectPrOutput {
                        pull_request: self.result.clone(),
                    }),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }

        let linear = RecoveredLinearEvidence {
            issue_state: "Implementing".to_string(),
            branch_name: Some(BranchName("luka/luc-337-implement-ota-updates".to_string())),
            workpad_native_run_id: None,
            workpad_mentions_pr: false,
            workpad_pr_number: None,
            unresolved_review_finding: None,
            reviewer_rework_requested: false,
        };
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-337".to_string(),
            branch: Some(BranchName("codex/LUC-337-ota-updates".to_string())),
            head_sha: Some(GitSha("current-head".to_string())),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };
        let inspector = RecordingInspector {
            seen: std::sync::Mutex::new(Vec::new()),
            result: pr_snapshot(95, "codex/LUC-337-ota-updates", "current-head"),
        };

        let evidence = recover_native_github_evidence_from_durable_sources(
            &repo_ref(),
            Some(&linear),
            Some(&workspace),
            &[],
            &inspector,
        )
        .expect("workspace branch PR should be accepted");

        assert_eq!(evidence.pull_request.number, GitHubPullRequestNumber(95));
        assert_eq!(
            inspector.seen.lock().unwrap().as_slice(),
            &[GitHubPullRequestRef::Branch {
                head_branch: BranchName("codex/LUC-337-ota-updates".to_string())
            }]
        );
    }

    #[test]
    fn durable_github_recovery_prefers_workspace_branch_over_workpad_pr_number() {
        struct RecordingInspector {
            seen: std::sync::Mutex<Vec<GitHubPullRequestRef>>,
            result: GitHubPullRequestSnapshot,
        }

        impl NativeStatusGitHubInspector for RecordingInspector {
            fn inspect_pr(&self, input: crate::tools::InspectPrInput) -> FugueToolResult {
                self.seen.lock().unwrap().push(input.pull_request);
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubInspectPr(crate::tools::InspectPrOutput {
                        pull_request: self.result.clone(),
                    }),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }

        let linear = RecoveredLinearEvidence {
            issue_state: "Reviewing".to_string(),
            branch_name: None,
            workpad_native_run_id: None,
            workpad_mentions_pr: true,
            workpad_pr_number: Some(GitHubPullRequestNumber(619)),
            unresolved_review_finding: None,
            reviewer_rework_requested: false,
        };
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-619".to_string(),
            branch: Some(BranchName(
                "luka/luc-619-find-3-bugs-and-fix-them".to_string(),
            )),
            head_sha: Some(GitSha(
                "ee6c54fc146ad4e847147edf70c65a5e61c31cb7".to_string(),
            )),
            upstream_branch: Some(BranchName(
                "origin/luka/luc-619-find-3-bugs-and-fix-them".to_string(),
            )),
            upstream_head_sha: Some(GitSha(
                "ee6c54fc146ad4e847147edf70c65a5e61c31cb7".to_string(),
            )),
            dirty: false,
        };
        let inspector = RecordingInspector {
            seen: std::sync::Mutex::new(Vec::new()),
            result: pr_snapshot(
                176,
                "luka/luc-619-find-3-bugs-and-fix-them",
                "ee6c54fc146ad4e847147edf70c65a5e61c31cb7",
            ),
        };

        let evidence = recover_native_github_evidence_from_durable_sources(
            &repo_ref(),
            Some(&linear),
            Some(&workspace),
            &[],
            &inspector,
        )
        .expect("workspace branch PR should be accepted before stale Workpad PR numbers");

        assert_eq!(evidence.pull_request.number, GitHubPullRequestNumber(176));
        assert_eq!(
            inspector.seen.lock().unwrap().as_slice(),
            &[GitHubPullRequestRef::Branch {
                head_branch: BranchName("luka/luc-619-find-3-bugs-and-fix-them".to_string())
            }]
        );
    }

    #[test]
    fn durable_github_recovery_ignores_linear_generated_branch_without_handoff_evidence() {
        struct RecordingInspector {
            seen: std::sync::Mutex<Vec<GitHubPullRequestRef>>,
        }

        impl NativeStatusGitHubInspector for RecordingInspector {
            fn inspect_pr(&self, input: crate::tools::InspectPrInput) -> FugueToolResult {
                self.seen.lock().unwrap().push(input.pull_request);
                FugueToolResult::Err {
                    error: FugueToolError {
                        operation: FugueToolOperation::GitHubInspectPr,
                        code: crate::tools::FugueToolErrorCode::InvalidInput,
                        message: "unexpected PR lookup".to_string(),
                        retry: crate::tools::RetryAdvice::DoNotRetry,
                        idempotency_key: Some(IdempotencyKey("test-no-pr-lookup".to_string())),
                    },
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }

        let linear = RecoveredLinearEvidence {
            issue_state: "Planning".to_string(),
            branch_name: Some(BranchName(
                "luka/luc-526-fugue-smoke-kapellmeister-only-todo-observation".to_string(),
            )),
            workpad_native_run_id: None,
            workpad_mentions_pr: false,
            workpad_pr_number: None,
            unresolved_review_finding: None,
            reviewer_rework_requested: false,
        };
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-526".to_string(),
            branch: Some(BranchName("main".to_string())),
            head_sha: Some(GitSha("local-main-head".to_string())),
            upstream_branch: Some(BranchName("origin/main".to_string())),
            upstream_head_sha: Some(GitSha("origin-main-head".to_string())),
            dirty: false,
        };
        let inspector = RecordingInspector {
            seen: std::sync::Mutex::new(Vec::new()),
        };

        let evidence = recover_native_github_evidence_from_durable_sources(
            &repo_ref(),
            Some(&linear),
            Some(&workspace),
            &[],
            &inspector,
        );

        assert!(evidence.is_none());
        assert!(inspector.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn native_status_review_accepts_workspace_branch_over_linear_generated_branch() {
        let run_id = MobRunId("native-execution:native-execution:LUC-338".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-338")),
            workspace_key: Some(workspace_key_for_identifier("LUC-338")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.identifier = "LUC-338".to_string();
        issue.state = "Reviewing".to_string();
        issue.branch_name = Some("luka/luc-338-integrateserve-meta-console".to_string());
        let linear = recover_native_linear_evidence(
            &issue,
            Some("## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #98 opened."),
        );
        let workspace = RecoveredWorkspaceEvidence {
            path: "/tmp/fugue-test-workspaces/LUC-338".to_string(),
            branch: Some(BranchName("codex/LUC-338-meta-console".to_string())),
            head_sha: Some(GitSha(
                "6c188ca12d437b0b3df83ff6a8dd0ede59c5a5c8".to_string(),
            )),
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        };
        let github = recover_native_github_evidence(pr_snapshot(
            98,
            "codex/LUC-338-meta-console",
            "6c188ca12d437b0b3df83ff6a8dd0ede59c5a5c8",
        ));

        let recovery = native_status_recovery(
            &run,
            Some(linear),
            Some(github),
            Some(workspace),
            &[],
            Some(2),
        );
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: Vec::new(),
            recovery,
        });

        assert_eq!(
            row.github_pr_url.as_deref(),
            Some("https://github.com/lukacf/meerkat-fugue/pull/98")
        );
        assert!(row.review_required.is_none());
    }

    #[test]
    fn native_status_heartbeat_refreshes_stale_existing_github_evidence() {
        struct Inspector {
            result: GitHubPullRequestSnapshot,
        }

        impl NativeStatusGitHubInspector for Inspector {
            fn inspect_pr(&self, _input: crate::tools::InspectPrInput) -> FugueToolResult {
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubInspectPr(crate::tools::InspectPrOutput {
                        pull_request: self.result.clone(),
                    }),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let workspace_root = temp.path().join("workspaces");
        let workspace_path = workspace_root.join("LUC-338");
        std::fs::create_dir_all(&workspace_path).unwrap();
        run_git(&workspace_path, &["init"]);
        run_git(
            &workspace_path,
            &["config", "user.email", "fugue@example.invalid"],
        );
        run_git(&workspace_path, &["config", "user.name", "Fugue Test"]);
        std::fs::write(workspace_path.join("README.md"), "hi\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "initial"]);
        run_git(
            &workspace_path,
            &["checkout", "-b", "codex/LUC-338-meta-console"],
        );
        let head_sha = git_head_sha(&workspace_path);

        let mut definition = native_workflow_definition();
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": workspace_root.display().to_string() }),
        );
        let config = WorkflowConfigManager::from_definition_for_test(
            temp.path().join("fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let workspace_manager = WorkspaceManager::for_service(&config);
        let mut issue = active_issue_with_labels(&["native"]);
        issue.identifier = "LUC-338".to_string();
        issue.state = "Reviewing".to_string();
        issue.branch_name = Some("luka/luc-338-integrateserve-meta-console".to_string());
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-338".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-338")),
            workspace_key: Some(workspace_key_for_identifier("LUC-338")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let existing = NativeStatusRecovery {
            github: Some(recover_native_github_evidence(pr_snapshot(
                98,
                "codex/LUC-338-meta-console",
                "stale-head",
            ))),
            ..NativeStatusRecovery::default()
        };
        let inspector = Inspector {
            result: pr_snapshot(98, "codex/LUC-338-meta-console", &head_sha),
        };

        let refreshed = refresh_native_status_recovery(
            &run,
            &issue,
            &config,
            &workspace_manager,
            &[],
            &existing,
            Some("## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #98 opened."),
            Some(&inspector),
        );

        assert_eq!(
            refreshed
                .github
                .as_ref()
                .map(|github| github.pull_request.head_sha.0.as_str()),
            Some(head_sha.as_str())
        );
        assert!(refreshed.review.is_none());
    }

    #[test]
    fn native_launch_recovery_hydrates_workpad_pr_evidence_before_event_snapshot() {
        struct Inspector {
            result: GitHubPullRequestSnapshot,
        }

        impl NativeStatusGitHubInspector for Inspector {
            fn inspect_pr(&self, _input: crate::tools::InspectPrInput) -> FugueToolResult {
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubInspectPr(crate::tools::InspectPrOutput {
                        pull_request: self.result.clone(),
                    }),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": temp.path().join("workspaces").display().to_string() }),
        );
        let config = WorkflowConfigManager::from_definition_for_test(
            temp.path().join("fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let workspace_manager = WorkspaceManager::for_service(&config);
        let mut issue = active_issue_with_labels(&["native"]);
        issue.identifier = "LUC-620".to_string();
        issue.state = "Reviewing".to_string();
        issue.branch_name = Some("luka/luc-620-refactor-srcorchestratorrs".to_string());
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:LUC-620".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-620")),
            workspace_key: Some(workspace_key_for_identifier("LUC-620")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let existing = NativeStatusRecovery {
            workspace: Some(RecoveredWorkspaceEvidence {
                path: temp.path().join("workspaces/LUC-620").display().to_string(),
                branch: issue.branch_name.clone().map(BranchName),
                head_sha: None,
                upstream_branch: None,
                upstream_head_sha: None,
                dirty: false,
            }),
            ..NativeStatusRecovery::default()
        };
        let inspector = Inspector {
            result: pr_snapshot(
                180,
                "luka/luc-620-refactor-srcorchestratorrs",
                "357d93967dc70a486582a424282261f91f1345a7",
            ),
        };

        let refreshed = refresh_native_status_recovery(
            &run,
            &issue,
            &config,
            &workspace_manager,
            &[],
            &existing,
            Some(
                "## Fugue Workpad\n\n### Pull request or Handoff\n- [x] PR #180 opened and CI/review evidence is green.",
            ),
            Some(&inspector),
        );

        assert_eq!(
            refreshed
                .linear
                .as_ref()
                .and_then(|linear| linear.workpad_pr_number),
            Some(GitHubPullRequestNumber(180))
        );
        assert_eq!(
            refreshed
                .github
                .as_ref()
                .map(|github| github.pull_request.number),
            Some(GitHubPullRequestNumber(180))
        );
        assert!(refreshed.review.is_none());
    }

    #[test]
    fn compact_lane_red_review_accepts_only_planning_replan() {
        let mut definition = native_workflow_definition();
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "linear",
                "api_key": "token",
                "project_slug": "project",
                "intake_states": ["Todo"],
                "active_states": ["Todo", "Planning", "Implementing", "Reviewing"],
                "terminal_states": ["Done", "Canceled"]
            }),
        );
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Reviewing".to_string();
        let workpad = "## Fugue Workpad\n\n#### Reviewer gate\n- [x] Reviewer decision: red / return to Planning.\n\n#### Reviewer findings requiring focused rework\n- [ ] Source: focused finding.\n";
        let native_run = NativeRunHandle {
            run_id: run_id.clone(),
            observed_events: Vec::new(),
            recovery: native_status_recovery(
                &MobRunSnapshot {
                    run_id,
                    meta_flow_run_id: None,
                    flow_id: None,
                    issue: Some(issue_ref_for_identifier("LUC-168")),
                    workspace_key: Some(workspace_key_for_identifier("LUC-168")),
                    template_id: None,
                    status: MobRunStatus::Succeeded,
                    started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
                    updated_at: Utc.timestamp_opt(20, 0).unwrap(),
                    completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
                },
                Some(recover_native_linear_evidence(&issue, Some(workpad))),
                None,
                None,
                &[],
                Some(2),
            ),
        };

        assert!(native_run_requests_replan(&native_run));
        assert!(config.tracker.is_active_state("Planning"));
        for noncanonical_lane in ["Rework", "Human Review", "In Review", "Merging"] {
            assert!(
                !config.tracker.is_active_state(noncanonical_lane),
                "noncanonical lane {noncanonical_lane} must not be part of compact red-review routing"
            );
        }
    }

    #[test]
    fn structured_reviewer_red_gate_requests_replan_without_workpad_magic() {
        let native_run = NativeRunHandle {
            run_id: MobRunId("native-execution:native-execution:LUC-401".to_string()),
            observed_events: vec![native_event(
                1,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: Some(json!({
                        "steps": {
                            "reviewer": {
                                "schema_version": crate::native_reviewer_gate::REVIEWER_GATE_SCHEMA_VERSION,
                                "status": "red",
                                "next_action": "replan",
                                "summary": "Local implementation is missing the console integration test.",
                                "findings": [{
                                    "severity": "high",
                                    "title": "Missing console smoke",
                                    "detail": "Add a focused production-style console smoke before PR handoff."
                                }]
                            }
                        }
                    })),
                },
                None,
            )],
            recovery: NativeStatusRecovery::default(),
        };

        assert!(native_run_requests_replan(&native_run));
    }

    #[test]
    fn workpad_reviewer_red_overrides_invalid_structured_gate_for_replan() {
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Succeeded,
                structured_output: Some(json!({
                    "steps": {
                        "reviewer": {
                            "schema_version": crate::native_reviewer_gate::REVIEWER_GATE_SCHEMA_VERSION,
                            "status": "green",
                            "summary": "missing next_action, so this gate is invalid",
                            "findings": []
                        }
                    }
                })),
            },
            Some("flow completed with malformed reviewer gate"),
        )];
        let reviewer_gate = latest_reviewer_gate_observation(&events);

        assert!(reviewer_gate.is_invalid());
        assert_eq!(
            select_native_run_terminal_action(
                MobRunStatus::Succeeded,
                None,
                &events,
                &reviewer_gate,
                true,
                true,
                false,
            ),
            NativeRunTerminalAction::Replan
        );
    }

    #[test]
    fn structured_reviewer_green_gate_overrides_red_workpad_words() {
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Reviewing".to_string();
        let red_workpad = "## Fugue Workpad\n\nReviewer decision: red / return to Planning.";
        let native_run = NativeRunHandle {
            run_id: MobRunId("native-execution:native-execution:LUC-401".to_string()),
            observed_events: vec![native_event(
                1,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: Some(json!({
                        "steps": {
                            "reviewer": {
                                "schema_version": crate::native_reviewer_gate::REVIEWER_GATE_SCHEMA_VERSION,
                                "status": "green",
                                "next_action": "pr_handoff",
                                "summary": "Local reviewer gate is green.",
                                "findings": []
                            }
                        }
                    })),
                },
                None,
            )],
            recovery: NativeStatusRecovery {
                linear: Some(recover_native_linear_evidence(&issue, Some(red_workpad))),
                ..NativeStatusRecovery::default()
            },
        };

        assert!(!native_run_requests_replan(&native_run));
    }

    #[test]
    fn native_compact_lane_repair_uses_projected_mob_state_not_noncanonical_lane_names() {
        let definition = native_workflow_definition();
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Progress".to_string();
        let native_run = NativeRunHandle {
            run_id: MobRunId("native-execution:native-execution:LUC-264".to_string()),
            observed_events: vec![MobEventSnapshot {
                sequence: 1,
                occurred_at: Utc::now(),
                kind: crate::tools::MobEventKind::MemberStarted {
                    member: "coder".to_string(),
                },
                message: None,
            }],
            recovery: NativeStatusRecovery::default(),
        };

        assert_eq!(
            native_compact_lane_repair_target(&config, &issue, &native_run, MobRunStatus::Running)
                .as_deref(),
            Some("Implementing")
        );
        issue.state = "Implementing".to_string();
        assert_eq!(
            native_compact_lane_repair_target(&config, &issue, &native_run, MobRunStatus::Running),
            None
        );
    }

    #[test]
    fn observation_projection_does_not_move_terminal_cancelled_run_to_canceled_lane() {
        let definition = native_workflow_definition();
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let issue = active_issue_with_labels(&["native"]);
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::RunCompleted {
                status: MobRunStatus::Cancelled,
                structured_output: None,
            },
            Some("Meta flow was cancelled during service restart."),
        )];

        assert_eq!(
            projected_native_lane_for_observation(
                &config,
                &issue,
                MobRunStatus::Cancelled,
                &events,
            ),
            None
        );
    }

    #[test]
    fn observation_projection_still_maps_active_flow_nodes() {
        let definition = native_workflow_definition();
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap()
        .effective()
        .clone();
        let issue = active_issue_with_labels(&["native"]);
        let events = vec![native_event(
            1,
            crate::tools::MobEventKind::MemberStarted {
                member: "reviewer".to_string(),
            },
            None,
        )];

        assert_eq!(
            projected_native_lane_for_observation(&config, &issue, MobRunStatus::Running, &events)
                .as_deref(),
            Some("Reviewing")
        );
    }

    #[test]
    fn linear_projection_preflight_reports_exact_missing_states() {
        let required = vec![
            LinearRequiredState {
                name: "Backlog".to_string(),
                state_type: LinearStateType::Backlog,
            },
            LinearRequiredState {
                name: "Todo".to_string(),
                state_type: LinearStateType::Unstarted,
            },
            LinearRequiredState {
                name: "Reviewing".to_string(),
                state_type: LinearStateType::Started,
            },
            LinearRequiredState {
                name: "Done".to_string(),
                state_type: LinearStateType::Completed,
            },
        ];
        let existing = vec![
            LinearStateSnapshot {
                id: LinearStateId("state-backlog".to_string()),
                name: "backlog".to_string(),
                state_type: LinearStateType::Backlog,
            },
            LinearStateSnapshot {
                id: LinearStateId("state-done".to_string()),
                name: "Done".to_string(),
                state_type: LinearStateType::Completed,
            },
        ];

        assert_eq!(
            crate::linear_adapter::missing_required_states(&required, &existing),
            vec!["Todo (Unstarted)", "Reviewing (Started)"]
        );
    }

    #[test]
    fn terminal_recovery_refresh_detects_reviewer_red_workpad_before_action() {
        let run_id = MobRunId("native-execution:native-execution:LUC-264".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-264")),
            workspace_key: Some(workspace_key_for_identifier("LUC-264")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Reviewing".to_string();
        let mut native_run = NativeRunHandle {
            run_id,
            observed_events: Vec::new(),
            recovery: native_status_recovery(
                &run,
                Some(recover_native_linear_evidence(&issue, None)),
                None,
                None,
                &[],
                Some(3),
            ),
        };
        assert!(!native_run_requests_replan(&native_run));

        let workpad = "## Fugue Workpad\n\n#### Reviewer gate\n- [x] Reviewer decision: **red / return to Planning**.\n\n#### Reviewer findings requiring focused rework\n- [ ] Rework: add terminal Workpad refresh before incomplete-E2E fallback.\n";
        refresh_native_run_linear_recovery_from_workpad(&mut native_run, &issue, Some(workpad));

        assert!(native_run_requests_replan(&native_run));
    }

    #[test]
    fn native_rework_attempts_use_fresh_idempotency_keys() {
        assert_eq!(
            native_execution_idempotency_key("LUC-264", None).0,
            "native-execution:LUC-264"
        );
        assert_eq!(
            native_execution_idempotency_key("LUC-264", Some(0)).0,
            "native-execution:LUC-264"
        );
        assert_eq!(
            native_execution_idempotency_key("LUC-264", Some(1)).0,
            "native-execution:LUC-264:attempt-1"
        );
    }

    #[test]
    fn active_native_lane_dispatch_requires_explicit_retry_generation() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Progress".to_string();

        assert_eq!(
            native_run_generation_for_dispatch(&issue, &config, None),
            None
        );
    }

    #[test]
    fn active_native_lane_dispatch_uses_retry_attempt_as_generation() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Implementing".to_string();

        assert_eq!(
            native_run_generation_for_dispatch(&issue, &config, Some(4)),
            Some(4)
        );
    }

    #[test]
    fn native_terminal_attempt_advances_from_launched_generation() {
        let run_id = MobRunId("native-execution:native-execution:LUC-277:attempt-1".to_string());
        let entry = RunningEntry {
            issue: active_issue_with_labels(&["native"]),
            identifier: "LUC-277".to_string(),
            policy_mode: IssuePolicyMode::Kapellmeister,
            workspace_path: "/tmp/fugue-test-workspaces/LUC-277".to_string(),
            session: LiveSession::default(),
            started_at: Utc::now(),
            retry_attempt: None,
            cancel: CancellationToken::new(),
            handle: None,
            termination: None,
            native_run: Some(NativeRunHandle {
                run_id,
                observed_events: Vec::new(),
                recovery: NativeStatusRecovery::default(),
            }),
        };

        assert_eq!(next_native_terminal_attempt(&entry), 2);
    }

    #[test]
    fn restart_recovery_prefers_newest_native_run_generation_per_issue() {
        let issue = issue_ref_for_identifier("LUC-277");
        let older = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-277:attempt-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue.clone()),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let newer = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-277:attempt-3".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(21, 0).unwrap(),
            completed_at: None,
        };

        let recovered = newest_native_runs_by_issue(vec![older, newer]);

        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].run_id.0,
            "native-execution:native-execution:LUC-277:attempt-3"
        );
    }

    #[test]
    fn active_dispatch_generation_advances_past_durable_run_history() {
        let issue = issue_ref_for_identifier("LUC-277");
        let runs = vec![
            MobRunSnapshot {
                run_id: MobRunId("native-execution:native-execution:LUC-277:attempt-1".to_string()),
                meta_flow_run_id: None,
                flow_id: None,
                issue: Some(issue.clone()),
                workspace_key: None,
                template_id: None,
                status: MobRunStatus::Succeeded,
                started_at: None,
                updated_at: Utc.timestamp_opt(20, 0).unwrap(),
                completed_at: Some(Utc.timestamp_opt(21, 0).unwrap()),
            },
            MobRunSnapshot {
                run_id: MobRunId("native-execution:native-execution:LUC-277:attempt-3".to_string()),
                meta_flow_run_id: None,
                flow_id: None,
                issue: Some(issue),
                workspace_key: None,
                template_id: None,
                status: MobRunStatus::Running,
                started_at: None,
                updated_at: Utc.timestamp_opt(22, 0).unwrap(),
                completed_at: None,
            },
        ];

        assert_eq!(
            next_native_run_generation_from_durable_runs(&runs, Some(1)),
            Some(4)
        );
    }

    #[test]
    fn active_dispatch_generation_advances_from_workpad_when_registry_is_empty() {
        assert_eq!(
            next_native_run_generation_from_workpad(Some(1), Some(3)),
            Some(4)
        );
    }

    #[test]
    fn restart_recovery_does_not_treat_newer_run_than_workpad_as_stale() {
        let workpad_run_id =
            MobRunId("native-execution:native-execution:LUC-277:attempt-3".to_string());
        let run_id = MobRunId("native-execution:native-execution:LUC-277:attempt-4".to_string());

        assert!(
            native_run_generation_sort_key(&workpad_run_id)
                <= native_run_generation_sort_key(&run_id)
        );
    }

    #[test]
    fn intake_native_dispatch_keeps_base_generation() {
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Todo".to_string();

        assert_eq!(
            native_run_generation_for_dispatch(&issue, &config, None),
            None
        );
    }

    #[test]
    fn workpad_run_id_recovery_prefers_latest_generated_progress_marker() {
        let body = "## Fugue Workpad\n\n### Progress\n- [x] Native mob run: `native-execution:native-execution:LUC-264` (Succeeded).\n\n### Progress\n- [x] Native mob run: `native-execution:native-execution:LUC-264:attempt-1` (Running).\n";

        assert_eq!(
            parse_workpad_native_run_id(body).map(|run_id| run_id.0),
            Some("native-execution:native-execution:LUC-264:attempt-1".to_string())
        );
    }

    #[test]
    fn post_upsert_refresh_retains_unresolved_operator_finding_when_rendered_body_lacks_preserved_section()
     {
        let temp = tempfile::tempdir().unwrap();
        let config = WorkflowConfigManager::from_definition_for_test(
            temp.path().join("fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let workspace_manager = WorkspaceManager::for_service(&config);
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "Rework".to_string();
        let rendered_without_preserved_findings = "- [x] Native mob run: `native-execution:native-execution:LUC-168` (Succeeded).\n- [x] Pull request: https://github.com/lukacf/meerkat-fugue/pull/7\n";
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCheckCi,
                },
                Some("reran external Claude Code Review"),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                Some("known external symlink/directory-mismatch failure"),
            ),
        ];
        let existing = NativeStatusRecovery {
            linear: Some(RecoveredLinearEvidence {
                issue_state: "Rework".to_string(),
                branch_name: Some(BranchName("codex/LUC-168".to_string())),
                workpad_native_run_id: Some(run_id.clone()),
                workpad_mentions_pr: true,
                workpad_pr_number: Some(GitHubPullRequestNumber(7)),
                unresolved_review_finding: Some(
                    "Source: preserve operator Rework finding through post-upsert refresh."
                        .to_string(),
                ),
                reviewer_rework_requested: false,
            }),
            github: Some(recover_native_github_evidence(pr_snapshot(
                7,
                "codex/LUC-168",
                "abc123abc123abc123abc123abc123abc123abcd",
            ))),
            workspace: None,
            review: Some(NativeStatusReviewEvidence {
                reason: "Linear issue is in Rework with unresolved operator review finding: Source: preserve operator Rework finding through post-upsert refresh.".to_string(),
            }),
        };

        let refreshed = refresh_native_status_recovery(
            &run,
            &issue,
            &config,
            &workspace_manager,
            &events,
            &existing,
            Some(rendered_without_preserved_findings),
            None,
        );
        let row = native_recovery_status_row(&NativeRunHandle {
            run_id,
            observed_events: events,
            recovery: refreshed,
        });

        assert!(
            row.unresolved_review_finding
                .as_deref()
                .unwrap()
                .contains("Source")
        );
        let reason = row.review_required.as_deref().unwrap();
        assert!(reason.contains("Rework"), "unexpected reason: {reason}");
        assert!(reason.contains("Source"), "unexpected reason: {reason}");
    }

    #[test]
    fn parse_pr_number_handles_non_ascii_message_boundaries() {
        assert_eq!(
            parse_pr_number("opened 🚀 https://github.com/x/y/pull/7"),
            Some(GitHubPullRequestNumber(7))
        );
    }

    #[tokio::test]
    async fn snapshot_exposes_native_recovery_refreshed_from_durable_sources() {
        let run_id = MobRunId("native-execution:native-execution:LUC-168".to_string());
        let run = MobRunSnapshot {
            run_id: run_id.clone(),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Succeeded,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let mut issue = active_issue_with_labels(&["native"]);
        issue.state = "In Review".to_string();
        let events = vec![
            native_event(
                1,
                crate::tools::MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCreatePr,
                },
                Some("opened https://github.com/lukacf/meerkat-fugue/pull/7"),
            ),
            native_event(
                2,
                crate::tools::MobEventKind::RunCompleted {
                    status: MobRunStatus::Succeeded,
                    structured_output: None,
                },
                Some("native mob done"),
            ),
        ];
        let workpad = "## Fugue Workpad

- [x] Native mob run: `native-execution:native-execution:LUC-168` (Succeeded).

### Pull request or Handoff
- [x] Pull request: https://github.com/lukacf/meerkat-fugue/pull/7";
        let head_sha = "abc123abc123abc123abc123abc123abc123abcd";
        let github = recover_native_github_evidence(pr_snapshot(7, "codex/LUC-168", head_sha));
        let evidence = NativeDurableEvidence {
            run: Some(run.clone()),
            linear: Some(recover_native_linear_evidence(&issue, Some(workpad))),
            github: Some(github),
            workspace: Some(RecoveredWorkspaceEvidence {
                path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
                branch: Some(BranchName("codex/LUC-168".to_string())),
                head_sha: Some(GitSha(head_sha.to_string())),
                upstream_branch: None,
                upstream_head_sha: None,
                dirty: false,
            }),
            events: events.clone(),
        };
        let config = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            native_workflow_definition(),
        )
        .unwrap()
        .effective()
        .clone();
        let recovery = native_status_recovery_from_durable_evidence(&run, evidence, Some(2));
        let mut state = OrchestratorState {
            poll_interval_ms: 30_000,
            max_concurrent_agents: 2,
            running: HashMap::new(),
            claimed: HashSet::new(),
            retry_attempts: HashMap::new(),
            completed: HashSet::new(),
            meerkat_totals: TokenTotals::default(),
            ended_runtime_seconds: 0.0,
            meerkat_rate_limits: None,
            last_error: None,
            recent_events: HashMap::new(),
        };
        state.running.insert(
            issue.id.clone(),
            RunningEntry {
                issue: issue.clone(),
                identifier: issue.identifier.clone(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                workspace_path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
                session: LiveSession {
                    session_id: Some(run_id.0.clone()),
                    ..LiveSession::default()
                },
                started_at: Utc.timestamp_opt(10, 0).unwrap(),
                retry_attempt: None,
                cancel: CancellationToken::new(),
                handle: None,
                termination: None,
                native_run: Some(NativeRunHandle {
                    run_id,
                    observed_events: events,
                    recovery,
                }),
            },
        );
        let shared = Arc::new(RwLock::new(state));

        let snapshot = snapshot(&shared, &config).await;
        let row = &snapshot.running[0];
        let native_recovery = row.native_recovery.as_ref().unwrap();

        assert_eq!(row.backend, StatusBackend::NativeMob);
        assert_eq!(
            native_recovery.workpad_run_id.as_deref(),
            Some("native-execution:native-execution:LUC-168")
        );
        assert_eq!(native_recovery.workpad_pr_number, Some(7));
        assert_eq!(
            native_recovery.github_pr_url.as_deref(),
            Some("https://github.com/lukacf/meerkat-fugue/pull/7")
        );
        assert_eq!(
            native_recovery.github_pr_state,
            Some(PullRequestState::Open)
        );
        assert_eq!(native_recovery.github_head_sha.as_deref(), Some(head_sha));
        assert!(native_recovery.review_required.is_none());
    }

    #[test]
    fn human_lane_moves_follow_configured_active_and_terminal_rules() {
        let config = guarded_config();
        let tracker = &config.tracker;

        assert_eq!(
            interpret_human_lane_move(tracker, "In Progress"),
            HumanLaneAction::Continue
        );
        assert_eq!(
            interpret_human_lane_move(tracker, "Done"),
            HumanLaneAction::StopAndCleanup
        );
        assert_eq!(
            interpret_human_lane_move(tracker, "Canceled"),
            HumanLaneAction::StopAndCleanup
        );
    }

    #[tokio::test]
    async fn labeled_guarded_dispatch_observes_without_host_worker_start() {
        let orchestrator = orchestrator_for_definition(workflow_definition(json!({
            "authority_mode": "guarded_labeled",
            "safe_selectors": {
                "linear_labels": ["kapellmeister-safe"]
            }
        })));

        orchestrator
            .dispatch_issue_with_policy(
                active_issue_with_labels(&["kapellmeister-safe"]),
                None,
                false,
            )
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(state.running.is_empty());
    }

    #[tokio::test]
    async fn unlabeled_dispatch_observes_without_host_start() {
        let orchestrator = orchestrator_for_definition(workflow_definition(json!({
            "authority_mode": "guarded_labeled",
            "safe_selectors": {
                "linear_labels": ["kapellmeister-safe"]
            }
        })));

        orchestrator
            .dispatch_issue_with_policy(active_issue_with_labels(&[]), None, false)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(state.running.is_empty());
    }

    #[tokio::test]
    async fn kapellmeister_host_dispatch_observes_without_starting_native_mob() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());

        orchestrator
            .dispatch_issue_with_policy(
                active_issue_with_labels(&["kapellmeister-safe"]),
                None,
                false,
            )
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(state.running.is_empty());
        assert_eq!(native_host.spawn_call_count(), 0);
    }

    #[tokio::test]
    async fn duplicate_kapellmeister_host_dispatch_signal_stays_observation_only() {
        let (orchestrator, native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let issue = active_issue_with_labels(&["kapellmeister-safe"]);

        orchestrator
            .dispatch_issue_with_policy(issue.clone(), None, false)
            .await;
        orchestrator
            .dispatch_issue_with_policy(issue, None, false)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(state.running.is_empty());
        assert_eq!(native_host.spawn_call_count(), 0);
    }

    #[tokio::test]
    async fn kapellmeister_authorized_native_launch_does_not_host_project_planning_lane() {
        let (orchestrator, _native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let config = orchestrator.config.effective().clone();
        let mut issue = active_issue_with_labels(&["kapellmeister-safe"]);
        issue.state = "Todo".to_string();

        orchestrator
            .dispatch_native_execution_mob(
                issue,
                None,
                None,
                IssuePolicyMode::Kapellmeister,
                config,
            )
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert_eq!(state.running.len(), 1);
        assert_eq!(
            state.running["issue-1"].issue.state, "Todo",
            "Kapellmeister-mode launch must leave Linear projection to issue lead tools"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kapellmeister_native_reconcile_refreshes_recovery_without_host_projection() {
        let temp = tempfile::tempdir().unwrap();
        let ims_path = temp.path().join("fugue-ims.sqlite");
        let mut tracker = crate::fugue_ims::fugue_ims_tracker_config(&ims_path);
        tracker.project_slug = Some("lukacf/meerkat-fugue".into());
        crate::fugue_ims::create_issue_for_config(
            &tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("LUC-168".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Refresh Kapellmeister recovery".to_string(),
                description: Some("SQLite-backed IMS issue".to_string()),
                stage: crate::ims::ImsStageName("Implementing".to_string()),
                labels: vec!["kapellmeister-safe".to_string()],
            },
        )
        .unwrap();

        let workspace_root = temp.path().join("workspaces");
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "tracker".to_string(),
            json!({
                "kind": "fugue-ims",
                "endpoint": ims_path.display().to_string(),
                "project_slug": "lukacf/meerkat-fugue",
                "intake_states": ["Todo"],
                "active_states": ["Todo", "Rework", "Planning", "Implementing", "Reviewing"],
                "terminal_states": ["Ready for review", "Done", "Canceled", "Duplicate"]
            }),
        );
        definition.config.insert(
            "workspace".to_string(),
            json!({ "root": workspace_root.display().to_string() }),
        );
        let (mut orchestrator, native_host) = native_orchestrator_for_definition(definition);
        orchestrator.native_event_observer = crate::mob_event_observer::NativeMobEventObserver::new(
            crate::mob_event_observer::MobEventCursorStore::new(
                temp.path().join("meta-structural-cursors.json"),
            ),
        );
        let result = orchestrator
            .handle_kapellmeister_start_issue("LUC-168".to_string())
            .await;
        assert!(result.accepted, "{}", result.message);

        let workspace_path = workspace_root.join("LUC-168");
        std::fs::create_dir_all(&workspace_path).unwrap();
        run_git(&workspace_path, &["init"]);
        run_git(
            &workspace_path,
            &["config", "user.email", "fugue@example.invalid"],
        );
        run_git(&workspace_path, &["config", "user.name", "Fugue Test"]);
        std::fs::write(workspace_path.join("README.md"), "kapellmeister\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "initial"]);
        run_git(&workspace_path, &["checkout", "-b", "codex/LUC-168"]);
        std::fs::write(workspace_path.join("README.md"), "kapellmeister refresh\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "refresh"]);
        let refreshed_head = git_head_sha(&workspace_path);
        let run_id = {
            let state = orchestrator.shared_state();
            let state = state.read().await;
            state
                .running
                .values()
                .find(|entry| entry.identifier == "LUC-168")
                .unwrap()
                .native_run
                .as_ref()
                .unwrap()
                .run_id
                .clone()
        };
        native_host.set_status_override(MobRunSnapshot {
            run_id,
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(issue_ref_for_identifier("LUC-168")),
            workspace_key: Some(workspace_key_for_identifier("LUC-168")),
            template_id: None,
            status: MobRunStatus::Running,
            started_at: Some(Utc.timestamp_opt(10, 0).unwrap()),
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        });

        orchestrator
            .reconcile_native_mob_events_with_tools(None, None)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        let entry = state
            .running
            .values()
            .find(|entry| entry.identifier == "LUC-168")
            .unwrap();
        assert_eq!(entry.issue.state, "Implementing");
        let recovery = &entry.native_run.as_ref().unwrap().recovery;
        assert_eq!(
            recovery
                .workspace
                .as_ref()
                .and_then(|workspace| workspace.head_sha.as_ref())
                .map(|sha| sha.0.as_str()),
            Some(refreshed_head.as_str())
        );
    }

    // Exercise terminal handling against a real local tracker, including its
    // fresh lane read. The worker and coordinator transports are recorded so
    // these policy tests do not start competing process-global Meta runtimes.
    async fn dispatched_terminal_fixture(
        lane: &str,
    ) -> (
        tempfile::TempDir,
        Orchestrator,
        Arc<RecordingNativeMobExecutionHost>,
        Issue,
    ) {
        let workspace = tempfile::tempdir().expect("isolated workspace");
        let mut definition = native_kapellmeister_workflow_definition();
        definition.config.insert(
            "tracker".to_owned(),
            json!({
                "kind": "fugue-ims",
                "endpoint": workspace.path().join("issues.sqlite"),
                "project_slug": "fixture/repository",
                "active_states": ["Todo", "In Progress"],
                "terminal_states": ["Done", "Canceled"]
            }),
        );
        definition.config.insert(
            "workspace".to_owned(),
            json!({"root": workspace.path().join("workspaces")}),
        );
        definition.config.insert(
            "meerkat".to_owned(),
            json!({"state_root": workspace.path().join("runtime")}),
        );
        let (orchestrator, native_host) = native_orchestrator_for_definition(definition);
        let config = orchestrator.config.effective().clone();
        let created = crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("LUC-168".to_owned()),
                repo: "fixture/repository".to_owned(),
                title: "Terminal event fixture".to_owned(),
                description: None,
                stage: crate::ims::ImsStageName("Todo".to_owned()),
                labels: vec!["kapellmeister-safe".to_owned()],
            },
        )
        .expect("create local issue");
        let adapter = IssueAdapter::from_config(&config.tracker).expect("local tracker");
        let issue = adapter
            .fetch_issue_states_by_ids(&[created.id.0])
            .await
            .expect("read local issue")
            .pop()
            .expect("fixture issue");
        orchestrator
            .dispatch_native_execution_mob(
                issue.clone(),
                None,
                None,
                IssuePolicyMode::Kapellmeister,
                config,
            )
            .await;
        {
            let state = orchestrator.shared_state();
            let state = state.read().await;
            assert!(
                state.running.contains_key(&issue.id),
                "native dispatch must succeed before terminal handling: {:?}",
                state.last_error
            );
        }
        adapter
            .move_issue_as_operator(MoveIssueInput {
                issue: issue_ref_for_identifier(&issue.identifier),
                state: LinearStateRef::Name {
                    name: lane.to_owned(),
                },
                expected_updated_at: None,
                idempotency_key: IdempotencyKey(format!("fixture-lane:{lane}")),
            })
            .await
            .expect("update tracker lane");
        let messages = orchestrator
            .captured_native_signal_messages
            .as_ref()
            .expect("recorded coordinator transport")
            .lock()
            .expect("signal messages");
        assert_eq!(messages.len(), 1, "dispatch must notify the coordinator");
        assert!(messages[0].contains("worker_started"));
        assert!(messages[0].contains(&issue.identifier));
        drop(messages);
        assert!(
            !workspace.path().join("runtime").exists(),
            "recording worker tests must not start a real Meta runtime"
        );
        (workspace, orchestrator, native_host, issue)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kapellmeister_native_terminal_event_keeps_nonterminal_issue_running() {
        let (_workspace, mut orchestrator, native_host, issue) =
            dispatched_terminal_fixture("Planning").await;

        orchestrator
            .handle_native_run_finished(issue.id.clone(), issue.clone(), MobRunStatus::Succeeded)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(
            state.running.contains_key(&issue.id),
            "Meta terminal status alone must not retire prompt-controlled nonterminal issues"
        );
        assert_eq!(state.running[&issue.id].issue.state, "Planning");
        assert!(
            native_host.cleanup_calls().is_empty(),
            "cleanup must wait until the issue agent moves the IMS lane to a terminal state"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kapellmeister_native_terminal_event_keeps_review_ready_issue_running() {
        let (_workspace, mut orchestrator, native_host, issue) =
            dispatched_terminal_fixture("Ready for review").await;

        orchestrator
            .handle_native_run_finished(issue.id.clone(), issue.clone(), MobRunStatus::Succeeded)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(
            state.running.contains_key(&issue.id),
            "Ready for review is an attention lane and must keep the issue mob visible"
        );
        assert_eq!(state.running[&issue.id].issue.state, "Ready for review");
        assert!(
            native_host.cleanup_calls().is_empty(),
            "Ready for review must not retire visible Meta members"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn kapellmeister_native_terminal_event_retires_terminal_issue_members() {
        let (_workspace, mut orchestrator, native_host, issue) =
            dispatched_terminal_fixture("Done").await;

        orchestrator
            .handle_native_run_finished(issue.id.clone(), issue.clone(), MobRunStatus::Succeeded)
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(
            !state.running.contains_key(&issue.id),
            "terminal issue lanes must remove Fugue running state"
        );
        let cleanup_calls = native_host.cleanup_calls();
        assert_eq!(cleanup_calls.len(), 1);
        assert_eq!(cleanup_calls[0].1, "native-terminal-lane-cleanup");
        assert!(
            cleanup_calls[0].0.0.contains("LUC-168"),
            "terminal issue lanes must also retire visible Meta members"
        );
    }

    #[tokio::test]
    async fn kapellmeister_running_issue_ignores_nonterminal_host_stop_but_accepts_terminal_stop() {
        let (mut orchestrator, _native_host) =
            native_orchestrator_for_definition(native_kapellmeister_workflow_definition());
        let config = orchestrator.config.effective().clone();
        let mut issue = active_issue_with_labels(&["kapellmeister-safe"]);
        issue.state = "Implementing".to_string();

        orchestrator
            .dispatch_native_execution_mob(
                issue.clone(),
                None,
                None,
                IssuePolicyMode::Kapellmeister,
                config,
            )
            .await;

        orchestrator
            .terminate_running_issue(
                &issue.id,
                false,
                false,
                "non_active_state",
                TerminationAuthority::LinearNonterminalLane,
            )
            .await;
        {
            let state = orchestrator.shared_state();
            let state = state.read().await;
            assert!(
                state.running.contains_key(&issue.id),
                "nonterminal Linear drift must stay observation-only for Kapellmeister issues"
            );
        }

        orchestrator
            .terminate_running_issue(
                &issue.id,
                true,
                false,
                "terminal_state",
                TerminationAuthority::LinearTerminalLane,
            )
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(
            !state.running.contains_key(&issue.id),
            "terminal Linear lanes retire the live issue mob"
        );
    }

    #[tokio::test]
    async fn kapellmeister_retry_scheduling_is_suppressed() {
        let definition = workflow_definition(json!({
            "authority_mode": "guarded_labeled",
            "safe_selectors": {
                "linear_labels": ["kapellmeister-safe"]
            }
        }));
        let (refresh_tx, refresh_rx) = mpsc::channel(1);
        drop(refresh_tx);
        let manager = WorkflowConfigManager::from_definition_for_test(
            PathBuf::from("/tmp/fugue.yaml"),
            definition,
        )
        .unwrap();
        let orchestrator = Orchestrator::new(manager, refresh_rx);

        orchestrator
            .schedule_retry(
                "issue-1".to_string(),
                "LUC-168".to_string(),
                IssuePolicyMode::Kapellmeister,
                1,
                Some("first".to_string()),
                false,
            )
            .await;
        orchestrator
            .schedule_retry(
                "issue-1".to_string(),
                "LUC-168".to_string(),
                IssuePolicyMode::Kapellmeister,
                2,
                Some("second".to_string()),
                false,
            )
            .await;

        let state = orchestrator.shared_state();
        let state = state.read().await;
        assert!(state.retry_attempts.is_empty());
        assert!(!state.claimed.contains("issue-1"));
    }

    #[tokio::test]
    async fn snapshot_exposes_policy_mode_for_running_and_retrying_issues() {
        let config = guarded_config();
        let mut state = OrchestratorState {
            poll_interval_ms: 30_000,
            max_concurrent_agents: 2,
            running: HashMap::new(),
            claimed: HashSet::new(),
            retry_attempts: HashMap::new(),
            completed: HashSet::new(),
            meerkat_totals: TokenTotals::default(),
            ended_runtime_seconds: 0.0,
            meerkat_rate_limits: None,
            last_error: None,
            recent_events: HashMap::new(),
        };
        let cancel = CancellationToken::new();
        let running_handle = tokio::spawn(async {});
        state.running.insert(
            "running-1".to_string(),
            RunningEntry {
                issue: issue("running-1", "LUC-168", Some(1), 10),
                identifier: "LUC-168".to_string(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                workspace_path: "/tmp/fugue-test-workspaces/LUC-168".to_string(),
                session: LiveSession::default(),
                started_at: Utc.timestamp_opt(10, 0).unwrap(),
                retry_attempt: None,
                cancel,
                handle: Some(running_handle),
                termination: None,
                native_run: None,
            },
        );
        let retry_handle = tokio::spawn(async {});
        state.retry_attempts.insert(
            "retry-1".to_string(),
            RetryEntry {
                identifier: "LUC-169".to_string(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                workspace_path: "/tmp/fugue-test-workspaces/LUC-169".to_string(),
                attempt: 1,
                native_run_generation: None,
                due_at: Utc.timestamp_opt(20, 0).unwrap(),
                error: None,
                handle: retry_handle,
            },
        );
        let shared = Arc::new(RwLock::new(state));

        let snapshot = snapshot(&shared, &config).await;
        let encoded = serde_json::to_value(&snapshot).unwrap();

        assert_eq!(
            snapshot.running[0].policy_mode,
            IssuePolicyMode::Kapellmeister
        );
        assert_eq!(
            snapshot.retrying[0].policy_mode,
            IssuePolicyMode::Kapellmeister
        );
        assert_eq!(snapshot.running[0].backend, StatusBackend::NativeMob);
        assert_eq!(snapshot.running[0].workspace_key, "LUC-168");
        assert_eq!(snapshot.running[0].issue.identifier, "LUC-168");
        assert_eq!(snapshot.running[0].lifecycle.lane, "Todo");
        assert_eq!(
            snapshot.running[0].lifecycle.status,
            RunningLifecycleStatus::Running
        );
        assert!(snapshot.running[0].native_recovery.is_none());
        assert_eq!(encoded["running"][0]["policy_mode"], "kapellmeister");
        assert_eq!(encoded["running"][0]["backend"], "native_mob");
        assert_eq!(encoded["running"][0]["issue"]["identifier"], "LUC-168");
        assert_eq!(encoded["running"][0]["workspace_key"], "LUC-168");
        assert_eq!(encoded["running"][0]["lifecycle"]["lane"], "Todo");
        assert!(encoded["running"][0]["native_recovery"].is_null());
        assert_eq!(encoded["retrying"][0]["policy_mode"], "kapellmeister");
    }

    #[tokio::test]
    async fn snapshot_replaces_stale_native_delivery_timeout_with_recovery_summary() {
        let config = guarded_config();
        let head_sha = "19167f64d6c696182f120a08d99e7add364b72f1";
        let pr = pr_snapshot(191, "luka/luc-624-final-review", head_sha);
        let mut state = OrchestratorState {
            poll_interval_ms: 30_000,
            max_concurrent_agents: 2,
            running: HashMap::new(),
            claimed: HashSet::new(),
            retry_attempts: HashMap::new(),
            completed: HashSet::new(),
            meerkat_totals: TokenTotals::default(),
            ended_runtime_seconds: 0.0,
            meerkat_rate_limits: None,
            last_error: None,
            recent_events: HashMap::new(),
        };
        let mut issue = issue("running-1", "LUC-624", Some(1), 10);
        issue.state = "Final Review & Merge".to_string();
        state.running.insert(
            issue.id.clone(),
            RunningEntry {
                issue,
                identifier: "LUC-624".to_string(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                workspace_path: "/tmp/fugue-test-workspaces/LUC-624".to_string(),
                session: LiveSession {
                    session_id: Some("final-pr-gate:meerkat-fugue:191:6bb24de3".to_string()),
                    last_meerkat_event: Some("native_mob_structural_event".to_string()),
                    last_meerkat_message: Some(
                        "issue lead task delivery timed out after 15s".to_string(),
                    ),
                    ..LiveSession::default()
                },
                started_at: Utc.timestamp_opt(10, 0).unwrap(),
                retry_attempt: None,
                cancel: CancellationToken::new(),
                handle: None,
                termination: None,
                native_run: Some(NativeRunHandle {
                    run_id: MobRunId("final-pr-gate:meerkat-fugue:191:6bb24de3".to_string()),
                    observed_events: Vec::new(),
                    recovery: NativeStatusRecovery {
                        github: Some(recover_native_github_evidence(pr)),
                        workspace: Some(RecoveredWorkspaceEvidence {
                            path: "/tmp/fugue-test-workspaces/LUC-624".to_string(),
                            branch: Some(BranchName("luka/luc-624-final-review".to_string())),
                            head_sha: Some(GitSha(head_sha.to_string())),
                            upstream_branch: None,
                            upstream_head_sha: None,
                            dirty: false,
                        }),
                        ..NativeStatusRecovery::default()
                    },
                }),
            },
        );
        let shared = Arc::new(RwLock::new(state));

        let snapshot = snapshot(&shared, &config).await;
        let row = &snapshot.running[0];

        assert_eq!(row.issue.identifier, "LUC-624");
        assert_eq!(row.workspace_key, "LUC-624");
        assert_eq!(row.lifecycle.lane, "Final Review & Merge");
        let message = row.last_message.as_deref().unwrap();
        assert!(
            message.contains("tracks PR #191"),
            "unexpected message: {message}"
        );
        assert!(
            !message.contains("timed out"),
            "stale timeout leaked into status row: {message}"
        );
    }

    #[test]
    fn dispatch_sort_priority_created_identifier() {
        let mut issues = vec![
            issue("3", "B-2", Some(2), 10),
            issue("1", "B-1", Some(1), 20),
            issue("2", "A-1", Some(1), 10),
        ];
        sort_for_dispatch(&mut issues);
        assert_eq!(
            issues
                .iter()
                .map(|issue| issue.identifier.as_str())
                .collect::<Vec<_>>(),
            vec!["A-1", "B-1", "B-2"]
        );
    }

    #[test]
    fn workpad_pr_detection_requires_concrete_pr_evidence() {
        let pending =
            "### Pull request or Handoff\n- [ ] Pending native mob output and PR handoff.\n";
        let url = "### Pull request or Handoff\n- [x] Opened https://github.com/lukacf/meerkat-fugue/pull/55\n";
        let shorthand = "### Pull request or Handoff\n- [x] PR #55 is ready for merge.\n";
        let unrelated_commit_title = "\
### Runtime recovery — coder terminal failure
- [x] Issue lead inspected workspace state after the failure: `git status --short` was clean, branch was `main`, and HEAD was `f9635a8 Fix Overview layout responsiveness (#245)`.

### PR Review Monitor
- [x] PR #245 observed: https://github.com/lukacf/meerkat-fugue/pull/245; state `Merged`.
- [ ] Rejected PR evidence: PR #245: head branch `luka/luc-652-this-looks-like-shit` does not match current issue/workspace branch `luka/luc-654-hardening-and-improving-of-ui`.
";

        assert!(!workpad_mentions_pr(pending));
        assert_eq!(parse_first_pr_number(pending), None);
        assert!(workpad_mentions_pr(url));
        assert_eq!(
            parse_first_pr_number(url),
            Some(GitHubPullRequestNumber(55))
        );
        assert!(workpad_mentions_pr(shorthand));
        assert_eq!(
            parse_first_pr_number(shorthand),
            Some(GitHubPullRequestNumber(55))
        );
        assert!(!workpad_mentions_pr(unrelated_commit_title));
        assert_eq!(parse_first_pr_number(unrelated_commit_title), None);
    }

    #[test]
    fn snapshot_rows_sort_deterministically() {
        let now = Utc.timestamp_opt(10, 0).unwrap();
        let mut running = vec![
            RunningRow {
                issue_id: "2".to_string(),
                issue_identifier: "B-1".to_string(),
                issue: RunningIssueRow {
                    id: "2".to_string(),
                    identifier: "B-1".to_string(),
                    title: "B".to_string(),
                    url: None,
                },
                workspace_key: "B-1".to_string(),
                lifecycle: RunningLifecycleRow {
                    lane: "Todo".to_string(),
                    status: RunningLifecycleStatus::Running,
                },
                policy_mode: IssuePolicyMode::Kapellmeister,
                backend: StatusBackend::NativeMob,
                state: "Todo".to_string(),
                session_id: None,
                session_ref: None,
                stream_id: None,
                turn_count: 0,
                last_event: None,
                last_message: None,
                started_at: now,
                last_event_at: None,
                tokens: TokenTotals::default(),
                native_recovery: None,
            },
            RunningRow {
                issue_id: "1".to_string(),
                issue_identifier: "A-1".to_string(),
                issue: RunningIssueRow {
                    id: "1".to_string(),
                    identifier: "A-1".to_string(),
                    title: "A".to_string(),
                    url: None,
                },
                workspace_key: "A-1".to_string(),
                lifecycle: RunningLifecycleRow {
                    lane: "Todo".to_string(),
                    status: RunningLifecycleStatus::Running,
                },
                policy_mode: IssuePolicyMode::Kapellmeister,
                backend: StatusBackend::NativeMob,
                state: "Todo".to_string(),
                session_id: None,
                session_ref: None,
                stream_id: None,
                turn_count: 0,
                last_event: None,
                last_message: None,
                started_at: now,
                last_event_at: None,
                tokens: TokenTotals::default(),
                native_recovery: None,
            },
        ];
        sort_running_rows(&mut running);
        assert_eq!(
            running
                .iter()
                .map(|row| row.issue_identifier.as_str())
                .collect::<Vec<_>>(),
            vec!["A-1", "B-1"]
        );

        let mut retrying = vec![
            RetryRow {
                issue_id: "2".to_string(),
                issue_identifier: "B-1".to_string(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                attempt: 1,
                due_at: Utc.timestamp_opt(20, 0).unwrap(),
                error: None,
            },
            RetryRow {
                issue_id: "1".to_string(),
                issue_identifier: "A-1".to_string(),
                policy_mode: IssuePolicyMode::Kapellmeister,
                attempt: 1,
                due_at: now,
                error: None,
            },
        ];
        sort_retry_rows(&mut retrying);
        assert_eq!(
            retrying
                .iter()
                .map(|row| row.issue_identifier.as_str())
                .collect::<Vec<_>>(),
            vec!["A-1", "B-1"]
        );
    }
}
