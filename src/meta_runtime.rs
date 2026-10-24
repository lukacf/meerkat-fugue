use crate::host_signal_journal::HostSignalBinding;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use chrono::Utc;
use meerkat_core::mcp_config::McpServerConfig;
use meerkat_core::service::{CreateSessionRequest, SessionBuildOptions, SessionError};
use meerkat_core::types::{HandlingMode, ToolDef};
use meerkat_core::{AgentToolDispatcher, ToolCallView, ToolResult};
use meerkat_meta::{
    ConsoleFrame, ConsoleTimelineMode, ConsoleTimelineQuery, ConsoleTimelineWindowQuery,
    ConsoleVisibilityPolicy, EventQuery, MobStructuralEventEnvelope, RuntimeDecisionState,
    SessionHook, UnifiedRuntime,
};
use meerkat_mob::definition::LimitsSpec;
use meerkat_mob::{
    AgentIdentity, MobDefinition, MobHandle, MobRuntimeMode, Profile, ProfileBinding, ProfileName,
    SpawnMemberSpec, ToolConfig,
};
use meerkat_tools::builtin::shell::ShellConfig;
use meerkat_tools::{BuiltinToolConfig, CompositeDispatcher, MemoryTaskStore, ToolPolicyLayer};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::warn;

use crate::agent::{
    execute_github_fugue_tool_with_host, external_tools_for_tracker, is_github_fugue_tool_name,
};
use crate::config::{
    FinalReviewReviewerConfig, IndependentReviewConfig, MaterializedMobTemplate, MeerkatConfig,
    MetaConsoleBasicAuthConfig, NativeAgentProfile, NativeMcpServerConfig, NativeMobFlow,
    TrackerConfig, default_agent_model, is_native_linear_mcp_server,
};
use crate::deploy_gate::DeployStartRequest;
use crate::final_pr_gate::FinalPrGateStartRequest;
use crate::independent_review::{
    IndependentReviewCompletionRequest, IndependentReviewLaunchRequest,
    independent_review_completion_input_schema,
};
use crate::linear_tools::{
    LINEAR_ACTION_STATES_ENV, LINEAR_REVIEW_HANDOFF_STATES_ENV, execute_linear_fugue_tool_bound,
    is_linear_fugue_tool_name,
};
use crate::mob_tools::{NativeMobExecutionHost, NativeMobLaunchPlan, ObserveStructuralEventsInput};
use crate::tools::{
    CancelExecutionInput, Cursor, FugueToolErrorCode, FugueToolOperation, FugueToolOutput,
    FugueToolResult, GitHubOwner, GitHubRepoName, GitHubRepoRef, InspectRunInput, LinearIssueRef,
    ListRunsInput, ListRunsOutput, MobEventKind, MobEventSnapshot, MobRunId, MobRunSnapshot,
    MobRunStatus, RetryAdvice, SpawnOrResumeExecutionInput, SpawnOrResumeExecutionOutput,
    ToolExecutionMetadata,
};
use crate::workgraph_gate::{
    WORKGRAPH_BLOCKED_STATES_ENV, WorkGraphTerminalGate,
    guard_public_lane_move as guard_direct_public_lane_move,
};

#[cfg(test)]
use crate::tools::{LinearStateRef, MoveIssueInput};

const MOB_ID: &str = crate::native_mob_contract::FUGUE_MOB_ID;
pub(crate) const KAPELLMEISTER: &str = "kapellmeister";
const ISSUE_LEAD: &str = "issue_lead";
const FUGUE_CONTEXT_KEY: &str = "fugue";
const FUGUE_CONTEXT_WORKTREE_PATH_KEY: &str = "worktree_path";
const FUGUE_HOST_API_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const FUGUE_HOST_API_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const META_EVENT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_NATIVE_MAX_ORPHANED_TURNS: u32 = 128;
const TURN_LIMIT_PHASE: &str = "turn_limit_reached";
const MEMBER_TERMINAL_FAILURE_PHASE: &str = "member_terminal_failure";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FugueMobJob {
    run: MobRunSnapshot,
    member_identities: Vec<String>,
    notified_member_failure_cursors: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    launch_failure: Option<crate::tools::NativeLaunchFailure>,
}

impl FugueMobJob {
    fn new(run: MobRunSnapshot, member_identities: Vec<String>) -> Self {
        Self {
            run,
            member_identities,
            notified_member_failure_cursors: BTreeSet::new(),
            launch_failure: None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct FugueConsoleVisibilityPolicy;

impl ConsoleVisibilityPolicy for FugueConsoleVisibilityPolicy {
    fn include_implicit_delegate_members(&self) -> bool {
        false
    }

    fn frame_visible(&self, frame: &ConsoleFrame) -> bool {
        !is_raw_comms_admission_run_started_frame(frame)
    }
}

fn is_raw_comms_admission_run_started_frame(frame: &ConsoleFrame) -> bool {
    if frame.kind != "run_started" {
        return false;
    }
    let Some(prompt) = console_frame_prompt_text(frame) else {
        return false;
    };
    is_raw_comms_admission_prompt(prompt.trim())
}

fn console_frame_prompt_text(frame: &ConsoleFrame) -> Option<&str> {
    frame.payload.get("prompt")?.as_str()
}

fn is_raw_comms_admission_prompt(prompt: &str) -> bool {
    let first_line = prompt.lines().next().unwrap_or_default().trim();
    if first_line.starts_with("[COMMS ") {
        return true;
    }
    let lower = first_line.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "peer message" | "peer request" | "peer response"
    ) || lower.starts_with("peer message from ")
        || lower.starts_with("peer request from ")
        || lower.starts_with("peer response from ")
}

#[derive(Clone, Debug)]
pub(crate) struct RepositorySpecialist {
    pub identity: String,
    pub label: String,
    pub focus: String,
    pub paths: Vec<PathBuf>,
}

fn resolve_review_specialists(
    reviewers: &meerkat_mob::FlowRoleTargets,
    specialists: &[RepositorySpecialist],
    issue_participants: &BTreeSet<String>,
) -> Result<meerkat_mob::FlowRoleTargets, crate::review_requests::ReviewRequestError> {
    let mut bindings = Vec::new();
    for (role, identities) in reviewers.bound_roles() {
        let mut resolved = Vec::new();
        for identity in identities {
            let specialist = specialists
                .iter()
                .find(|specialist| {
                    specialist.identity == identity.as_str()
                        || meerkat_meta::member_comms_id::roster_member_id_for_identity(
                            &specialist.identity,
                        ) == *identity
                })
                .ok_or(crate::review_requests::ReviewRequestError::IneligibleReviewer)?;
            let native =
                meerkat_meta::member_comms_id::roster_member_id_for_identity(&specialist.identity);
            if issue_participants.contains(native.as_str())
                || issue_participants.contains(&specialist.identity)
            {
                return Err(crate::review_requests::ReviewRequestError::IneligibleReviewer);
            }
            resolved.push(native);
        }
        bindings.push((role.clone(), resolved));
    }
    meerkat_mob::FlowRoleTargets::new(bindings)
        .map_err(|_| crate::review_requests::ReviewRequestError::IneligibleReviewer)
}

fn require_review_capacity(
    required: usize,
    specialists: &[RepositorySpecialist],
    participants: &BTreeSet<String>,
) -> Result<(), crate::review_requests::ReviewRequestError> {
    let eligible: BTreeSet<_> = specialists
        .iter()
        .filter_map(|specialist| {
            let native =
                meerkat_meta::member_comms_id::roster_member_id_for_identity(&specialist.identity);
            (!participants.contains(&specialist.identity)
                && !participants.contains(native.as_str()))
            .then_some(native)
        })
        .collect();
    if eligible.len() < required {
        return Err(
            crate::review_requests::ReviewRequestError::InsufficientCapacity {
                required,
                available: eligible.len(),
            },
        );
    }
    Ok(())
}

fn review_selection_count(
    independent: &IndependentReviewConfig,
    final_review: &crate::config::FinalReviewConfig,
    reviewers: &meerkat_mob::FlowRoleTargets,
    specialists: &[RepositorySpecialist],
    participants: &BTreeSet<String>,
) -> Result<usize, crate::review_requests::ReviewRequestError> {
    // Admission supports either enabled procedure. Check the minimum capacity
    // for any admissible procedure before validating the caller's selection.
    // Exact counts and required profiles are still checked below.
    let minimum = independent
        .enabled
        .then_some(usize::from(independent.reviewer_count))
        .into_iter()
        .chain(final_review.enabled.then(|| {
            usize::from(final_review.minimum_reviewers).max(
                final_review
                    .reviewers
                    .iter()
                    .filter(|reviewer| reviewer.required)
                    .count(),
            )
        }))
        .min()
        .ok_or(crate::review_requests::ReviewRequestError::Unavailable)?;
    require_review_capacity(minimum, specialists, participants)?;
    Ok(crate::native_review::configured_review_count(
        independent,
        final_review,
        reviewers,
    )?)
}

#[derive(Clone)]
pub struct MetaRuntimeRegistry {
    meerkat: MeerkatConfig,
    repo_mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
    tracker: TrackerConfig,
    host_signal_binding: Option<HostSignalBinding>,
    host_api_url: Option<String>,
    host_api_auth: Option<MetaConsoleBasicAuthConfig>,
    templates: BTreeMap<String, MaterializedMobTemplate>,
    final_review: crate::config::FinalReviewConfig,
    independent_review: IndependentReviewConfig,
    runtime: Arc<tokio::sync::Mutex<Option<Arc<UnifiedRuntime>>>>,
    specialists: Arc<OnceLock<Vec<RepositorySpecialist>>>,
    console: Arc<OnceLock<Arc<crate::console::ConsoleHub>>>,
    meta_task_runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    event_drain_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    kapellmeister_bootstrap: Arc<tokio::sync::Mutex<()>>,
    jobs: Arc<Mutex<BTreeMap<String, FugueMobJob>>>,
    jobs_loaded: Arc<OnceLock<Result<(), String>>>,
    stopping: Arc<AtomicBool>,
    launch_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl MetaRuntimeRegistry {
    fn review_requests(&self) -> crate::review_requests::ReviewRequestStore {
        crate::review_requests::ReviewRequestStore::new(
            self.state_path().join("review-requests.sqlite3"),
        )
    }

    pub(crate) async fn with_current_review<T: Send + 'static>(
        &self,
        intent: crate::native_review::ReviewIntent,
        action: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, crate::review_requests::ReviewRequestError> {
        let store = self.review_requests();
        tokio::task::spawn_blocking(move || store.with_current(&intent, action))
            .await
            .map_err(|_| crate::review_requests::ReviewRequestError::Unavailable)?
    }

    pub(crate) fn verify_review_independence(
        &self,
        intent: &crate::native_review::ReviewIntent,
    ) -> Result<(), crate::review_requests::ReviewRequestError> {
        use crate::review_requests::ReviewRequestError;
        self.restore_jobs()
            .map_err(|_| ReviewRequestError::Unavailable)?;
        let participants: BTreeSet<_> = self
            .jobs
            .lock()
            .map_err(|_| ReviewRequestError::Unavailable)?
            .values()
            .filter(|job| match &job.run.issue {
                Some(LinearIssueRef::Id { id }) => id == &intent.candidate.issue_id,
                Some(LinearIssueRef::Identifier { identifier }) => {
                    identifier.0 == intent.candidate.issue_identifier
                }
                None => false,
            })
            .flat_map(|job| job.member_identities.iter().cloned())
            .collect();
        let specialists = self
            .specialists
            .get()
            .ok_or(ReviewRequestError::Unavailable)?;
        let resolved = resolve_review_specialists(&intent.reviewers, specialists, &participants)?;
        if resolved != intent.reviewers {
            return Err(ReviewRequestError::IneligibleReviewer);
        }
        Ok(())
    }

    pub(crate) async fn read_current_native_review(
        &self,
        candidate: &crate::native_review::ReviewCandidate,
    ) -> Result<crate::native_review::VerifiedReview, crate::native_review::ReviewProofError> {
        let store = self.review_requests();
        let lookup = candidate.clone();
        let intent = tokio::task::spawn_blocking(move || store.current(&lookup))
            .await
            .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?
            .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?
            .ok_or(crate::native_review::ReviewProofError::Superseded)?;
        let reference =
            crate::native_review::reference_for(&self.meerkat.mob_id().as_ref().into(), &intent)?;
        self.read_native_review(candidate, &reference).await
    }

    pub(crate) async fn admit_native_review(
        &self,
        mut draft: crate::review_requests::ReviewRequestDraft,
    ) -> Result<
        (
            crate::native_review::ReviewIntent,
            crate::native_review::ReviewReference,
        ),
        crate::review_requests::ReviewRequestError,
    > {
        use crate::review_requests::ReviewRequestError;
        if !self.reviews_enabled() || self.stopping.load(Ordering::SeqCst) {
            return Err(ReviewRequestError::Unavailable);
        }
        self.restore_jobs()
            .map_err(|_| ReviewRequestError::Unavailable)?;
        let participants: BTreeSet<String> = self
            .jobs
            .lock()
            .map_err(|_| ReviewRequestError::Unavailable)?
            .values()
            .filter(|job| match &job.run.issue {
                Some(LinearIssueRef::Id { id }) => id == &draft.candidate.issue_id,
                Some(LinearIssueRef::Identifier { identifier }) => {
                    identifier.0 == draft.candidate.issue_identifier
                }
                None => false,
            })
            .flat_map(|job| job.member_identities.iter().cloned())
            .collect();
        let specialists = self
            .specialists
            .get()
            .ok_or(ReviewRequestError::Unavailable)?;
        let required = review_selection_count(
            &self.independent_review,
            &self.final_review,
            &draft.reviewers,
            specialists,
            &participants,
        )?;
        draft.reviewers = resolve_review_specialists(&draft.reviewers, specialists, &participants)?;
        let runtime = self
            .runtime()
            .await
            .map_err(|_| ReviewRequestError::Unavailable)?;
        let handle = runtime.mob_handle();
        for (role, identities) in draft.reviewers.bound_roles() {
            for identity in identities {
                let member = handle
                    .get_member(identity)
                    .await
                    .map_err(|_| ReviewRequestError::Unavailable)?
                    .ok_or(ReviewRequestError::IneligibleReviewer)?;
                if &member.role != role {
                    return Err(ReviewRequestError::IneligibleReviewer);
                }
            }
        }
        let store = self.review_requests();
        // Commit the current-request decision before any native admission.
        let intent = tokio::task::spawn_blocking(move || store.reserve(draft, required))
            .await
            .map_err(|_| ReviewRequestError::Unavailable)??;
        let reference = crate::native_review::admit(&handle, &intent, required).await?;
        let store = self.review_requests();
        tokio::task::spawn_blocking(move || {
            store.require_current(&intent)?;
            Ok((intent, reference))
        })
        .await
        .map_err(|_| ReviewRequestError::Unavailable)?
    }

    pub(crate) async fn read_native_review(
        &self,
        candidate: &crate::native_review::ReviewCandidate,
        reference: &crate::native_review::ReviewReference,
    ) -> Result<crate::native_review::VerifiedReview, crate::native_review::ReviewProofError> {
        let store = self.review_requests();
        let candidate_for_request = candidate.clone();
        let current = tokio::task::spawn_blocking(move || store.current(&candidate_for_request))
            .await
            .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?
            .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?
            .ok_or(crate::native_review::ReviewProofError::Superseded)?;
        if &current.candidate != candidate {
            return Err(crate::native_review::ReviewProofError::Superseded);
        }
        let runtime = self
            .runtime()
            .await
            .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?;
        let expected =
            crate::native_review::reference_for(&runtime.mob_handle().definition().id, &current)?;
        if expected.runs.len() != reference.runs.len()
            || expected.runs.iter().collect::<BTreeSet<_>>()
                != reference.runs.iter().collect::<BTreeSet<_>>()
        {
            return Err(crate::native_review::ReviewProofError::Superseded);
        }
        let proof = crate::native_review::read(
            &runtime.mob_handle(),
            candidate,
            self.configured_review_count(&current.reviewers)?,
            reference,
        )
        .await?;
        let store = self.review_requests();
        tokio::task::spawn_blocking(move || {
            if proof.intent() != &current {
                return Err(crate::native_review::ReviewProofError::Superseded);
            }
            store
                .with_current(&current, || proof)
                .map_err(|_| crate::native_review::ReviewProofError::Superseded)
        })
        .await
        .map_err(|_| crate::native_review::ReviewProofError::Unavailable)?
    }

    pub(crate) fn connections(&self) -> Option<&Arc<crate::setup::connections::Connections>> {
        self.meerkat.runtime_connections.as_ref()
    }
    pub(crate) fn auth_binding(&self) -> Option<meerkat_core::AuthBindingRef> {
        self.meerkat.auth_binding.clone()
    }
    pub(crate) fn configure_model(&self, config: &mut meerkat::Config) -> anyhow::Result<()> {
        if let Some(connections) = self.connections() {
            connections.apply_model_config(config, &self.state_path())?;
        }
        Ok(())
    }

    pub(crate) fn repository_binding(&self) -> Option<&crate::config::RepositoryRuntimeBinding> {
        self.meerkat.repository_runtime.as_ref()
    }

    pub(crate) fn repository_definition(&self) -> Result<MobDefinition, String> {
        self.mob_definition()
    }

    pub(crate) fn repository_session_hook(&self) -> Result<Arc<dyn SessionHook>, String> {
        Ok(Arc::new(FugueDirectToolSessionHook {
            connections: self.meerkat.runtime_connections.clone(),
            mob_id: self.meerkat.mob_id().into_owned(),
            agent_profiles: self
                .templates
                .values()
                .flat_map(|template| template.members.iter())
                .map(|member| (member.profile.clone(), member.agent.clone()))
                .collect(),
            default_auth_binding: self.meerkat.auth_binding.clone(),
            review_orchestrator_profile: self.independent_review.orchestrator_profile.clone(),
            independent_review_enabled: self.reviews_enabled(),
            tracker: self.tracker.clone(),
            host_signal_binding: self.host_signal_binding.clone(),
            host_api_url: self.host_api_url.clone(),
            host_api_auth: self.host_api_auth.clone(),
            github_repo: github_repo_from_mcp_servers(&self.repo_mcp_servers)?,
            review_handoff_states: review_handoff_states_from_mcp_servers(&self.repo_mcp_servers),
            action_states: action_states_from_mcp_servers(&self.repo_mcp_servers),
            workgraph_gate: WorkGraphTerminalGate::new(
                self.state_path().join("workgraph.sqlite3"),
                self.tracker.terminal_states.clone(),
                workgraph_blocked_states_from_mcp_servers(&self.repo_mcp_servers),
            )
            .for_mob(self.meerkat.mob_id().into_owned()),
        }))
    }

    /// Borrow the repository's native owner. Never construct a second queue mob.
    pub(crate) async fn attach_repository_runtime(
        &self,
        runtime: Arc<UnifiedRuntime>,
        specialists: Vec<RepositorySpecialist>,
    ) -> Result<(), String> {
        let binding = self
            .repository_binding()
            .ok_or("queue has no repository binding")?;
        if runtime.mob_handle().mob_id().as_str() != binding.mob_id() {
            return Err("repository runtime does not match the queue binding".into());
        }
        let mut current = self.runtime.lock().await;
        if current.is_some() {
            return Err("repository runtime is already attached".into());
        }
        self.specialists
            .set(specialists)
            .map_err(|_| "repository specialists are already attached")?;
        *current = Some(runtime);
        Ok(())
    }
    pub fn new(meerkat: MeerkatConfig) -> Self {
        Self::with_templates(meerkat, BTreeMap::new(), BTreeMap::new())
    }

    pub fn with_templates(
        meerkat: MeerkatConfig,
        repo_mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
        templates: BTreeMap<String, MaterializedMobTemplate>,
    ) -> Self {
        Self::with_config(
            meerkat,
            default_tracker_config(),
            None,
            None,
            repo_mcp_servers,
            templates,
        )
    }

    pub fn with_config(
        meerkat: MeerkatConfig,
        tracker: TrackerConfig,
        host_api_url: Option<String>,
        host_api_auth: Option<MetaConsoleBasicAuthConfig>,
        repo_mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
        templates: BTreeMap<String, MaterializedMobTemplate>,
    ) -> Self {
        Self {
            meerkat,
            repo_mcp_servers,
            tracker,
            host_signal_binding: None,
            host_api_url,
            host_api_auth,
            templates,
            final_review: crate::config::FinalReviewConfig::default(),
            independent_review: IndependentReviewConfig::default(),
            runtime: Arc::new(tokio::sync::Mutex::new(None)),
            specialists: Arc::new(OnceLock::new()),
            console: Arc::new(OnceLock::new()),
            meta_task_runtime: Arc::new(Mutex::new(None)),
            event_drain_task: Arc::new(Mutex::new(None)),
            kapellmeister_bootstrap: Arc::new(tokio::sync::Mutex::new(())),
            jobs: Arc::new(Mutex::new(BTreeMap::new())),
            jobs_loaded: Arc::new(OnceLock::new()),
            stopping: Arc::new(AtomicBool::new(false)),
            launch_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_host_signal_binding(mut self, binding: HostSignalBinding) -> Self {
        self.host_signal_binding = Some(binding);
        self
    }

    pub fn with_repo_mcp_servers(
        meerkat: MeerkatConfig,
        repo_mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
    ) -> Self {
        Self::with_templates(meerkat, repo_mcp_servers, BTreeMap::new())
    }

    pub fn with_final_review_reviewers(
        mut self,
        reviewers: Vec<FinalReviewReviewerConfig>,
    ) -> Self {
        self.final_review.enabled = !reviewers.is_empty();
        self.final_review.reviewers = reviewers;
        self
    }

    pub fn with_final_review_config(mut self, config: crate::config::FinalReviewConfig) -> Self {
        self.final_review = config;
        self
    }

    fn reviews_enabled(&self) -> bool {
        self.independent_review.enabled || self.final_review.enabled
    }

    fn review_runtime_config(&self) -> IndependentReviewConfig {
        let mut config = self.independent_review.clone();
        config.enabled = self.reviews_enabled();
        config
    }

    fn configured_review_count(
        &self,
        reviewers: &meerkat_mob::FlowRoleTargets,
    ) -> Result<usize, crate::native_review::ReviewProofError> {
        crate::native_review::configured_review_count(
            &self.independent_review,
            &self.final_review,
            reviewers,
        )
    }

    pub fn with_independent_review_config(mut self, config: IndependentReviewConfig) -> Self {
        self.independent_review = config;
        self
    }

    pub async fn bootstrap_repo_kapellmeister(&self) -> Result<(), String> {
        let _bootstrap_guard = self.kapellmeister_bootstrap.lock().await;
        let runtime = self.runtime().await?;
        self.ensure_repo_kapellmeister_member(&runtime).await?;
        self.wire_repository_members(&runtime).await
    }

    async fn ensure_repo_kapellmeister_member(
        &self,
        runtime: &UnifiedRuntime,
    ) -> Result<(), String> {
        ensure_member(
            runtime,
            KAPELLMEISTER,
            KAPELLMEISTER,
            Some("You are the repo-scoped Fugue Kapellmeister. Startup rule: do not inspect Linear, call tools, send peer messages, start issue mobs, mutate lifecycle lanes, or infer work from this initial autonomous wake. Reply briefly that you are idle and waiting for explicit Fugue host signals, issue-lead messages, or operator messages. When a Fugue host signal arrives, treat that signal payload as the task context and then coordinate through the provided tools one call at a time."),
            BTreeMap::from([
                ("role".to_string(), KAPELLMEISTER.to_string()),
                ("display_name".to_string(), "Queue coordinator".to_string()),
            ]),
            None,
            None,
            self.meerkat.auth_binding.as_ref(),
        )
        .await
    }

    pub(crate) async fn wire_repository_members(
        &self,
        runtime: &UnifiedRuntime,
    ) -> Result<(), String> {
        let Some(binding) = self.repository_binding() else {
            return Ok(());
        };
        let authority = runtime
            .identity_runtime()
            .ok_or("repository identity runtime is missing")?;
        let identities = self
            .specialists
            .get()
            .into_iter()
            .flatten()
            .map(|specialist| specialist.identity.clone())
            .chain(std::iter::once(format!(
                "{}:representative",
                binding.repository
            )));
        for identity in identities {
            let identity = meerkat_meta::identity_first::AgentIdentity::parse(&identity)
                .map_err(|error| error.to_string())?;
            let status = authority
                .status(&identity)
                .await
                .map_err(|error| error.to_string())?;
            if status.agent_runtime_id.is_none() {
                return Err(format!("specialist {identity} is unavailable"));
            }
        }
        // One repository is one communication group. Reconcile the native
        // roster on bootstrap as well as admission so existing installations
        // gain missing links without replacing members or their sessions.
        let members = runtime.mob_handle().list_members().await;
        for (index, member) in members.iter().enumerate() {
            for peer in &members[index + 1..] {
                wire_pair(
                    runtime,
                    member.agent_identity.as_str(),
                    peer.agent_identity.as_str(),
                )
                .await?;
            }
        }
        Ok(())
    }

    fn repository_collaboration_prompt(&self, prompt: String) -> String {
        if self.repository_binding().is_none() {
            return prompt;
        }
        use std::fmt::Write;
        let mut directory = String::new();
        for specialist in self.specialists.get().into_iter().flatten() {
            let _ = writeln!(
                directory,
                "- {}: {}. {}. Paths: {:?}",
                specialist.identity, specialist.label, specialist.focus, specialist.paths
            );
        }
        format!(
            "{prompt}\n\nRepository specialist directory:\n{directory}\nYou share this repository mob with its persistent domain specialists and representative. Use native peers to discover them. Consult the relevant domain specialist before planning or implementing a change, and include its response as evidence in your handoff. Consult an independent specialist for review where its charter applies. These specialists retain repository knowledge between tickets. After changing their domain, send the affected specialists a change summary, changed paths, tests, and commit SHA. Share the ticket worktree path and commit SHA; specialists can use repository_read_revision to inspect that commit without changing the checkout. Include an uncommitted diff directly when no commit exists yet; do not ask them to mutate the main checkout. WorkGraph is the same shared ledger for this whole repository. If consultation fails, report the blocker rather than silently substituting a generic role."
        )
    }

    pub async fn deliver_repo_kapellmeister_signal(
        &self,
        scope: &crate::host_signal_journal::HostSignalScope,
        signal: &crate::host_signal_journal::PendingSignal,
    ) -> Result<meerkat_contracts::WireContinuationReceipt, String> {
        self.validate_host_signal_scope(scope)?;
        let runtime = self.runtime().await?;
        {
            let _bootstrap_guard = self.kapellmeister_bootstrap.lock().await;
            self.ensure_repo_kapellmeister_member(&runtime).await?;
        }
        let handle = runtime.mob_handle();
        if handle.mob_id().as_str() != scope.mob_id {
            return Err("host signal mob does not match the active native owner".into());
        }
        let mode = match signal.envelope.mode {
            crate::host_signal_journal::SignalHandlingMode::Steer => HandlingMode::Steer,
        };
        // This is the public native admission door also used by Meta's keyed
        // identity sender. Use the original spawn identity for this Core member.
        // Native continuation service owns admission, replay and lifecycle.
        runtime
            .mob_runtime()
            .agent_mob_mcp_state()
            .ok_or("native member continuation service is unavailable")?
            .mob_member_send_keyed(
                handle.mob_id(),
                AgentIdentity::from(scope.member.as_str()),
                signal.envelope.content.clone().into(),
                mode,
                &signal.key.0,
            )
            .await
            .map_err(|error| format!("admit coordinator host signal: {error}"))
    }

    fn validate_host_signal_scope(
        &self,
        scope: &crate::host_signal_journal::HostSignalScope,
    ) -> Result<(), String> {
        let binding = self
            .host_signal_binding
            .as_ref()
            .ok_or("host signal routing is not configured")?;
        if scope != &binding.scope
            || scope.member != KAPELLMEISTER
            || scope.mob_id != self.meerkat.mob_id()
            || scope.runtime_path
                != std::path::absolute(self.state_path()).map_err(|error| error.to_string())?
        {
            return Err("host signal scope does not match the coordinator runtime".into());
        }
        Ok(())
    }

    pub async fn observe_repo_kapellmeister_input(
        &self,
        scope: &crate::host_signal_journal::HostSignalScope,
        receipt: &meerkat_contracts::WireContinuationReceipt,
    ) -> Result<crate::host_signal_journal::NativeInputObservation, String> {
        use crate::host_signal_journal::{InputUnavailable, NativeInputObservation};
        use meerkat_contracts::WireContinuationStatus;
        use meerkat_runtime::{RuntimeDriverError, RuntimeState, SessionServiceRuntimeExt};
        self.validate_host_signal_scope(scope)?;
        let WireContinuationStatus::Admitted {
            session_id,
            input_id,
        } = &receipt.status
        else {
            return Err("input observation requires native admission".into());
        };
        let session =
            meerkat_core::SessionId::parse(session_id).map_err(|error| error.to_string())?;
        let input = uuid::Uuid::parse_str(input_id)
            .map(meerkat_core::lifecycle::InputId)
            .map_err(|error| error.to_string())?;
        let runtime = self.runtime().await?;
        if runtime.mob_handle().mob_id().as_str() != scope.mob_id {
            return Err("host signal mob does not match the active native owner".into());
        }
        let Some(service) = runtime.mob_runtime().session_service() else {
            return Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::MissingSessionService,
            ));
        };
        let Some(machine) = service
            .acquire_runtime_adapter(None)
            .map_err(|error| error.to_string())?
        else {
            return Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::MissingRuntimeAdapter,
            ));
        };
        match machine.input_state(&session, &input).await {
            Ok(Some(state)) => Ok(NativeInputObservation::Present(Box::new(state))),
            Ok(None) => Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::MissingInput,
            )),
            Err(RuntimeDriverError::NotFound { .. }) => Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::MissingRuntime,
            )),
            Err(
                RuntimeDriverError::Destroyed
                | RuntimeDriverError::NotReady {
                    state: RuntimeState::Destroyed,
                },
            ) => Ok(NativeInputObservation::Unresolvable(
                InputUnavailable::DestroyedRuntime,
            )),
            Err(error) => Err(format!("observe native coordinator input: {error}")),
        }
    }

    pub fn use_console(&self, console: Arc<crate::console::ConsoleHub>) -> Result<(), String> {
        self.console
            .set(console)
            .map_err(|_| "console is already attached".into())
    }

    pub async fn reference_console_router<S>(
        &self,
        decision_state: RuntimeDecisionState,
    ) -> Result<Router<S>, String>
    where
        S: Clone + Send + Sync + 'static,
    {
        if let Some(console) = self.console.get() {
            if self.repository_binding().is_none() {
                use sha2::{Digest, Sha256};
                let key = format!(
                    "queue-{:x}",
                    Sha256::digest(self.state_path().as_os_str().as_encoded_bytes())
                );
                console.register(key, self.runtime().await?);
            }
            return Ok(console.router(decision_state).with_state::<S>(()));
        }
        let runtime = self.runtime().await?;
        Ok(runtime
            .build_reference_app_router_with_console_visibility_policy(
                decision_state,
                Arc::new(FugueConsoleVisibilityPolicy),
            )
            .with_state::<S>(()))
    }

    async fn runtime(&self) -> Result<Arc<UnifiedRuntime>, String> {
        let mut guard = self.runtime.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return Err("Fugue runtime is shutting down".to_string());
        }
        if let Some(runtime) = guard.as_ref() {
            return Ok(runtime.clone());
        }

        if self.repository_binding().is_some() {
            return Err("Repository formation has not attached its shared mob yet".into());
        }
        let definition = self.mob_definition()?;
        let state_path = self.state_path();
        let (runtime, meta_task_runtime) = build_unified_runtime(
            definition,
            state_path,
            self.templates
                .values()
                .flat_map(|template| template.members.iter())
                .map(|member| (member.profile.clone(), member.agent.clone()))
                .collect(),
            self.meerkat.auth_binding.clone(),
            self.meerkat.max_sessions,
            self.review_runtime_config(),
            self.repo_mcp_servers.clone(),
            self.tracker.clone(),
            self.host_signal_binding.clone(),
            self.host_api_url.clone(),
            self.host_api_auth.clone(),
        )?;
        let runtime = Arc::new(runtime);
        if let Some(task_runtime) = meta_task_runtime {
            self.meta_task_runtime
                .lock()
                .map_err(|_| "Meta task runtime lock is poisoned".to_string())?
                .replace(task_runtime);
        }
        self.event_drain_task
            .lock()
            .map_err(|_| "Meta event drain task lock is poisoned".to_string())?
            .replace(Arc::clone(&runtime).spawn_event_drain_task());
        *guard = Some(runtime.clone());
        Ok(runtime)
    }

    /// Quiesce the platform before dropping its stores or event subscriptions.
    pub async fn shutdown(&self) -> Result<(), String> {
        self.stopping.store(true, Ordering::Release);
        let tasks = std::mem::take(
            &mut *self
                .launch_tasks
                .lock()
                .map_err(|_| "Fugue launch task registry poisoned".to_string())?,
        );
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        let mut guard = self.runtime.lock().await;
        let Some(runtime) = guard.take() else {
            return Ok(());
        };
        if self.repository_binding().is_some() {
            // Ensemble owns shutdown. The queue only relinquishes its lease.
            return Ok(());
        }
        let report = runtime.shutdown().await;
        let drain_task = self
            .event_drain_task
            .lock()
            .map_err(|_| "Meta event drain task lock is poisoned".to_string())?
            .take();
        if let Some(task) = drain_task {
            task.abort();
            let _ = task.await;
        }
        if !report.cleanup_completed() && !report.cleanup_retained_until_process_exit() {
            return Err(format!("Meta shutdown did not complete: {report:?}"));
        }
        Ok(())
    }

    fn mob_definition(&self) -> Result<MobDefinition, String> {
        let model = self
            .meerkat
            .model
            .as_deref()
            .unwrap_or(default_agent_model());
        let mut definition = MobDefinition::explicit(self.meerkat.mob_id().as_ref());
        definition.limits = Some(LimitsSpec {
            max_orphaned_turns: Some(DEFAULT_NATIVE_MAX_ORPHANED_TURNS),
            ..LimitsSpec::default()
        });
        definition.profiles.insert(
            ProfileName::from(KAPELLMEISTER),
            ProfileBinding::Inline(Box::new(control_profile(
                "Repo-level Fugue coordinator",
                &self.repo_mcp_servers,
                model,
            ))),
        );
        let issue_lead_profile = match self
            .templates
            .values()
            .flat_map(|template| template.members.iter())
            .find(|member| is_issue_lead_member(member))
        {
            Some(member) => agent_profile(&member.agent, &self.repo_mcp_servers)?,
            None => control_profile(
                "Issue-level Fugue coordinator",
                &self.repo_mcp_servers,
                model,
            ),
        };
        definition.profiles.insert(
            ProfileName::from(ISSUE_LEAD),
            ProfileBinding::Inline(Box::new(issue_lead_profile)),
        );
        for template in self.templates.values() {
            for member in &template.members {
                if is_issue_lead_member(member) {
                    continue;
                }
                let profile = agent_profile(&member.agent, &self.repo_mcp_servers)?;
                let name = ProfileName::from(member.profile.as_str());
                if let Some(existing) = definition.profiles.get(&name) {
                    if existing != &ProfileBinding::Inline(Box::new(profile.clone())) {
                        return Err(format!(
                            "conflicting definitions for agent profile {}",
                            member.profile
                        ));
                    }
                } else {
                    definition
                        .profiles
                        .insert(name, ProfileBinding::Inline(Box::new(profile)));
                }
            }
        }
        if self.reviews_enabled() {
            definition
                .profiles
                .entry(ProfileName::from(
                    self.independent_review.orchestrator_profile.as_str(),
                ))
                .or_insert_with(|| {
                    ProfileBinding::Inline(Box::new(independent_review_orchestrator_profile(
                        &self.repo_mcp_servers,
                        model,
                    )))
                });
        }
        Ok(definition)
    }

    fn restore_jobs(&self) -> Result<(), String> {
        self.jobs_loaded
            .get_or_init(|| {
                let path = self.state_path().join("fugue-runs.sqlite3");
                if !path.exists() {
                    return Ok(());
                }
                let connection = rusqlite::Connection::open_with_flags(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(|error| format!("open Fugue run records: {error}"))?;
                let mut query = connection
                    .prepare("SELECT record FROM fugue_runs ORDER BY run_id")
                    .map_err(|error| format!("read Fugue run records: {error}"))?;
                let rows = query
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|error| format!("query Fugue run records: {error}"))?;
                let mut jobs = self
                    .jobs
                    .lock()
                    .map_err(|_| "Fugue run registry poisoned".to_string())?;
                for row in rows {
                    let record = row.map_err(|error| format!("read Fugue run record: {error}"))?;
                    let job: FugueMobJob = serde_json::from_str(&record)
                        .map_err(|error| format!("decode Fugue run record: {error}"))?;
                    jobs.entry(job.run.run_id.0.clone()).or_insert(job);
                }
                Ok(())
            })
            .clone()
    }

    /// Application issue/run mapping only. Native member/session state stays in Meta.
    fn persist_jobs(&self, jobs: &BTreeMap<String, FugueMobJob>) -> Result<(), String> {
        let root = self.state_path();
        std::fs::create_dir_all(&root)
            .map_err(|error| format!("create Fugue run directory: {error}"))?;
        let mut connection = rusqlite::Connection::open(root.join("fugue-runs.sqlite3"))
            .map_err(|error| format!("open Fugue run records: {error}"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|error| format!("set Fugue run store timeout: {error}"))?;
        connection.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS fugue_runs (run_id TEXT PRIMARY KEY, record TEXT NOT NULL);")
            .map_err(|error| format!("initialize Fugue run store: {error}"))?;
        let transaction = connection
            .transaction()
            .map_err(|error| format!("begin Fugue run update: {error}"))?;
        transaction
            .execute("DELETE FROM fugue_runs", [])
            .map_err(|error| format!("replace Fugue run records: {error}"))?;
        for job in jobs.values() {
            let record = serde_json::to_string(job)
                .map_err(|error| format!("encode Fugue run record: {error}"))?;
            transaction
                .execute(
                    "INSERT INTO fugue_runs (run_id, record) VALUES (?1, ?2)",
                    rusqlite::params![job.run.run_id.0, record],
                )
                .map_err(|error| format!("write Fugue run record: {error}"))?;
        }
        transaction
            .commit()
            .map_err(|error| format!("commit Fugue run records: {error}"))
    }

    fn state_path(&self) -> PathBuf {
        meta_runtime_state_path(&self.meerkat)
    }
}

pub fn meta_runtime_state_path(meerkat: &MeerkatConfig) -> PathBuf {
    if let Some(binding) = &meerkat.repository_runtime {
        return binding.state_path.clone();
    }
    meerkat
        .state_root
        .clone()
        .unwrap_or_else(|| PathBuf::from(".fugue/meerkat"))
        .join("mobkit-runtime")
}

#[derive(Clone)]
pub struct MetaRuntimeNativeMobExecutionHost {
    registry: MetaRuntimeRegistry,
}

struct FugueDirectToolSessionHook {
    connections: Option<Arc<crate::setup::connections::Connections>>,
    mob_id: String,
    agent_profiles: BTreeMap<String, NativeAgentProfile>,
    default_auth_binding: Option<meerkat_core::AuthBindingRef>,
    review_orchestrator_profile: String,
    independent_review_enabled: bool,
    tracker: TrackerConfig,
    host_signal_binding: Option<HostSignalBinding>,
    host_api_url: Option<String>,
    host_api_auth: Option<MetaConsoleBasicAuthConfig>,
    github_repo: Option<GitHubRepoRef>,
    review_handoff_states: Vec<String>,
    action_states: Vec<String>,
    workgraph_gate: WorkGraphTerminalGate,
}

#[async_trait]
impl SessionHook for FugueDirectToolSessionHook {
    async fn before_create(&self, req: &mut CreateSessionRequest) -> Result<(), SessionError> {
        let authority = HostToolAuthority::for_mob_binding(
            req.build
                .as_ref()
                .and_then(|build| build.mob_member_binding.as_ref()),
            &self.review_orchestrator_profile,
            &self.mob_id,
        );
        let workspace_root = fugue_worktree_path_from_request(req);
        let profile = req
            .build
            .as_ref()
            .and_then(|build| build.mob_member_binding.as_ref())
            .and_then(|binding| self.agent_profiles.get(&binding.role));
        if let Some(profile) = profile {
            req.max_tokens = profile
                .max_tokens
                .map(|value| {
                    u32::try_from(value)
                        .map_err(|_| SessionError::Unsupported("max_tokens exceeds u32".into()))
                })
                .transpose()?;
            if let Some(prompt) = &profile.system_prompt {
                req.system_prompt = meerkat_core::config::SystemPromptOverride::Set(prompt.clone());
            }
        }
        let build = req.build.get_or_insert_with(SessionBuildOptions::default);
        if let Some(profile) = profile {
            build.structured_output_retries = profile
                .structured_output_retries
                .map(|value| {
                    u32::try_from(value).map_err(|_| {
                        SessionError::Unsupported("structured_output_retries exceeds u32".into())
                    })
                })
                .transpose()?;
            if let Some(instructions) = &profile.additional_instructions {
                let existing = build.additional_instructions.get_or_insert_with(Vec::new);
                for instruction in instructions {
                    if !existing.contains(instruction) {
                        existing.push(instruction.clone());
                    }
                }
            }
            if build.auth_binding.is_none() {
                build.auth_binding = profile.auth_binding.clone();
            }
        }
        if build.auth_binding.is_none() {
            build.auth_binding = self.default_auth_binding.clone();
        }
        build.keep_alive = true;
        if let Some(connections) = &self.connections {
            build
                .shell_env
                .get_or_insert_with(Default::default)
                .extend(connections.github_environment());
        }
        let direct_tools = direct_tool_dispatcher(
            &self.tracker,
            self.host_api_url.clone(),
            self.host_api_auth.clone(),
            authority,
            profile.map(|profile| profile.tools.as_slice()),
            self.github_repo.clone(),
            self.review_handoff_states.clone(),
            self.action_states.clone(),
            self.workgraph_gate.clone(),
            self.independent_review_enabled,
            self.connections.clone(),
            self.host_signal_binding.clone(),
        );
        let existing_tools = build.external_tools.take();
        let fugue_tools = match workspace_root {
            Some(workspace_root) => {
                let builtins =
                    build.override_builtins == meerkat_core::ToolCategoryOverride::Enable;
                let shell = build.override_shell == meerkat_core::ToolCategoryOverride::Enable;
                build.override_builtins = meerkat_core::ToolCategoryOverride::Disable;
                build.override_shell = meerkat_core::ToolCategoryOverride::Disable;
                workspace_bounded_tool_dispatcher(
                    workspace_root.as_path(),
                    direct_tools,
                    builtins,
                    shell,
                    self.connections.as_deref(),
                )?
            }
            None => direct_tools,
        };
        build.external_tools = Some(compose_session_external_tools(existing_tools, fugue_tools));
        Ok(())
    }
}

fn compose_session_external_tools(
    existing_tools: Option<Arc<dyn AgentToolDispatcher>>,
    fugue_tools: Arc<dyn AgentToolDispatcher>,
) -> Arc<dyn AgentToolDispatcher> {
    meerkat_meta::tool_compose::ComposedExternalTools::over(fugue_tools, existing_tools)
}

fn agent_mcp_server_configs(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<McpServerConfig> {
    servers
        .iter()
        .filter(|(name, _)| is_agent_mcp_server(name))
        .filter_map(|(name, server)| mcp_server_config(name, server))
        .collect()
}

fn is_agent_mcp_server(name: &str) -> bool {
    !matches!(name, "linear" | "jira" | "github" | "fugue_host")
}

fn mcp_server_config(name: &str, server: &NativeMcpServerConfig) -> Option<McpServerConfig> {
    if let Some(url) = server.url.as_deref().filter(|url| !url.trim().is_empty()) {
        let mut config = McpServerConfig::streamable_http(name, url, Default::default());
        config.connect_timeout_secs = Some(60);
        return Some(config);
    }

    let (command, args) = server.command.split_first()?;
    let mut config = McpServerConfig::stdio(
        name,
        command,
        args.to_vec(),
        server
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    config.connect_timeout_secs = Some(60);
    Some(config)
}

fn fugue_worktree_path_from_request(req: &CreateSessionRequest) -> Option<PathBuf> {
    req.build
        .as_ref()
        .and_then(|build| build.app_context.as_ref())
        .and_then(|context| context.get(FUGUE_CONTEXT_KEY))
        .and_then(|context| context.get(FUGUE_CONTEXT_WORKTREE_PATH_KEY))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

fn workspace_bounded_tool_dispatcher(
    workspace_root: &Path,
    external: Arc<dyn AgentToolDispatcher>,
    builtins: bool,
    shell: bool,
    connections: Option<&crate::setup::connections::Connections>,
) -> Result<Arc<dyn AgentToolDispatcher>, SessionError> {
    let workspace_root = workspace_root.to_path_buf();
    let shell_config = shell.then(|| {
        let mut config = ShellConfig::with_project_root(workspace_root.clone());
        if let Some(connections) = connections {
            config.env_vars.extend(connections.github_environment());
        }
        config
    });
    let mut policy = ToolPolicyLayer::new().with_mode(if builtins {
        meerkat_tools::ToolMode::AllowAll
    } else {
        meerkat_tools::ToolMode::DenyAll
    });
    if shell {
        for name in [
            "shell",
            "shell_jobs",
            "shell_job_status",
            "shell_job_cancel",
        ] {
            policy = policy.enable_tool(name);
        }
    }
    let config = BuiltinToolConfig {
        policy,
        ..BuiltinToolConfig::default()
    };
    let dispatcher = CompositeDispatcher::new(
        Arc::new(MemoryTaskStore::new()),
        &config,
        Some(workspace_root),
        shell_config,
        Some(external),
        None,
    )
    .map_err(|error| {
        SessionError::Unsupported(format!("build workspace-bounded Fugue tools: {error}"))
    })?;
    Ok(Arc::new(dispatcher))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostToolAuthority {
    Worker,
    IssueLead,
    ReviewOrchestrator,
    Coordinator,
}

impl HostToolAuthority {
    #[cfg(test)]
    fn for_binding(binding: Option<&meerkat_core::MobMemberBinding>, review_profile: &str) -> Self {
        Self::for_mob_binding(binding, review_profile, MOB_ID)
    }

    fn for_mob_binding(
        binding: Option<&meerkat_core::MobMemberBinding>,
        review_profile: &str,
        mob_id: &str,
    ) -> Self {
        let Some(binding) = binding.filter(|binding| binding.mob_id == mob_id) else {
            return Self::Worker;
        };
        match (binding.role.as_str(), binding.member.as_str()) {
            (KAPELLMEISTER, KAPELLMEISTER) => Self::Coordinator,
            (ISSUE_LEAD, member)
                if member.starts_with("issue_") && member.ends_with("__issue_5Flead") =>
            {
                Self::IssueLead
            }
            (role, member)
                if role == review_profile
                    && member.starts_with("issue_")
                    && member.ends_with("__review_5Forchestrator") =>
            {
                Self::ReviewOrchestrator
            }
            _ => Self::Worker,
        }
    }

    fn can_move_issue(self) -> bool {
        matches!(self, Self::Coordinator | Self::IssueLead)
    }

    fn allows_host_tool(self, name: &str) -> bool {
        match name {
            "fugue_admit_independent_review"
            | "fugue_complete_independent_review"
            | "fugue_complete_final_pr_gate" => {
                matches!(self, Self::ReviewOrchestrator)
            }
            "fugue_start_issue_mob"
            | "fugue_start_final_pr_gate"
            | "fugue_start_independent_review"
            | "fugue_run_deploy" => matches!(self, Self::Coordinator),
            _ => false,
        }
    }
}

struct FugueDirectToolDispatcher {
    github: crate::github_tools::GitHubToolHost,
    tracker: TrackerConfig,
    host_signal_binding: Option<HostSignalBinding>,
    host_api_url: Option<String>,
    host_api_auth: Option<MetaConsoleBasicAuthConfig>,
    allow_issue_mutation: bool,
    github_repo: Option<GitHubRepoRef>,
    review_handoff_states: Vec<String>,
    action_states: Vec<String>,
    workgraph_gate: WorkGraphTerminalGate,
    tools: Arc<[Arc<ToolDef>]>,
}

#[async_trait]
impl AgentToolDispatcher for FugueDirectToolDispatcher {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        Arc::clone(&self.tools)
    }

    fn tool_catalog_capabilities(&self) -> meerkat_core::ToolCatalogCapabilities {
        // Every accepted call is present in this immutable, visible catalog.
        meerkat_core::ToolCatalogCapabilities {
            exact_catalog: true,
            may_require_catalog_control_plane: false,
        }
    }

    async fn dispatch(
        &self,
        call: ToolCallView<'_>,
    ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat_core::error::ToolError> {
        if !self.tools.iter().any(|tool| tool.name == call.name) {
            return Err(meerkat_core::error::ToolError::NotFound {
                name: call.name.to_string(),
            });
        }
        let arguments: serde_json::Value =
            serde_json::from_str(call.args.get()).map_err(|error| {
                meerkat_core::error::ToolError::InvalidArguments {
                    name: call.name.to_string(),
                    reason: error.to_string(),
                }
            })?;
        if is_linear_fugue_tool_name(call.name) {
            if linear_tool_mutates_issue_lane(call.name) && !self.allow_issue_mutation {
                return Ok(ToolResult::new(
                    call.id.to_string(),
                    json!({
                        "error": "fugue_tool_forbidden",
                        "message": format!("{name} is only available to issue lead or Kapellmeister sessions", name = call.name)
                    })
                    .to_string(),
                    true,
                )
                .into());
            }
            let lane_move = crate::workgraph_gate::lane_move_for_tool(
                call.name.parse::<FugueToolOperation>().map_err(|error| {
                    meerkat_core::error::ToolError::InvalidArguments {
                        name: call.name.to_owned(),
                        reason: error.to_string(),
                    }
                })?,
                &arguments,
            )
            .map_err(|error| meerkat_core::error::ToolError::InvalidArguments {
                name: call.name.to_string(),
                reason: error.to_string(),
            })?;
            if let Some(input) = lane_move {
                if let Err(message) = guard_direct_public_lane_move(
                    &self.tracker,
                    &self.review_handoff_states,
                    &self.action_states,
                    &input,
                ) {
                    return Ok(ToolResult::new(
                        call.id.to_string(),
                        json!({
                            "error": "noncanonical_public_lane",
                            "message": message
                        })
                        .to_string(),
                        true,
                    )
                    .into());
                }
                if let Err(message) = self
                    .workgraph_gate
                    .guard_terminal_move(&self.tracker, &input)
                    .await
                {
                    return Ok(ToolResult::new(
                        call.id.to_string(),
                        json!({
                            "error": "unresolved_workgraph_obligations",
                            "message": message
                        })
                        .to_string(),
                        true,
                    )
                    .into());
                }
            }
            let (is_error, content) = execute_linear_fugue_tool_bound(
                &self.tracker,
                call.name,
                &arguments,
                &self.review_handoff_states,
                self.host_signal_binding.as_ref(),
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if is_github_fugue_tool_name(call.name) {
            let arguments = apply_direct_github_repo_scope(arguments, self.github_repo.as_ref());
            let (is_error, content, _signals) =
                execute_github_fugue_tool_with_host(call.name, &arguments, &self.github);
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if call.name == "fugue_start_issue_mob" {
            let (is_error, content) = call_fugue_start_issue_mob(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if call.name == "fugue_start_final_pr_gate" {
            let (is_error, content) = call_fugue_start_final_pr_gate(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if call.name == "fugue_start_independent_review" {
            let (is_error, content) = call_fugue_start_independent_review(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if call.name == "fugue_admit_independent_review" {
            let (is_error, content) = call_fugue_admit_independent_review(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if matches!(
            call.name,
            "fugue_complete_independent_review" | "fugue_complete_final_pr_gate"
        ) {
            let endpoint = if call.name == "fugue_complete_final_pr_gate" {
                "complete-final-pr-gate"
            } else {
                "complete-independent-review"
            };
            let (is_error, content) = call_fugue_complete_native_review(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
                endpoint,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        if call.name == "fugue_run_deploy" {
            let (is_error, content) = call_fugue_run_deploy(
                self.host_api_url.as_deref(),
                self.host_api_auth.as_ref(),
                &arguments,
            )
            .await;
            return Ok(ToolResult::new(call.id.to_string(), content, is_error).into());
        }
        Err(meerkat_core::error::ToolError::NotFound {
            name: call.name.to_string(),
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn direct_tool_dispatcher(
    tracker: &TrackerConfig,
    host_api_url: Option<String>,
    host_api_auth: Option<MetaConsoleBasicAuthConfig>,
    authority: HostToolAuthority,
    declared_tools: Option<&[String]>,
    github_repo: Option<GitHubRepoRef>,
    review_handoff_states: Vec<String>,
    action_states: Vec<String>,
    workgraph_gate: WorkGraphTerminalGate,
    independent_review_enabled: bool,
    connections: Option<Arc<crate::setup::connections::Connections>>,
    host_signal_binding: Option<HostSignalBinding>,
) -> Arc<dyn AgentToolDispatcher> {
    let allow_issue_mutation = authority.can_move_issue();
    let tracker = tracker.clone();
    let mut tools = fugue_tool_defs(&tracker, allow_issue_mutation);
    tools.push(Arc::new(ToolDef::new(
        "fugue_start_issue_mob",
        "Ask the Fugue host to start or resume the Meta issue mob for a Linear issue. Use this only when a host observation says an issue needs runtime coordination.",
        json!({
            "type": "object",
            "properties": {
                "issue_identifier": {
                    "type": "string",
                    "description": "Linear issue identifier, for example LUC-123."
                }
            },
            "required": ["issue_identifier"],
            "additionalProperties": false
        }),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_start_final_pr_gate",
        "Ask the Fugue host to start or resume the repo-level final PR gate from a typed FinalPrGateStartRequest. Use only after a final_pr_gate_requested host signal and only as repo Kapellmeister authority.",
        json!({
            "type": "object",
            "description": "Typed FinalPrGateStartRequest payload from the final_pr_gate_requested signal.",
            "additionalProperties": true
        }),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_start_independent_review",
        "Request configured independent review for an open, ready-for-review PR after a Review action or holding observation. Supply the inspected issue, repository, PR number, exact head and stable request key. The host resolves current evidence and supplies its own runtime profile, template and workspace. Only repository Kapellmeister authority may start this operation.",
        crate::independent_review::independent_review_launch_input_schema(),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_admit_independent_review",
        "Request native review by selected existing repository specialists. The host resolves the candidate, reserves the current request and admits native flows. Replay an uncertain response with exactly the same key and assignment; replacing a request requires its current ID in supersedes.",
        crate::independent_review::independent_review_admission_input_schema(),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_complete_independent_review",
        "Submit native review flow references after the selected specialists finish. Fugue reads durable producer results and verifies the current repository, tracker issue and PR head before recording review evidence. Do not submit verdicts or timeout claims. WorkGraph scope must be this repository native realm and default namespace.",
        independent_review_completion_input_schema(),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_complete_final_pr_gate",
        "Finish the configured final review using native run references from the selected existing specialists. The host verifies required profiles, current intent, immutable PR head, CI and WorkGraph before its guarded merge, then projects the actual success/rework/blocked outcome. Caller verdicts are not accepted.",
        independent_review_completion_input_schema(),
    )));
    tools.push(Arc::new(ToolDef::new(
        "fugue_run_deploy",
        "Run the configured guarded merge-only or merge-and-OTA action. Use only as repository authority after explicit action-lane/operator intent. Replay lost responses unchanged. For a failed or uncertain deployment, report the attempt ID and wait for operator inspection and authorization before supplying retry_deployment with a new key.",
        crate::deploy_gate::deploy_start_input_schema(),
    )));
    tools.retain(|tool| {
        if !independent_review_enabled
            && matches!(
                tool.name.as_str(),
                "fugue_start_independent_review"
                    | "fugue_admit_independent_review"
                    | "fugue_complete_independent_review"
                    | "fugue_complete_final_pr_gate"
            )
        {
            return false;
        }
        // Merges must traverse the typed deploy gate, even for the coordinator.
        if tool.name == "github_merge_pr" {
            return false;
        }
        if tool.name == "github_create_pr" && !matches!(authority, HostToolAuthority::Coordinator) {
            return false;
        }
        if tool.name.starts_with("fugue_") {
            return authority.allows_host_tool(&tool.name);
        }
        declared_tools.is_none_or(|declared| {
            if is_linear_fugue_tool_name(&tool.name) {
                declared.iter().any(|name| {
                    name == "mcp:linear" || (tracker.kind == "jira" && name == "mcp:jira")
                })
            } else if is_github_fugue_tool_name(&tool.name) {
                declared.iter().any(|name| name == "mcp:github")
            } else {
                false
            }
        })
    });
    // These tools are in-process host callbacks. Stable source identity lets
    // native delegation preserve the parent's witnessed tool ceiling. Without
    // it, Meerkat correctly refuses to inherit this catalog into a child.
    for tool in &mut tools {
        Arc::make_mut(tool).provenance = Some(meerkat_core::ToolProvenance::new(
            meerkat_core::ToolSourceKind::Callback,
            "fugue-direct-tools",
        ));
    }
    Arc::new(FugueDirectToolDispatcher {
        github: crate::github_tools::GitHubToolHost::with_connections(connections),
        tracker,
        host_signal_binding,
        host_api_url,
        host_api_auth,
        allow_issue_mutation,
        github_repo,
        review_handoff_states,
        action_states,
        workgraph_gate,
        tools: tools.into(),
    })
}

fn apply_direct_github_repo_scope(arguments: Value, repository: Option<&GitHubRepoRef>) -> Value {
    let Some(repository) = repository else {
        return arguments;
    };
    let mut object = match arguments {
        Value::Object(object) => object,
        other => return other,
    };
    object
        .entry("repo".to_string())
        .or_insert_with(|| json!(repository));
    if let Some(host) = &repository.host
        && let Some(repo) = object.get_mut("repo").and_then(Value::as_object_mut)
    {
        repo.entry("host".to_string())
            .or_insert_with(|| json!(host));
    }
    Value::Object(object)
}

fn fugue_tool_defs(config: &TrackerConfig, allow_issue_mutation: bool) -> Vec<Arc<ToolDef>> {
    external_tools_for_tracker(config)
        .unwrap_or_default()
        .into_iter()
        .filter_map(tool_def_from_value)
        .filter(|tool| {
            is_linear_fugue_tool_name(tool.name.as_str())
                || is_github_fugue_tool_name(tool.name.as_str())
        })
        .filter(|tool| allow_issue_mutation || !linear_tool_mutates_issue_lane(tool.name.as_str()))
        .map(Arc::new)
        .collect()
}

fn linear_tool_mutates_issue_lane(name: &str) -> bool {
    matches!(
        name,
        "linear_move_issue" | "linear_claim_issue" | "linear_delete_issue"
    )
}

fn tool_def_from_value(value: Value) -> Option<ToolDef> {
    let object = value.as_object()?;
    let name = object.get("name")?.as_str()?;
    let description = object
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let input_schema = object
        .get("input_schema")
        .cloned()
        .unwrap_or_else(|| json!({ "type": "object" }));
    Some(ToolDef::new(
        name.to_string(),
        description.to_string(),
        input_schema,
    ))
}

fn default_tracker_config() -> TrackerConfig {
    TrackerConfig {
        jira: None,
        kind: "linear".to_string(),
        endpoint: "https://api.linear.app/graphql".to_string(),
        api_key: std::env::var("LINEAR_API_KEY")
            .ok()
            .filter(|value| !value.is_empty()),
        project_slug: std::env::var("LINEAR_PROJECT_SLUG").ok(),
        intake_states: env_list("FUGUE_LINEAR_INTAKE_STATES"),
        in_flight_states: env_list("FUGUE_LINEAR_IN_FLIGHT_STATES"),
        active_states: env_list("FUGUE_LINEAR_ACTIVE_STATES"),
        attachment_root: None,
        max_attachment_files: env_u64("FUGUE_LINEAR_MAX_ATTACHMENT_FILES")
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(20),
        max_attachment_bytes: env_u64("FUGUE_LINEAR_MAX_ATTACHMENT_BYTES")
            .unwrap_or(25 * 1024 * 1024),
        max_attachment_total_bytes: env_u64("FUGUE_LINEAR_MAX_ATTACHMENT_TOTAL_BYTES")
            .unwrap_or(100 * 1024 * 1024),
        terminal_states: env_list("FUGUE_LINEAR_TERMINAL_STATES"),
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .map(|value| resolve_env_value(&value))
        .and_then(|value| value.parse::<u64>().ok())
}

fn env_list(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|value| resolve_env_value(&value))
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_env_value(value: &str) -> String {
    if let Some(name) = value.strip_prefix('$') {
        std::env::var(name).unwrap_or_default()
    } else {
        value.to_string()
    }
}

fn fugue_host_api_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(FUGUE_HOST_API_CONNECT_TIMEOUT)
        .timeout(FUGUE_HOST_API_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| format!("build Fugue host API HTTP client: {error}"))
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn build_unified_runtime(
    definition: MobDefinition,
    state_path: PathBuf,
    agent_profiles: BTreeMap<String, NativeAgentProfile>,
    default_auth_binding: Option<meerkat_core::AuthBindingRef>,
    max_sessions: usize,
    independent_review: IndependentReviewConfig,
    servers: BTreeMap<String, NativeMcpServerConfig>,
    tracker: TrackerConfig,
    host_signal_binding: Option<HostSignalBinding>,
    host_api_url: Option<String>,
    host_api_auth: Option<MetaConsoleBasicAuthConfig>,
) -> Result<(UnifiedRuntime, Option<tokio::runtime::Runtime>), String> {
    let build = move || async move {
        let github_repo = github_repo_from_mcp_servers(&servers)?;
        let review_handoff_states = review_handoff_states_from_mcp_servers(&servers);
        let action_states = action_states_from_mcp_servers(&servers);
        let workgraph_gate = WorkGraphTerminalGate::new(
            state_path.join("workgraph.sqlite3"),
            tracker.terminal_states.clone(),
            workgraph_blocked_states_from_mcp_servers(&servers),
        );
        std::fs::create_dir_all(&state_path)
            .map_err(|error| format!("create Meta state directory: {error}"))?;
        let (mob_storage, provenance) =
            meerkat_meta::mob_composition_manifest::persistent_mob_storage(
                state_path.join("mob.sqlite3"),
            )
            .map_err(|error| format!("open durable Fugue mob: {error}"))?;
        if let Some(previous) = mob_storage
            .created_definition_snapshot()
            .await
            .map_err(|error| format!("read durable Fugue definition: {error}"))?
            && let Some(migrated) = migrate_duplicate_jira_mcp(previous.definition(), &definition)
        {
            mob_storage
                .update_definition(previous.epoch(), migrated)
                .await
                .map_err(|error| format!("remove duplicate Jira MCP registration: {error}"))?;
        }
        let builder = UnifiedRuntime::builder()
            .definition(definition)
            .mob_storage(mob_storage, provenance)
            .persistent_state(state_path)
            .require_incremental_session_persistence(true)
            .session_hook(Arc::new(FugueDirectToolSessionHook {
                connections: None,
                mob_id: MOB_ID.to_owned(),
                agent_profiles,
                default_auth_binding,
                review_orchestrator_profile: independent_review.orchestrator_profile,
                independent_review_enabled: independent_review.enabled,
                tracker,
                host_signal_binding,
                host_api_url,
                host_api_auth,
                github_repo,
                review_handoff_states,
                action_states,
                workgraph_gate,
            }))
            .timeout(Duration::from_secs(30))
            .max_sessions(max_sessions);
        #[cfg(test)]
        let builder = builder.default_llm_client(tests::runtime_test_llm_client());
        builder
            .build()
            .await
            .map_err(|err| format!("bootstrap Meta runtime: {err}"))
    };
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|_| "Meta runtime build requires a Tokio runtime".to_string())?;
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
        return tokio::task::block_in_place(|| handle.block_on(build()))
            .map(|runtime| (runtime, None));
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .map_err(|err| format!("create Meta task runtime: {err}"))?;
    let unified = runtime.block_on(build())?;
    Ok((unified, Some(runtime)))
}

/// Upgrade only the host-owned duplicate adapter registration. Unrelated
/// configuration drift must still be rejected by the native composition gate.
fn migrate_duplicate_jira_mcp(
    previous: &MobDefinition,
    desired: &MobDefinition,
) -> Option<MobDefinition> {
    let mut migrated = previous.clone();
    for binding in migrated.profiles.values_mut() {
        if let ProfileBinding::Inline(profile) = binding {
            profile
                .tools
                .mcp_servers
                .retain(|server| server.name != "jira");
        }
    }
    (previous != desired && &migrated == desired).then_some(migrated)
}

#[derive(Deserialize)]
struct StartIssueMobArgs {
    issue_identifier: String,
}

async fn call_fugue_start_issue_mob(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
) -> (bool, String) {
    let args = match serde_json::from_value::<StartIssueMobArgs>(arguments.clone()) {
        Ok(args) if !args.issue_identifier.trim().is_empty() => args,
        Ok(_) => {
            return (
                true,
                json!({
                    "error": "invalid_arguments",
                    "message": "issue_identifier must not be empty"
                })
                .to_string(),
            );
        }
        Err(error) => {
            return (
                true,
                json!({
                    "error": "invalid_arguments",
                    "message": error.to_string()
                })
                .to_string(),
            );
        }
    };
    let Some(host_api_url) = host_api_url.filter(|url| !url.trim().is_empty()) else {
        return (
            true,
            json!({
                "error": "fugue_host_unconfigured",
                "message": "fugue_start_issue_mob requires server.port or an explicit Fugue host API URL"
            })
            .to_string(),
        );
    };
    let url = format!(
        "{}/api/v1/kapellmeister/start-issue",
        host_api_url.trim_end_matches('/')
    );
    let body = json!({ "issue_identifier": args.issue_identifier.trim() });
    let client = match fugue_host_api_client() {
        Ok(client) => client,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "fugue_host_client_error",
                    "message": error
                })
                .to_string(),
            );
        }
    };
    let mut request = client.post(url).json(&body);
    if let Some(auth) = host_api_auth {
        let username = match std::env::var(&auth.username_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console username env {} is missing or empty", auth.username_env)
                    })
                    .to_string(),
                );
            }
        };
        let password = match std::env::var(&auth.password_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console password env {} is missing or empty", auth.password_env)
                    })
                    .to_string(),
                );
            }
        };
        request = request.basic_auth(username, Some(password));
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(text) if status.is_success() => (false, text),
                Ok(text) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "body": text
                    })
                    .to_string(),
                ),
                Err(error) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "message": error.to_string()
                    })
                    .to_string(),
                ),
            }
        }
        Err(error) => (
            true,
            json!({
                "error": "fugue_host_unreachable",
                "message": error.to_string()
            })
            .to_string(),
        ),
    }
}

async fn call_fugue_start_final_pr_gate(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
) -> (bool, String) {
    let request = match serde_json::from_value::<FinalPrGateStartRequest>(arguments.clone()) {
        Ok(request) => request,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "invalid_arguments",
                    "message": error.to_string()
                })
                .to_string(),
            );
        }
    };
    let Some(host_api_url) = host_api_url.filter(|url| !url.trim().is_empty()) else {
        return (
            true,
            json!({
                "error": "fugue_host_unconfigured",
                "message": "fugue_start_final_pr_gate requires server.port or an explicit Fugue host API URL"
            })
            .to_string(),
        );
    };
    let url = format!(
        "{}/api/v1/kapellmeister/start-final-pr-gate",
        host_api_url.trim_end_matches('/')
    );
    let client = match fugue_host_api_client() {
        Ok(client) => client,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "fugue_host_client_error",
                    "message": error
                })
                .to_string(),
            );
        }
    };
    let mut http_request = client.post(url).json(&request);
    if let Some(auth) = host_api_auth {
        let username = match std::env::var(&auth.username_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console username env {} is missing or empty", auth.username_env)
                    })
                    .to_string(),
                );
            }
        };
        let password = match std::env::var(&auth.password_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console password env {} is missing or empty", auth.password_env)
                    })
                    .to_string(),
                );
            }
        };
        http_request = http_request.basic_auth(username, Some(password));
    }
    match http_request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(text) if status.is_success() => (false, text),
                Ok(text) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "body": text
                    })
                    .to_string(),
                ),
                Err(error) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "message": error.to_string()
                    })
                    .to_string(),
                ),
            }
        }
        Err(error) => (
            true,
            json!({
                "error": "fugue_host_unreachable",
                "message": error.to_string()
            })
            .to_string(),
        ),
    }
}

async fn call_fugue_start_independent_review(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
) -> (bool, String) {
    let request = match serde_json::from_value::<IndependentReviewLaunchRequest>(arguments.clone())
    {
        Ok(request) => request,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "invalid_arguments",
                    "message": error.to_string()
                })
                .to_string(),
            );
        }
    };
    call_fugue_review_request(
        host_api_url,
        host_api_auth,
        "start-independent-review",
        &request,
    )
    .await
}

async fn call_fugue_admit_independent_review(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
) -> (bool, String) {
    let request = match serde_json::from_value::<
        crate::independent_review::IndependentReviewAdmissionRequest,
    >(arguments.clone())
    {
        Ok(request) => request,
        Err(_) => {
            return (
                true,
                json!({"error":"invalid_review_assignment"}).to_string(),
            );
        }
    };
    call_fugue_review_request(
        host_api_url,
        host_api_auth,
        "admit-independent-review",
        &request,
    )
    .await
}

async fn call_fugue_complete_native_review(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
    endpoint: &str,
) -> (bool, String) {
    let request =
        match serde_json::from_value::<IndependentReviewCompletionRequest>(arguments.clone()) {
            Ok(request) => request,
            Err(error) => {
                return (
                    true,
                    json!({
                        "error": "invalid_arguments",
                        "message": error.to_string()
                    })
                    .to_string(),
                );
            }
        };
    call_fugue_review_request(host_api_url, host_api_auth, endpoint, &request).await
}

async fn call_fugue_review_request<T: Serialize>(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    endpoint: &str,
    request: &T,
) -> (bool, String) {
    let Some(host_api_url) = host_api_url.filter(|url| !url.trim().is_empty()) else {
        return (
            true,
            json!({
                "error": "fugue_host_unconfigured",
                "message": "Review operations require server.port or an explicit Fugue host API URL"
            })
            .to_string(),
        );
    };
    let url = format!(
        "{}/api/v1/kapellmeister/{endpoint}",
        host_api_url.trim_end_matches('/')
    );
    let client = match fugue_host_api_client() {
        Ok(client) => client,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "fugue_host_client_error",
                    "message": error
                })
                .to_string(),
            );
        }
    };
    let mut http_request = client.post(url).json(&request);
    if let Some(auth) = host_api_auth {
        let username = match std::env::var(&auth.username_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console username env {} is missing or empty", auth.username_env)
                    })
                    .to_string(),
                );
            }
        };
        let password = match std::env::var(&auth.password_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console password env {} is missing or empty", auth.password_env)
                    })
                    .to_string(),
                );
            }
        };
        http_request = http_request.basic_auth(username, Some(password));
    }
    match http_request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(text)
                    if status.is_success()
                        && crate::independent_review::completion_acknowledged(&text) =>
                {
                    (false, text)
                }
                Ok(text) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "body": text
                    })
                    .to_string(),
                ),
                Err(error) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "message": error.to_string()
                    })
                    .to_string(),
                ),
            }
        }
        Err(error) => (
            true,
            json!({
                "error": "fugue_host_unreachable",
                "message": error.to_string()
            })
            .to_string(),
        ),
    }
}

async fn call_fugue_run_deploy(
    host_api_url: Option<&str>,
    host_api_auth: Option<&MetaConsoleBasicAuthConfig>,
    arguments: &Value,
) -> (bool, String) {
    let request = match serde_json::from_value::<DeployStartRequest>(arguments.clone()) {
        Ok(request) => request,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "invalid_arguments",
                    "message": error.to_string()
                })
                .to_string(),
            );
        }
    };
    let Some(host_api_url) = host_api_url.filter(|url| !url.trim().is_empty()) else {
        return (
            true,
            json!({
                "error": "fugue_host_unconfigured",
                "message": "fugue_run_deploy requires server.port or an explicit Fugue host API URL"
            })
            .to_string(),
        );
    };
    let url = format!(
        "{}/api/v1/kapellmeister/deploy",
        host_api_url.trim_end_matches('/')
    );
    let client = match fugue_host_api_client() {
        Ok(client) => client,
        Err(error) => {
            return (
                true,
                json!({
                    "error": "fugue_host_client_error",
                    "message": error
                })
                .to_string(),
            );
        }
    };
    let mut http_request = client.post(url).json(&request);
    if let Some(auth) = host_api_auth {
        let username = match std::env::var(&auth.username_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console username env {} is missing or empty", auth.username_env)
                    })
                    .to_string(),
                );
            }
        };
        let password = match std::env::var(&auth.password_env) {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                return (
                    true,
                    json!({
                        "error": "fugue_host_auth_unavailable",
                        "message": format!("configured console password env {} is missing or empty", auth.password_env)
                    })
                    .to_string(),
                );
            }
        };
        http_request = http_request.basic_auth(username, Some(password));
    }
    match http_request.send().await {
        Ok(response) => {
            let status = response.status();
            match response.text().await {
                Ok(text) if status.is_success() => (false, text),
                Ok(text) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "body": text
                    })
                    .to_string(),
                ),
                Err(error) => (
                    true,
                    json!({
                        "error": "fugue_host_error",
                        "status": status.as_u16(),
                        "message": error.to_string()
                    })
                    .to_string(),
                ),
            }
        }
        Err(error) => (
            true,
            json!({
                "error": "fugue_host_unreachable",
                "message": error.to_string()
            })
            .to_string(),
        ),
    }
}

impl MetaRuntimeNativeMobExecutionHost {
    pub fn new(meerkat: MeerkatConfig) -> Self {
        Self::from_registry(MetaRuntimeRegistry::new(meerkat))
    }

    pub fn from_registry(registry: MetaRuntimeRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> MetaRuntimeRegistry {
        self.registry.clone()
    }
}

#[async_trait]
impl NativeMobExecutionHost for MetaRuntimeNativeMobExecutionHost {
    fn restore_persisted_runtimes(
        &self,
        _runtime_keys: Vec<String>,
        _max_successes: usize,
    ) -> Result<(), String> {
        self.registry.restore_jobs()?;
        let jobs = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Fugue run registry poisoned".to_string())?
            .values()
            .filter(|job| meta_runtime_run_status_is_active(job.run.status))
            .cloned()
            .collect::<Vec<_>>();
        block_on(async {
            let runtime = self.registry.runtime().await?;
            for job in jobs {
                for identity in &job.member_identities {
                    let member = runtime
                        .mob_handle()
                        .get_member(&AgentIdentity::from(identity.as_str()))
                        .await
                        .map_err(|error| {
                            format!("verify recovered native member {identity}: {error}")
                        })?;
                    if member.is_none() {
                        return Err(format!(
                            "Fugue run {} has an incomplete native launch: member {identity} is absent; preserve its workspace and requeue the issue after inspection",
                            job.run.run_id.0
                        ));
                    }
                }
            }
            Ok(())
        })?
    }

    fn list_runs(&self, input: ListRunsInput) -> FugueToolResult {
        if let Err(message) = self.registry.restore_jobs() {
            return err(FugueToolOperation::MobListRuns, &message);
        }
        match self.registry.jobs.lock() {
            Ok(jobs) => {
                if input.limit == 0 {
                    return err(
                        FugueToolOperation::MobListRuns,
                        "list limit must be positive",
                    );
                }
                let mut matching = jobs
                    .values()
                    .map(|job| &job.run)
                    .filter(|run| input.status.is_none_or(|status| run.status == status))
                    .filter(|run| {
                        input
                            .issue
                            .as_ref()
                            .is_none_or(|issue| run.issue.as_ref() == Some(issue))
                    })
                    .filter(|run| {
                        input
                            .cursor
                            .as_ref()
                            .is_none_or(|cursor| run.run_id.0 > cursor.0)
                    });
                let runs = matching
                    .by_ref()
                    .take(usize::from(input.limit))
                    .cloned()
                    .collect::<Vec<_>>();
                let next_cursor = matching
                    .next()
                    .and_then(|_| runs.last().map(|run| Cursor(run.run_id.0.clone())));
                ok(FugueToolOutput::MobListRuns(ListRunsOutput {
                    runs,
                    next_cursor,
                }))
            }
            Err(_) => err(
                FugueToolOperation::MobListRuns,
                "Meta run registry lock is poisoned",
            ),
        }
    }

    fn status(&self, input: InspectRunInput) -> FugueToolResult {
        if let Err(message) = self.registry.restore_jobs() {
            return err(FugueToolOperation::MobInspectRun, &message);
        }
        match self.registry.jobs.lock() {
            Ok(jobs) => match jobs.get(&input.run_id.0) {
                Some(job) => ok(FugueToolOutput::MobInspectRun(
                    crate::tools::InspectRunOutput {
                        run: job.run.clone(),
                        launch_failure: job.launch_failure.clone(),
                    },
                )),
                None => err(
                    FugueToolOperation::MobInspectRun,
                    "native mob run not found",
                ),
            },
            Err(_) => err(
                FugueToolOperation::MobInspectRun,
                "Meta run registry lock is poisoned",
            ),
        }
    }

    fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult {
        err_with_key(
            FugueToolOperation::MobSpawnOrResumeExecution,
            "Meta launch requires NativeMobLaunchPlan",
            Some(input.idempotency_key),
        )
    }

    fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
        if let Err(message) = self.registry.restore_jobs() {
            return err(FugueToolOperation::MobSpawnOrResumeExecution, &message);
        }
        if self.registry.stopping.load(Ordering::Acquire) {
            return err(
                FugueToolOperation::MobSpawnOrResumeExecution,
                "Fugue runtime is shutting down",
            );
        }
        let run_id = MobRunId(plan.input.idempotency_key.0.clone());
        let now = Utc::now();
        let (run, spawned, resumed) = match self.registry.jobs.lock() {
            Ok(mut jobs) => {
                if let Some(existing) = jobs.get_mut(&run_id.0)
                    && meta_runtime_run_status_is_active(existing.run.status)
                {
                    existing.run.updated_at = now;
                    (existing.run.clone(), false, true)
                } else {
                    let run = meta_runtime_run_snapshot_from_plan(&run_id, &plan, now);
                    jobs.insert(
                        run_id.0.clone(),
                        FugueMobJob::new(run.clone(), member_identities_for_plan(&plan)),
                    );
                    (run, true, false)
                }
            }
            Err(_) => {
                return err(
                    FugueToolOperation::MobSpawnOrResumeExecution,
                    "Meta run registry lock is poisoned",
                );
            }
        };

        match self.registry.jobs.lock() {
            Ok(jobs) => {
                if let Err(message) = self.registry.persist_jobs(&jobs) {
                    return err(FugueToolOperation::MobSpawnOrResumeExecution, &message);
                }
            }
            Err(_) => {
                return err(
                    FugueToolOperation::MobSpawnOrResumeExecution,
                    "Fugue run registry poisoned",
                );
            }
        }
        if spawned {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let host = self.clone();
                    let task = handle.spawn(async move {
                        if let Err(message) = host
                            .complete_spawn_or_resume_plan_async(plan, run_id.clone())
                            .await
                        {
                            warn!(
                                run_id = %run_id.0,
                                error = %message,
                                "Meta runtime issue launch failed"
                            );
                            if let Err(error) = host.set_launch_failure(&run_id, message.clone()) {
                                warn!(%error, "persist failed Fugue launch");
                            }
                        }
                    });
                    match self.registry.launch_tasks.lock() {
                        Ok(mut tasks) => {
                            if self.registry.stopping.load(Ordering::Acquire) {
                                task.abort();
                                return err(
                                    FugueToolOperation::MobSpawnOrResumeExecution,
                                    "Fugue runtime is shutting down",
                                );
                            }
                            tasks.retain(|task| !task.is_finished());
                            tasks.push(task);
                        }
                        Err(_) => {
                            task.abort();
                            return err(
                                FugueToolOperation::MobSpawnOrResumeExecution,
                                "Fugue launch task registry poisoned",
                            );
                        }
                    }
                }
                Err(_) => {
                    if let Err(message) =
                        block_on(self.complete_spawn_or_resume_plan_async(plan, run_id.clone()))
                            .and_then(|result| result)
                    {
                        if let Err(error) = self.set_launch_failure(&run_id, message.clone()) {
                            return err(FugueToolOperation::MobSpawnOrResumeExecution, &error);
                        }
                        return err(FugueToolOperation::MobSpawnOrResumeExecution, &message);
                    }
                }
            }
        }
        ok(FugueToolOutput::MobSpawnOrResumeExecution(
            SpawnOrResumeExecutionOutput {
                run,
                spawned,
                resumed,
            },
        ))
    }

    fn cancel_execution(&self, input: CancelExecutionInput) -> FugueToolResult {
        if let Err(message) = self.registry.restore_jobs() {
            return err(FugueToolOperation::MobCancelExecution, &message);
        }
        let job = match self.registry.jobs.lock() {
            Ok(jobs) => jobs.get(&input.run_id.0).cloned(),
            Err(_) => {
                return err(
                    FugueToolOperation::MobCancelExecution,
                    "Meta run registry lock is poisoned",
                );
            }
        };
        let Some(job) = job else {
            return err(
                FugueToolOperation::MobCancelExecution,
                "native mob run not found",
            );
        };
        let mut run = job.run.clone();
        run.status = MobRunStatus::Cancelled;
        run.completed_at = Some(Utc::now());
        run.updated_at = Utc::now();
        if let Err(message) = block_on(self.retire_run_members(&job)).and_then(|result| result) {
            return err(FugueToolOperation::MobCancelExecution, &message);
        }
        if let Err(message) = self.remove_job(&input.run_id) {
            return err(FugueToolOperation::MobCancelExecution, &message);
        }
        ok(FugueToolOutput::MobCancelExecution(
            crate::tools::CancelExecutionOutput {
                run,
                cancelled: true,
            },
        ))
    }

    fn cleanup_execution(&self, run_id: &MobRunId, _reason: &str) -> Result<usize, String> {
        self.registry.restore_jobs()?;
        let job = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Meta run registry lock is poisoned".to_string())?
            .get(&run_id.0)
            .cloned();
        let Some(job) = job else {
            return Ok(0);
        };
        let retired = block_on(self.retire_run_members(&job))??;
        self.remove_job(run_id)?;
        Ok(retired)
    }

    fn observe_structural_events(&self, input: ObserveStructuralEventsInput) -> FugueToolResult {
        match block_on(self.observe_meta_structural_events(input)) {
            Ok(Ok(output)) => ok(FugueToolOutput::MobObserveEvents(output)),
            Ok(Err(message)) => err(FugueToolOperation::MobObserveEvents, &message),
            Err(message) => err(FugueToolOperation::MobObserveEvents, &message),
        }
    }

    fn notify_issue_lead_member_turn_limit(
        &self,
        issue_identifier: &str,
        _run_id: &MobRunId,
        member_identity: &str,
        detail: &str,
    ) -> Result<(), String> {
        let issue_lead = native_member_identity(issue_identifier, ISSUE_LEAD);
        let content = turn_limit_issue_lead_message(issue_identifier, member_identity, detail);
        let _receipt = block_on(async {
            let runtime = self.registry.runtime().await?;
            runtime
                .mob_handle()
                .member(&AgentIdentity::from(issue_lead.as_str()))
                .await
                .map_err(|err| format!("resolve issue lead after turn limit: {err}"))?
                .send(content, HandlingMode::Steer)
                .await
                .map_err(|err| format!("deliver turn-limit notice to issue lead: {err}"))
        })??;
        Ok(())
    }

    fn notify_issue_lead_member_terminal_failure(
        &self,
        issue_identifier: &str,
        _run_id: &MobRunId,
        member_identity: &str,
        detail: &str,
    ) -> Result<(), String> {
        let issue_lead = native_member_identity(issue_identifier, ISSUE_LEAD);
        let content =
            member_terminal_failure_issue_lead_message(issue_identifier, member_identity, detail);
        let _receipt = block_on(async {
            let runtime = self.registry.runtime().await?;
            runtime
                .mob_handle()
                .member(&AgentIdentity::from(issue_lead.as_str()))
                .await
                .map_err(|err| format!("resolve issue lead after member terminal failure: {err}"))?
                .send(content, HandlingMode::Steer)
                .await
                .map_err(|err| {
                    format!("deliver member terminal-failure notice to issue lead: {err}")
                })
        })??;
        Ok(())
    }
}

fn turn_limit_issue_lead_message(issue: &str, member_identity: &str, detail: &str) -> String {
    format!(
        "Fugue runtime observed a member turn-limit failure for {issue}.\n\n\
Member: {member_identity}\n\
Detail: {detail}\n\n\
This notification is a fresh issue-lead turn, so the runtime delivery counter is reset. \
Decide the next issue-level action: steer the member to continue from the current workspace, \
ask planner/reviewer to re-scope, move the issue to Blocked for operator help, or advance the \
pipeline if the workspace and Workpad evidence are already sufficient. Do not discard existing \
workspace changes without explicit operator instruction."
    )
}

fn member_terminal_failure_issue_lead_message(
    issue: &str,
    member_identity: &str,
    detail: &str,
) -> String {
    format!(
        "Fugue runtime observed a member terminal turn failure for {issue}.\n\n\
Member: {member_identity}\n\
Detail: {detail}\n\n\
This is not evidence that the issue work is complete. Decide the next issue-level action: \
steer or respawn the failed member to continue from the current workspace, ask planner/reviewer \
to re-scope, move the issue to Blocked for operator help, or advance the pipeline only if the \
workspace and Workpad evidence are already sufficient. Preserve existing workspace changes unless \
the operator explicitly instructs otherwise."
    )
}

impl MetaRuntimeNativeMobExecutionHost {
    fn remove_job(&self, run_id: &MobRunId) -> Result<(), String> {
        let mut jobs = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Fugue run registry poisoned".to_string())?;
        let mut remaining = jobs.clone();
        remaining.remove(&run_id.0);
        self.registry.persist_jobs(&remaining)?;
        *jobs = remaining;
        Ok(())
    }

    async fn retire_run_members(&self, job: &FugueMobJob) -> Result<usize, String> {
        let runtime = self.registry.runtime().await?;
        let handle = runtime.mob_handle();
        let mut retired = 0;
        for identity in &job.member_identities {
            match handle.retire(AgentIdentity::from(identity.as_str())).await {
                Ok(()) => retired += 1,
                Err(meerkat_mob::MobError::MemberNotFound(_)) => {}
                Err(err) => return Err(format!("retire Meta member {identity}: {err}")),
            }
        }
        Ok(retired)
    }

    async fn complete_spawn_or_resume_plan_async(
        &self,
        plan: NativeMobLaunchPlan,
        run_id: MobRunId,
    ) -> Result<(), String> {
        self.registry.bootstrap_repo_kapellmeister().await?;
        let runtime = self.registry.runtime().await?;
        let issue = issue_key(&plan.input.issue);
        let issue_lead = native_member_identity(&issue, ISSUE_LEAD);
        ensure_member(
            &runtime,
            ISSUE_LEAD,
            &issue_lead,
            None,
            member_labels(&issue, ISSUE_LEAD),
            Some(
                self.registry
                    .repository_collaboration_prompt(issue_lead_prompt_for_tracker(
                        &issue,
                        plan.input.instructions.as_deref(),
                        &self.registry.tracker,
                        plan.template.linear_projection.as_ref(),
                    )),
            ),
            plan.input.worktree_path.as_deref(),
            plan.template
                .members
                .iter()
                .find(|member| is_issue_lead_member(member))
                .and_then(|member| member.agent.auth_binding.as_ref())
                .or(self.registry.meerkat.auth_binding.as_ref()),
        )
        .await?;
        wire_pair(&runtime, KAPELLMEISTER, &issue_lead).await?;
        self.registry.wire_repository_members(&runtime).await?;

        for member in plan
            .template
            .members
            .iter()
            .filter(|member| !is_issue_lead_member(member))
        {
            let identity = native_member_identity(&issue, &member.name);
            let initial_message = member_initial_message_for_plan(&plan, &member.name);
            ensure_member(
                &runtime,
                &member.profile,
                &identity,
                initial_message.as_deref(),
                member_labels(&issue, &member.name),
                Some(
                    self.registry
                        .repository_collaboration_prompt(worker_prompt_with_lane(
                            &issue,
                            &member.name,
                            plan.input.instructions.as_deref(),
                            running_lane(
                                &self.registry.tracker,
                                plan.template.linear_projection.as_ref(),
                            ),
                        )),
                ),
                plan.input.worktree_path.as_deref(),
                member
                    .agent
                    .auth_binding
                    .as_ref()
                    .or(self.registry.meerkat.auth_binding.as_ref()),
            )
            .await?;
            wire_pair(&runtime, &issue_lead, &identity).await?;
            self.registry.wire_repository_members(&runtime).await?;
        }
        wire_template_members(&runtime, &issue, &plan.template).await?;
        self.set_run_status(&run_id, MobRunStatus::Running)?;

        match tokio::time::timeout(
            Duration::from_secs(15),
            runtime
                .mob_handle()
                .member(&AgentIdentity::from(issue_lead.as_str()))
                .await
                .map_err(|err| format!("resolve issue lead: {err}"))?
                .send(issue_lead_task(&plan), HandlingMode::Steer),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return Err(format!("issue lead task delivery failed: {err}")),
            Err(_) => {
                warn!(
                    run_id = %run_id.0,
                    issue = %issue,
                    "issue lead task delivery still pending after 15s"
                );
            }
        }
        Ok(())
    }

    fn set_launch_failure(&self, run_id: &MobRunId, message: String) -> Result<(), String> {
        self.update_run_status(run_id, MobRunStatus::Failed, Some(message))
    }

    fn set_run_status(&self, run_id: &MobRunId, status: MobRunStatus) -> Result<(), String> {
        self.update_run_status(run_id, status, None)
    }

    fn update_run_status(
        &self,
        run_id: &MobRunId,
        status: MobRunStatus,
        launch_error: Option<String>,
    ) -> Result<(), String> {
        let mut jobs = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Fugue run registry poisoned".to_string())?;
        let mut updated = jobs.clone();
        let Some(job) = updated.get_mut(&run_id.0) else {
            return Err(format!("unknown Fugue run {}", run_id.0));
        };
        job.run.status = status;
        job.run.updated_at = Utc::now();
        job.launch_failure = launch_error.map(|message| crate::tools::NativeLaunchFailure {
            occurred_at: job.run.updated_at,
            message,
        });
        if matches!(
            status,
            MobRunStatus::Succeeded | MobRunStatus::Failed | MobRunStatus::Cancelled
        ) {
            job.run.completed_at = Some(job.run.updated_at);
        }
        self.registry.persist_jobs(&updated)?;
        *jobs = updated;
        Ok(())
    }

    async fn observe_meta_structural_events(
        &self,
        input: ObserveStructuralEventsInput,
    ) -> Result<crate::tools::ObserveEventsOutput, String> {
        let job = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Meta run registry lock is poisoned".to_string())?
            .get(&input.run_id.0)
            .cloned();
        let Some(job) = job else {
            return Ok(crate::tools::ObserveEventsOutput {
                events: Vec::new(),
                next_cursor: input.cursor.as_ref().map(|cursor| cursor.0.clone()),
            });
        };
        let after_seq = input
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.0.0.parse::<u64>().ok());
        let runtime = self.registry.runtime().await?;
        let envelopes = tokio::time::timeout(
            META_EVENT_QUERY_TIMEOUT,
            runtime.query_mob_events(&EventQuery {
                mob_id: Some(self.registry.meerkat.mob_id().into_owned()),
                after_seq,
                limit: Some(usize::from(input.limit)),
                ..EventQuery::default()
            }),
        )
        .await
        .map_err(|_| {
            format!(
                "query Meta structural events timed out after {}s",
                META_EVENT_QUERY_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| format!("query Meta structural events: {error}"))?;
        let mut next_cursor = input.cursor.as_ref().map(|cursor| cursor.0.clone());
        let mut events = Vec::new();
        for envelope in envelopes {
            next_cursor = Some(Cursor(envelope.cursor.to_string()));
            if !job_matches_structural_event(&job, &envelope) {
                continue;
            }
            if let Some(event) = structural_event_to_snapshot(&envelope) {
                events.push(event);
            }
        }
        self.refresh_run_progress_from_console(&runtime, &input.run_id, &job)
            .await?;
        events.extend(
            self.unreported_member_failure_events(&runtime, &input.run_id, &job)
                .await?,
        );
        Ok(crate::tools::ObserveEventsOutput {
            events,
            next_cursor,
        })
    }

    async fn refresh_run_progress_from_console(
        &self,
        runtime: &UnifiedRuntime,
        run_id: &MobRunId,
        job: &FugueMobJob,
    ) -> Result<(), String> {
        let console_store = runtime.console_log_store();
        let mut latest: Option<chrono::DateTime<Utc>> = None;
        for identity in &job.member_identities {
            let page = console_store
                .query_windowed_frames(ConsoleTimelineWindowQuery {
                    identity: Some(identity.clone()),
                    mode: ConsoleTimelineMode::Recent,
                    limit: 1,
                    ..ConsoleTimelineWindowQuery::default()
                })
                .await
                .map_err(|err| format!("query latest Meta console frame for {identity}: {err}"))?;
            for frame in page.frames {
                if let Some(observed_at) = i64::try_from(frame.timestamp_ms)
                    .ok()
                    .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                {
                    latest = Some(latest.map_or(observed_at, |current| current.max(observed_at)));
                }
            }
        }
        let Some(latest) = latest else {
            return Ok(());
        };
        let mut jobs = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Meta run registry lock is poisoned".to_string())?;
        if let Some(job) = jobs.get_mut(&run_id.0)
            && latest > job.run.updated_at
        {
            job.run.updated_at = latest;
        }
        Ok(())
    }

    async fn unreported_member_failure_events(
        &self,
        runtime: &UnifiedRuntime,
        run_id: &MobRunId,
        job: &FugueMobJob,
    ) -> Result<Vec<MobEventSnapshot>, String> {
        let console_store = runtime.console_log_store();
        let mut candidates = Vec::new();
        for identity in &job.member_identities {
            let mut after = None;
            for _ in 0..20 {
                let page = console_store
                    .query_frames(ConsoleTimelineQuery {
                        identity: Some(identity.clone()),
                        after: after.clone(),
                        limit: 500,
                        ..ConsoleTimelineQuery::default()
                    })
                    .await
                    .map_err(|err| format!("query Meta console timeline for {identity}: {err}"))?;
                if page.frames.is_empty() {
                    break;
                }
                after = page.next_cursor.clone();
                for frame in page.frames {
                    let Some(failure) = console_frame_member_failure(&frame) else {
                        continue;
                    };
                    let cursor = frame.cursor.as_str().to_string();
                    if job.notified_member_failure_cursors.contains(&cursor) {
                        continue;
                    }
                    let snapshot = MobEventSnapshot {
                        sequence: console_cursor_sequence(&cursor),
                        occurred_at: i64::try_from(frame.timestamp_ms)
                            .ok()
                            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                            .unwrap_or_else(Utc::now),
                        kind: MobEventKind::MemberKickoffUpdated {
                            member: identity.clone(),
                            phase: failure.phase().to_string(),
                        },
                        message: Some(failure.notification_detail(identity, &frame.payload)),
                    };
                    candidates.push((cursor, snapshot));
                }
                if after.is_none() {
                    break;
                }
            }
        }
        self.persist_unreported_member_failure_events(run_id, candidates)
    }

    fn persist_unreported_member_failure_events(
        &self,
        run_id: &MobRunId,
        candidates: Vec<(String, MobEventSnapshot)>,
    ) -> Result<Vec<MobEventSnapshot>, String> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let mut jobs = self
            .registry
            .jobs
            .lock()
            .map_err(|_| "Meta run registry lock is poisoned".to_string())?;
        let mut updated = jobs.clone();
        let Some(job) = updated.get_mut(&run_id.0) else {
            return Ok(Vec::new());
        };
        let mut snapshots = Vec::new();
        for (cursor, snapshot) in candidates {
            if job.notified_member_failure_cursors.insert(cursor) {
                snapshots.push(snapshot);
            }
        }
        if !snapshots.is_empty() {
            // Finish every console query before marking anything reported.
            // Persist before exposing notifications; a failed write leaves the
            // live registry unchanged so the next observation can retry.
            self.registry.persist_jobs(&updated)?;
            *jobs = updated;
        }
        snapshots.sort_by_key(|event| event.sequence);
        Ok(snapshots)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberFailureKind {
    TurnLimit,
    LlmFailure,
    TerminalFailure,
}

impl MemberFailureKind {
    fn phase(self) -> &'static str {
        match self {
            Self::TurnLimit => TURN_LIMIT_PHASE,
            Self::LlmFailure | Self::TerminalFailure => MEMBER_TERMINAL_FAILURE_PHASE,
        }
    }

    fn notification_detail(self, identity: &str, payload: &Value) -> String {
        match self {
            Self::TurnLimit => turn_limit_notification_detail(identity, payload),
            Self::LlmFailure | Self::TerminalFailure => {
                terminal_failure_notification_detail(identity, payload)
            }
        }
    }
}

fn console_cursor_sequence(cursor: &str) -> u64 {
    cursor
        .strip_prefix("console:")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1_000_000_000)
}

#[cfg(test)]
fn console_frame_is_turn_limit_failure(frame: &meerkat_meta::ConsoleFrame) -> bool {
    console_frame_member_failure(frame) == Some(MemberFailureKind::TurnLimit)
}

fn console_frame_member_failure(frame: &meerkat_meta::ConsoleFrame) -> Option<MemberFailureKind> {
    if frame.kind != "interaction_failed" {
        return None;
    }
    let text = frame.payload.to_string();
    if text.contains("turn_limit_reached")
        || text.contains("TurnLimitReached")
        || text.contains("\"error_class\":\"max_turns\"")
        || text.contains("\"terminal_cause_kind\":\"turn_limit_reached\"")
    {
        return Some(MemberFailureKind::TurnLimit);
    }
    let normalized = text.to_ascii_lowercase();
    if normalized.contains("llmfailure")
        || normalized.contains("llm failure terminal turn")
        || normalized.contains("\"terminal_cause_kind\":\"llm_failure\"")
    {
        return Some(MemberFailureKind::LlmFailure);
    }
    if normalized.contains("terminal failure") || normalized.contains("terminal_cause_kind") {
        return Some(MemberFailureKind::TerminalFailure);
    }
    None
}

fn turn_limit_notification_detail(identity: &str, payload: &Value) -> String {
    let error = payload
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("turn limit reached");
    format!("{identity} hit the autonomous turn limit: {error}")
}

fn terminal_failure_notification_detail(identity: &str, payload: &Value) -> String {
    let error = payload
        .get("error")
        .or_else(|| payload.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("terminal interaction failure");
    format!("{identity} had a terminal runtime turn failure: {error}")
}

fn meta_runtime_run_snapshot_from_plan(
    run_id: &MobRunId,
    plan: &NativeMobLaunchPlan,
    now: chrono::DateTime<Utc>,
) -> MobRunSnapshot {
    MobRunSnapshot {
        run_id: run_id.clone(),
        meta_flow_run_id: None,
        flow_id: plan
            .template
            .flow
            .map(|flow| flow.canonical_name().to_string()),
        issue: Some(plan.input.issue.clone()),
        workspace_key: Some(plan.input.workspace_key.clone()),
        template_id: Some(plan.input.template_id.clone()),
        status: MobRunStatus::Running,
        started_at: Some(now),
        updated_at: now,
        completed_at: None,
    }
}

/// Native comms components permit only ASCII alphanumerics, '-' and '_'.
/// Escape underscores too so distinct caller inputs cannot alias one identity.
fn native_member_identity(issue: &str, member: &str) -> String {
    use std::fmt::Write;
    let mut identity = String::from("issue_");
    for (index, component) in [issue, member].into_iter().enumerate() {
        if index != 0 {
            identity.push_str("__");
        }
        for byte in component.bytes() {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                identity.push(char::from(byte));
            } else {
                // Writing into a String cannot fail.
                let _ = write!(identity, "_{byte:02X}");
            }
        }
    }
    identity
}

fn member_identities_for_plan(plan: &NativeMobLaunchPlan) -> Vec<String> {
    let issue = issue_key(&plan.input.issue);
    let mut identities = Vec::with_capacity(plan.template.members.len().saturating_add(1));
    identities.push(native_member_identity(&issue, ISSUE_LEAD));
    identities.extend(
        plan.template
            .members
            .iter()
            .filter(|member| !is_issue_lead_member(member))
            .map(|member| native_member_identity(&issue, &member.name)),
    );
    identities
}

fn job_matches_structural_event(job: &FugueMobJob, envelope: &MobStructuralEventEnvelope) -> bool {
    if envelope.agent_identity.as_ref().is_some_and(|identity| {
        job.member_identities
            .iter()
            .any(|member| member == identity)
    }) {
        return true;
    }
    if let Some(envelope_run_id) = &envelope.run_id {
        return Some(envelope_run_id) == job.run.meta_flow_run_id.as_ref()
            || envelope_run_id == &job.run.run_id.0;
    }
    false
}

fn structural_event_to_snapshot(envelope: &MobStructuralEventEnvelope) -> Option<MobEventSnapshot> {
    let kind = match envelope.kind.as_str() {
        "member_spawned" => {
            let member = envelope.agent_identity.as_ref()?.clone();
            MobEventKind::MemberStarted { member }
        }
        "member_retired" | "member_reset" => {
            let member = envelope.agent_identity.as_ref()?.clone();
            MobEventKind::MemberCompleted { member }
        }
        "member_kickoff_updated" => {
            let member = envelope.agent_identity.as_ref()?.clone();
            let phase = envelope
                .data
                .get("phase")
                .and_then(Value::as_str)
                .unwrap_or(envelope.kind.as_str())
                .to_string();
            MobEventKind::MemberKickoffUpdated { member, phase }
        }
        "flow_started" => MobEventKind::RunStarted,
        "flow_completed" => MobEventKind::RunCompleted {
            status: MobRunStatus::Succeeded,
            structured_output: Some(envelope.data.clone()),
        },
        "flow_failed" => MobEventKind::RunCompleted {
            status: MobRunStatus::Failed,
            structured_output: Some(envelope.data.clone()),
        },
        "flow_canceled" => MobEventKind::RunCompleted {
            status: MobRunStatus::Cancelled,
            structured_output: Some(envelope.data.clone()),
        },
        _ => return None,
    };
    let occurred_at = i64::try_from(envelope.timestamp_ms)
        .ok()
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now);
    Some(MobEventSnapshot {
        sequence: envelope.cursor,
        occurred_at,
        kind,
        message: Some(envelope.kind.clone()),
    })
}

fn meta_runtime_run_status_is_active(status: MobRunStatus) -> bool {
    matches!(
        status,
        MobRunStatus::Pending | MobRunStatus::Running | MobRunStatus::WaitingForInput
    )
}

fn control_profile(
    description: &str,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
    model: &str,
) -> Profile {
    Profile {
        // Persisted mob membership supplies restart continuity; do not opt
        // into a second durable-worker discovery contract for these profiles.
        durable: false,
        model_fallback: None,
        provider: None,
        self_hosted_server_id: None,
        image_generation_provider: None,
        auto_compact_threshold: None,
        resume_overrides: Vec::new(),
        model: model.to_string(),
        skills: Vec::new(),
        tools: ToolConfig {
            builtins: false,
            comms: true,
            mob: false,
            workgraph: true,
            mcp_servers: agent_mcp_server_configs(mcp_servers),
            ..ToolConfig::default()
        },
        peer_description: description.to_string(),
        external_addressable: true,
        backend: None,
        runtime_mode: MobRuntimeMode::AutonomousHost,
        max_inline_peer_notifications: None,
        output_schema: None,
        provider_params: None,
    }
}

fn agent_profile(
    agent: &NativeAgentProfile,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Result<Profile, String> {
    Ok(Profile {
        durable: false,
        model_fallback: None,
        provider: agent
            .provider
            .as_ref()
            .map(|provider| {
                serde_json::from_value::<meerkat_core::Provider>(Value::String(provider.clone()))
                    .map_err(|error| format!("invalid agent provider {provider:?}: {error}"))
            })
            .transpose()?,
        self_hosted_server_id: None,
        image_generation_provider: None,
        auto_compact_threshold: None,
        resume_overrides: Vec::new(),
        model: agent
            .model
            .clone()
            .unwrap_or_else(|| default_agent_model().to_string()),
        skills: agent.skills.clone(),
        tools: ToolConfig {
            builtins: agent
                .tools
                .iter()
                .any(|tool| matches!(tool.as_str(), "builtins" | "git" | "shell")),
            shell: agent.tools.iter().any(|tool| tool == "shell"),
            comms: true,
            memory: agent.tools.iter().any(|tool| tool == "memory"),
            mob: agent.tools.iter().any(|tool| tool == "mob"),
            workgraph: true,
            mcp_servers: agent_mcp_servers(agent, mcp_servers)
                .iter()
                .filter(|name| is_agent_mcp_server(name))
                .filter_map(|name| {
                    mcp_servers
                        .get(name)
                        .and_then(|server| mcp_server_config(name, server))
                })
                .collect(),
            ..ToolConfig::default()
        },
        peer_description: String::new(),
        external_addressable: true,
        backend: None,
        runtime_mode: MobRuntimeMode::AutonomousHost,
        max_inline_peer_notifications: None,
        output_schema: agent
            .output_schema
            .clone()
            .map(|schema| {
                serde_json::from_value(Value::Object(schema))
                    .map_err(|error| format!("invalid agent output schema: {error}"))
            })
            .transpose()?,
        provider_params: agent
            .provider_params
            .clone()
            .map(|params| {
                serde_json::from_value(Value::Object(params))
                    .map_err(|error| format!("invalid agent provider parameters: {error}"))
            })
            .transpose()?,
    })
}

fn agent_mcp_servers(
    agent: &NativeAgentProfile,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<String> {
    let mut visible = agent
        .tools
        .iter()
        .filter_map(|tool| tool.strip_prefix("mcp:"))
        .filter(|server| mcp_servers.contains_key(*server))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if mcp_servers.contains_key("playwright")
        && !visible.iter().any(|server| server == "playwright")
    {
        visible.push("playwright".to_string());
    }
    visible
}

fn independent_review_orchestrator_profile(
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
    model: &str,
) -> Profile {
    Profile {
        durable: false,
        model_fallback: None,
        provider: None,
        self_hosted_server_id: None,
        image_generation_provider: None,
        auto_compact_threshold: None,
        resume_overrides: Vec::new(),
        model: model.to_string(),
        skills: Vec::new(),
        tools: ToolConfig {
            builtins: true,
            shell: true,
            comms: true,
            mob: true,
            workgraph: true,
            mcp_servers: agent_mcp_server_configs(mcp_servers),
            ..ToolConfig::default()
        },
        peer_description: "Fugue independent review orchestrator".to_string(),
        external_addressable: true,
        backend: None,
        runtime_mode: MobRuntimeMode::AutonomousHost,
        max_inline_peer_notifications: None,
        output_schema: None,
        provider_params: None,
    }
}

fn github_repo_from_mcp_servers(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Result<Option<GitHubRepoRef>, String> {
    let Some(github) = servers.get("github") else {
        return Ok(None);
    };
    let host = server_env_value(github, "FUGUE_GITHUB_HOST")
        .or_else(|| server_env_value(github, "GH_HOST"))
        .map(crate::tools::GitHubHost::try_from)
        .transpose()?;
    let (Some(owner), Some(name)) = (
        server_env_value(github, "FUGUE_GITHUB_OWNER"),
        server_env_value(github, "FUGUE_GITHUB_REPO"),
    ) else {
        return Ok(None);
    };
    Ok(Some(GitHubRepoRef {
        host,
        owner: GitHubOwner(owner),
        name: GitHubRepoName(name),
    }))
}

fn server_env_value(server: &NativeMcpServerConfig, key: &str) -> Option<String> {
    server
        .env
        .get(key)
        .map(|value| resolve_env_value(value))
        .filter(|value| !value.trim().is_empty())
}

fn review_handoff_states_from_mcp_servers(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<String> {
    states_from_linear_mcp_env(servers, LINEAR_REVIEW_HANDOFF_STATES_ENV)
}

fn action_states_from_mcp_servers(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<String> {
    states_from_linear_mcp_env(servers, LINEAR_ACTION_STATES_ENV)
}

fn states_from_linear_mcp_env(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
    env_name: &str,
) -> Vec<String> {
    let Some(value) = servers
        .iter()
        .find(|(name, server)| is_native_linear_mcp_server(name, server))
        .and_then(|(_, server)| server_env_value(server, env_name))
    else {
        return Vec::new();
    };
    let mut states = Vec::<String>::new();
    for state in value
        .split(',')
        .map(str::trim)
        .filter(|state| !state.is_empty())
    {
        if !states
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(state))
        {
            states.push(state.to_string());
        }
    }
    states
}

fn workgraph_blocked_states_from_mcp_servers(
    servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> Vec<String> {
    states_from_linear_mcp_env(servers, WORKGRAPH_BLOCKED_STATES_ENV)
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
async fn ensure_member(
    runtime: &UnifiedRuntime,
    profile: &str,
    identity: &str,
    initial_message: Option<&str>,
    labels: BTreeMap<String, String>,
    additional_instructions: Option<String>,
    worktree_path: Option<&str>,
    auth_binding: Option<&meerkat_core::AuthBindingRef>,
) -> Result<(), String> {
    let mut spec = SpawnMemberSpec::new(ProfileName::from(profile), AgentIdentity::from(identity))
        .with_runtime_mode(MobRuntimeMode::AutonomousHost)
        .with_labels(labels);
    if let Some(binding) = auth_binding {
        spec = spec.with_auth_binding(binding.clone());
    }
    if let Some(message) = initial_message {
        spec = spec.with_initial_message(message.to_string());
    }
    if let Some(instructions) = additional_instructions {
        spec.additional_instructions = Some(vec![instructions]);
    }
    if let Some(worktree_path) = worktree_path.map(str::trim).filter(|path| !path.is_empty()) {
        spec = spec.with_context(json!({
            FUGUE_CONTEXT_KEY: {
                FUGUE_CONTEXT_WORKTREE_PATH_KEY: worktree_path,
            }
        }));
        let mut env = std::collections::HashMap::new();
        env.insert("FUGUE_WORKTREE_PATH".to_string(), worktree_path.to_string());
        spec = spec.with_shell_env(env);
    }
    runtime
        .mob_handle()
        .ensure_member(spec)
        .await
        .map_err(|err| format!("ensure Meta member {identity}: {err}"))?;
    Ok(())
}

async fn wire_pair(runtime: &UnifiedRuntime, a: &str, b: &str) -> Result<(), String> {
    let handle = runtime.mob_handle();
    wire_one_with_retry(&handle, a, b).await?;
    wire_one_with_retry(&handle, b, a).await?;
    Ok(())
}

async fn wire_one_with_retry(handle: &MobHandle, from: &str, to: &str) -> Result<(), String> {
    let mut last_error = None;
    for _ in 0..10 {
        match handle
            .wire(AgentIdentity::from(from), AgentIdentity::from(to))
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) if err.to_string().contains("peer not found") => {
                last_error = Some(err.to_string());
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => return Err(format!("wire {from} -> {to}: {err}")),
        }
    }
    Err(format!(
        "wire {from} -> {to}: {}",
        last_error.unwrap_or_else(|| "peer was not visible after retry".to_string())
    ))
}

async fn wire_template_members(
    runtime: &UnifiedRuntime,
    issue: &str,
    template: &MaterializedMobTemplate,
) -> Result<(), String> {
    if !matches!(
        template.flow,
        Some(NativeMobFlow::PlannerCoderReviewer | NativeMobFlow::PlannerCoderReviewerRework)
            | None
    ) {
        return Ok(());
    }
    let members = template
        .members
        .iter()
        .filter(|member| !is_issue_lead_member(member))
        .map(|member| native_member_identity(issue, &member.name))
        .collect::<Vec<_>>();
    for i in 0..members.len() {
        for j in (i + 1)..members.len() {
            wire_pair(runtime, &members[i], &members[j]).await?;
        }
    }
    Ok(())
}

fn is_issue_lead_member(member: &crate::config::MaterializedMobMember) -> bool {
    member.name == ISSUE_LEAD || member.profile == ISSUE_LEAD
}

fn issue_key(issue: &LinearIssueRef) -> String {
    match issue {
        LinearIssueRef::Identifier { identifier } => identifier.0.clone(),
        LinearIssueRef::Id { id } => id.0.clone(),
    }
}

fn member_labels(issue: &str, role: &str) -> BTreeMap<String, String> {
    let label = match role {
        ISSUE_LEAD => "Issue lead",
        "planner" => "Planner",
        "coder" => "Coder",
        "reviewer" => "Reviewer",
        other => other,
    };
    BTreeMap::from([
        ("issue".to_string(), issue.to_string()),
        ("role".to_string(), role.to_string()),
        ("display_name".to_string(), format!("{issue} / {label}")),
    ])
}

fn running_lane<'a>(
    tracker: &'a crate::config::TrackerConfig,
    projection: Option<&'a crate::config::MaterializedLinearProjection>,
) -> &'a str {
    projection
        .and_then(|projection| projection.aggregate.fallback.as_deref())
        .or_else(|| tracker.in_flight_states.first().map(String::as_str))
        .unwrap_or("the configured running lane")
}

fn issue_lead_prompt_for_tracker(
    issue: &str,
    instructions: Option<&str>,
    tracker: &crate::config::TrackerConfig,
    projection: Option<&crate::config::MaterializedLinearProjection>,
) -> String {
    if tracker.kind != "jira" {
        return issue_lead_prompt(issue, instructions);
    }
    let active = running_lane(tracker, projection);
    let handoff = projection
        .map(|projection| projection.terminal_lanes.succeeded.as_str())
        .unwrap_or("the configured development handoff lane");
    format!(
        "You are the Fugue issue lead for {issue}. Coordinate visible worker peers through Meta comms. You own issue-level Jira lane projection: inspect the issue, update the Workpad, and use linear_move_issue with the configured Jira status names. Kapellmeister owns repo-level startup/resume/retire coordination. Once intake is accepted, project the issue to {active}. Record internal planner/coder/reviewer/remediation phases in labels, WorkGraph, Workpad, and Meta evidence. Follow the template's configured node and terminal projections; never invent tracker statuses or enter operator action lanes from inside the issue mob. Successful development projects to {handoff} after its issue-team review and implementation obligations are satisfied. The issue lead owns this development handoff projection; Kapellmeister coordinates the downstream repository review. When the configured handoff also requests repository independent review, entering it does not require the result of the subsequent repository review. Resolve current implementation findings first. Do not close future review or merge obligations to permit this handoff. This shared holding status requests review and is not terminal acceptance or review approval. A configured review holding lane may be entered for this handoff; other operator action lanes remain forbidden. Do not substitute the tracker completion lane for this projection. Merging requires a separate explicit action and verified merge completion follows its configured success projection. Red review, failed checks, or actionable operator findings must create or update WorkGraph blockers and keep or return the issue to {active} for focused replanning. Use labels `fugue`, `issue:{issue}`, and `blocking`. Deleted comments do not clear obligations without explicit newer acceptance. Use the default WorkGraph scope: omit realm_id and namespace unless following an exact returned item reference; never invent a realm or namespace from the issue identifier. Planner/coder may claim, update, and add evidence, but only reviewer-role agents may close completed review obligations. Before terminal success, query WorkGraph yourself and verify every blocking `issue:{issue}` item is terminal. Execute actual transition calls before reporting lane movement complete.\n\n{}",
        instructions.unwrap_or_default()
    )
}

fn issue_lead_prompt(issue: &str, instructions: Option<&str>) -> String {
    format!(
        "You are the Fugue issue lead for {issue}. Coordinate the visible worker peers through Meta comms. You own issue-level IMS/Linear lane projection for this issue: inspect the issue, update the Workpad, and call linear_move_issue when the issue's coarse operator lane should change. Kapellmeister owns repo-level startup/resume/retire coordination; do not wait for Kapellmeister to approve ordinary issue-mob projection such as Queue, Active, Blocked, Done, or Canceled when your mob has the evidence to move. Queue is the intake lane, and Active is the single running issue-mob lane; once an intake or legacy remediation signal has been accepted by the mob, project the issue to Active and record detailed planner/coder/reviewer/remediation phase as labels, WorkGraph, Workpad, and Meta runtime evidence. Review, Deploy, and final-review lanes are action lanes: never move an issue there from inside the issue mob; they are operator or host-observed control inputs. Done means accepted development work, not merged PR; merge/OTA is a separate Deploy action. Red review, failed checks, operator review findings, or concrete blocking remediation should create/update WorkGraph blockers and keep or return the issue to Active for focused replanning; do not create separate tracker statuses for internal phases. If Kapellmeister reports an issue comment add/update/delete, operator_review_finding, PR finding, check failure, or concrete blocking remediation, convert the actionable finding into WorkGraph items with labels `fugue`, `issue:{issue}`, and `blocking` before replanning; deleted comments are not acceptance unless a newer operator comment explicitly clears the finding. For normal issue-mob WorkGraph queries, use the default WorkGraph scope: omit realm_id and namespace unless you are following an exact returned item reference; never invent a realm or namespace from the issue identifier. Planner/coder may claim, update, and add evidence to WorkGraph items, but reviewer-role agents are the only agents allowed to close completed review obligations. Before moving this issue to a non-Blocked terminal success/cancel/duplicate lane, query WorkGraph yourself and ensure every blocking `issue:{issue}` item is terminal; the linear_move_issue tool will reject terminal success while unresolved obligations remain. If your task names a sequence of issue lanes, execute the actual linear_move_issue calls for each lane in that sequence before you report the sequence complete; do not merely announce future lane movement.\n\n{}",
        instructions.unwrap_or_default()
    )
}

#[cfg(test)]
fn worker_prompt(issue: &str, member: &str, instructions: Option<&str>) -> String {
    worker_prompt_with_lane(issue, member, instructions, "Active")
}

fn worker_prompt_with_lane(
    issue: &str,
    member: &str,
    instructions: Option<&str>,
    active: &str,
) -> String {
    let role_contract = match member {
        "reviewer" => format!(
            "Reviewer contract for {issue}: query WorkGraph items labeled `fugue`, `issue:{issue}`, and `blocking` before deciding green/red. Use the default WorkGraph scope: omit realm_id and namespace unless an exact item reference gives you a scope; never invent a realm or namespace from the issue identifier. For every review pass, use a fresh neutral review helper through native fork_off with message_count: 0. The empty transcript avoids inheriting earlier conclusions while the native fork preserves your issue workspace and WorkGraph scope. Plain delegate inherits tool visibility but does not preserve this issue workspace. Give the child strict neutral instructions to verify its working directory first, independently inspect the diff against base with shell/GitHub tools, and query WorkGraph itself; it must not delegate further, change code, close obligations, or move tracker lanes. Do not hand it a precomputed diff, curated findings, or a WorkGraph dump. Await its native result, record the evidence, and retire it. A workspace refusal blocks review; never work around it. If review is green, close only the completed WorkGraph obligations with workgraph_close after verifying evidence; if red, create or update blocking WorkGraph items and ask the issue lead to keep or project the issue to {active} for focused replanning/remediation with phase evidence."
        ),
        "planner" => format!(
            "Planner contract for {issue}: inspect WorkGraph items labeled `fugue`, `issue:{issue}`, and `blocking`, turn open obligations into concrete Workpad plan items, and add WorkGraph evidence as planning clarifies scope. Use the default WorkGraph scope: omit realm_id and namespace unless an exact item reference gives you a scope; never invent a realm or namespace from the issue identifier. Do not close WorkGraph review obligations."
        ),
        "coder" => format!(
            "Coder contract for {issue}: implement the current Workpad plan and open WorkGraph obligations labeled `fugue`, `issue:{issue}`, and `blocking`; claim/update/add evidence as work completes. Use the default WorkGraph scope: omit realm_id and namespace unless an exact item reference gives you a scope; never invent a realm or namespace from the issue identifier. Do not close WorkGraph review obligations."
        ),
        _ => format!(
            "WorkGraph contract for {issue}: use labels `fugue`, `issue:{issue}`, and `blocking` for durable review obligations. Use the default WorkGraph scope: omit realm_id and namespace unless an exact item reference gives you a scope; never invent a realm or namespace from the issue identifier. Only reviewer-role agents may close completed review obligations."
        ),
    };
    format!(
        "You are {member} for Fugue issue {issue}. Coordinate with the issue lead and other visible peers through Meta comms. Work only on your role's bounded contribution.\n\n{role_contract}\n\n{}",
        instructions.unwrap_or_default()
    )
}

fn issue_lead_task(plan: &NativeMobLaunchPlan) -> String {
    format!(
        "Fugue has started or resumed this issue mob. Coordinate the worker peers and report progress to Kapellmeister.\n\nIssue: {}\nWorkspace: {}\nTemplate: {}\n\n{}",
        issue_key(&plan.input.issue),
        plan.input
            .worktree_path
            .as_deref()
            .unwrap_or("<unknown workspace>"),
        plan.input.template_id.0,
        plan.input.instructions.as_deref().unwrap_or_default()
    )
}

fn member_initial_message_for_plan(plan: &NativeMobLaunchPlan, member: &str) -> Option<String> {
    if !plan.input.template_id.0.starts_with("final-pr-gate-") {
        return None;
    }
    Some(format!(
        "Start repo-level final review coordination now.\n\nIssue: {}\nCoordinator: {}\nWorkspace: {}\nTemplate: {}\n\nSelect existing specialists, admit native review, then call fugue_complete_final_pr_gate with native run references. Do not merge directly.\n\n{}",
        issue_key(&plan.input.issue),
        member,
        plan.input
            .worktree_path
            .as_deref()
            .unwrap_or("<unknown workspace>"),
        plan.input.template_id.0,
        plan.input.instructions.as_deref().unwrap_or_default()
    ))
}

fn ok(output: FugueToolOutput) -> FugueToolResult {
    FugueToolResult::Ok {
        output,
        metadata: ToolExecutionMetadata::default(),
    }
}

fn err(operation: FugueToolOperation, message: &str) -> FugueToolResult {
    err_with_key(operation, message, None)
}

fn err_with_key(
    operation: FugueToolOperation,
    message: &str,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
) -> FugueToolResult {
    let now = Utc::now();
    FugueToolResult::Err {
        error: crate::tools::FugueToolError {
            operation,
            code: FugueToolErrorCode::BackendUnavailable,
            message: message.to_string(),
            retry: RetryAdvice::SafeToRetry,
            idempotency_key,
        },
        metadata: ToolExecutionMetadata {
            started_at: Some(now),
            completed_at: Some(now),
            ..ToolExecutionMetadata::default()
        },
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> Result<T, String> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|_| "Meta runtime call requires a Tokio runtime".to_string())?;
    Ok(tokio::task::block_in_place(|| handle.block_on(future)))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[tokio::test]
    async fn review_admission_reports_shortage_before_reserving_or_starting_runtime() {
        for final_only in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut registry =
                MetaRuntimeRegistry::new(test_meerkat_config(temp.path().join("state")));
            if final_only {
                registry.final_review = crate::config::FinalReviewConfig {
                    enabled: true,
                    reviewers: vec![crate::config::FinalReviewReviewerConfig {
                        id: "domain".into(),
                        profile: "domain".into(),
                        required: true,
                    }],
                    ..Default::default()
                };
            } else {
                registry.independent_review.enabled = true;
            }
            registry
                .specialists
                .set(vec![RepositorySpecialist {
                    identity: "repo:domain".into(),
                    label: "Domain".into(),
                    focus: "Code".into(),
                    paths: vec![],
                }])
                .unwrap();
            let candidate = serde_json::from_value(json!({
                "tracker_context":"example-project", "issue_id":"row-1", "issue_identifier":"TEST-1",
                "repo":{"owner":"example","name":"app"}, "pull_request":1, "base_branch":"main", "head_sha":"a".repeat(40)
            })).unwrap();
            let draft = crate::review_requests::ReviewRequestDraft {
                candidate,
                reviewers: meerkat_mob::FlowRoleTargets::new([("domain", ["repo:domain".into()])])
                    .unwrap(),
                task: "Review the exact candidate".into(),
                idempotency_key: IdempotencyKey("review-1".into()),
                supersedes: None,
            };
            assert!(matches!(
                registry.admit_native_review(draft).await,
                Err(
                    crate::review_requests::ReviewRequestError::InsufficientCapacity {
                        required: 3,
                        available: 1
                    }
                )
            ));
            assert!(registry.runtime.lock().await.is_none());
            assert!(
                !registry
                    .state_path()
                    .join("review-requests.sqlite3")
                    .exists()
            );
        }
    }

    #[test]
    fn review_selection_resolves_durable_aliases_and_excludes_issue_participants() {
        let specialist = RepositorySpecialist {
            identity: "repo:domain".into(),
            label: "Domain".into(),
            focus: "Domain review".into(),
            paths: vec![],
        };
        assert!(
            require_review_capacity(1, std::slice::from_ref(&specialist), &BTreeSet::new()).is_ok()
        );
        assert!(matches!(
            require_review_capacity(3, std::slice::from_ref(&specialist), &BTreeSet::new()),
            Err(
                crate::review_requests::ReviewRequestError::InsufficientCapacity {
                    required: 3,
                    available: 1
                }
            )
        ));
        assert!(matches!(
            require_review_capacity(
                1,
                std::slice::from_ref(&specialist),
                &BTreeSet::from([specialist.identity.clone()])
            ),
            Err(
                crate::review_requests::ReviewRequestError::InsufficientCapacity {
                    required: 1,
                    available: 0
                }
            )
        ));
        let canonical =
            meerkat_meta::member_comms_id::roster_member_id_for_identity(&specialist.identity);
        let public =
            meerkat_mob::FlowRoleTargets::new([("domain", [specialist.identity.clone().into()])])
                .unwrap();
        let native = meerkat_mob::FlowRoleTargets::new([("domain", [canonical.clone()])]).unwrap();
        for input in [&public, &native] {
            assert_eq!(
                resolve_review_specialists(
                    input,
                    std::slice::from_ref(&specialist),
                    &BTreeSet::new()
                )
                .unwrap(),
                native
            );
            for participant in [&specialist.identity, &canonical.to_string()] {
                assert!(
                    resolve_review_specialists(
                        input,
                        std::slice::from_ref(&specialist),
                        &BTreeSet::from([participant.clone()])
                    )
                    .is_err()
                );
            }
        }
        let duplicates = meerkat_mob::FlowRoleTargets::new([(
            "domain",
            [canonical, specialist.identity.clone().into()],
        )])
        .unwrap();
        assert!(
            resolve_review_specialists(
                &duplicates,
                std::slice::from_ref(&specialist),
                &BTreeSet::new()
            )
            .is_err()
        );
        assert!(resolve_review_specialists(&public, &[], &BTreeSet::new()).is_err());
        let runtime_alias =
            meerkat_mob::FlowRoleTargets::new([("domain", ["rt:repo:domain:0".into()])]).unwrap();
        assert!(
            resolve_review_specialists(&runtime_alias, &[specialist], &BTreeSet::new()).is_err()
        );
    }

    use crate::config::{MaterializedMobMember, MobExecutionBackend};
    use crate::tools::{IdempotencyKey, LinearIssueIdentifier, WorkspaceKey};

    fn console_frame(kind: &str, payload: Value) -> ConsoleFrame {
        ConsoleFrame {
            id: "frame-1".to_string(),
            cursor: meerkat_meta::ConsoleCursor::from("console:1"),
            dedupe_key: "frame-1".to_string(),
            timestamp_ms: 1,
            runtime_key: "fugue".to_string(),
            identity: "LUC-650/issue_lead".to_string(),
            conversation_id: Some("LUC-650/issue_lead".to_string()),
            session_id: None,
            kind: kind.to_string(),
            status: meerkat_meta::ConsoleFrameStatus::Delivered,
            frame_version: 1,
            updated_at_ms: None,
            payload,
            source: meerkat_meta::ConsoleFrameSource {
                kind: meerkat_meta::ConsoleFrameSourceKind::ConsoleEvent,
                source_cursor: None,
                member_provenance: None,
            },
            source_event_id: None,
            interaction_id: None,
            turn_id: None,
            run_id: None,
            parent_frame_id: None,
            caused_by_frame_id: None,
        }
    }

    #[test]
    fn fugue_console_policy_hides_raw_comms_admission_prompts() {
        let policy = FugueConsoleVisibilityPolicy;
        let raw_prompt = "Peer message\nPeer message from fugue/planner/LUC-650/planner:\nPeer message from fugue/planner/LUC-650/planner:\nPlan updated.\nPeer message";
        let raw_frame = console_frame("run_started", json!({ "prompt": raw_prompt }));
        let notice_frame = console_frame(
            "system_notice",
            json!({
                "kind": "comms",
                "body": "Peer message",
                "blocks": [{
                    "type": "comms",
                    "summary": "Peer message",
                    "content": [{ "type": "text", "text": "Peer message from fugue/planner/LUC-650/planner:\nPlan updated." }]
                }]
            }),
        );
        let operator_frame = console_frame(
            "run_started",
            json!({ "prompt": "Please investigate this quoted peer text: Peer message from worker: done." }),
        );

        assert!(!policy.frame_visible(&raw_frame));
        assert!(policy.frame_visible(&notice_frame));
        assert!(policy.frame_visible(&operator_frame));
    }

    struct StaticToolDispatcher {
        tools: Arc<[Arc<ToolDef>]>,
    }

    impl StaticToolDispatcher {
        fn with_tool(name: &str) -> Self {
            Self {
                tools: vec![Arc::new(ToolDef::new(
                    name.to_string(),
                    "test external tool".to_string(),
                    json!({
                        "type": "object",
                        "properties": {},
                    }),
                ))]
                .into(),
            }
        }
    }

    #[async_trait]
    impl AgentToolDispatcher for StaticToolDispatcher {
        fn tools(&self) -> Arc<[Arc<ToolDef>]> {
            Arc::clone(&self.tools)
        }

        async fn dispatch(
            &self,
            call: ToolCallView<'_>,
        ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat_core::error::ToolError>
        {
            Ok(ToolResult::new(
                call.id.to_string(),
                json!({ "ok": true, "tool": call.name }).to_string(),
                false,
            )
            .into())
        }
    }

    #[test]
    fn native_member_names_are_comms_safe_and_collision_free() {
        assert_eq!(
            native_member_identity("LUC-900", "issue_lead"),
            "issue_LUC-900__issue_5Flead"
        );
        assert_ne!(
            native_member_identity("LUC/900", "coder"),
            native_member_identity("LUC_2F900", "coder")
        );
        let identity = native_member_identity("123/é", "review/orchestrator");
        assert!(identity.starts_with("issue_"));
        assert!(
            identity
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        );
    }

    // Meerkat's in-process comms routes are process-global. Exercise each real
    // runtime in its own process, as Meta's integration harness does, rather
    // than letting unrelated tests claim the same production mob identity.
    fn isolated_runtime_test(name: &str) -> bool {
        const MARKER: &str = "FUGUE_ISOLATED_RUNTIME_TEST";
        if std::env::var(MARKER).ok().as_deref() == Some(name) {
            return true;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .env(MARKER, name)
            .arg(format!("meta_runtime::tests::{name}"))
            .args(["--exact", "--nocapture"])
            .output()
            .expect("isolated runtime test process");
        if name == "runtime_members_emit_canonical_structural_events"
            && let Some(directory) = std::env::var_os("FUGUE_TEST_EVIDENCE_DIR")
        {
            let directory = PathBuf::from(directory);
            std::fs::write(
                directory.join("structural-events.stdout.log"),
                &output.stdout,
            )
            .expect("retain isolated runtime stdout");
            std::fs::write(
                directory.join("structural-events.stderr.log"),
                &output.stderr,
            )
            .expect("retain isolated runtime stderr");
        }
        if name.starts_with("admitted_host_signal_")
            && let Some(directory) = std::env::var_os("FUGUE_TEST_EVIDENCE_DIR")
        {
            let directory = PathBuf::from(directory);
            std::fs::write(directory.join(format!("{name}.stdout.log")), &output.stdout)
                .expect("retain host signal stdout");
            std::fs::write(directory.join(format!("{name}.stderr.log")), &output.stderr)
                .expect("retain host signal stderr");
        }
        assert!(
            output.status.success(),
            "isolated {name} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        false
    }

    pub(crate) fn test_meerkat_config(state_root: PathBuf) -> MeerkatConfig {
        let realm_suffix = state_root
            .parent()
            .and_then(|path| path.file_name())
            .map(|name| name.to_string_lossy().replace('.', "-"))
            .unwrap_or_else(|| "default".to_string());
        let context_root = state_root.join("context");
        let user_config_root = state_root.join("user-config");
        MeerkatConfig {
            runtime_connections: None,
            repository_runtime: None,
            command: "rkat-rpc".to_string(),
            realm: Some(format!("fugue-test-{realm_suffix}")),
            instance: None,
            realm_backend: Some("sqlite".to_string()),
            state_root: Some(state_root),
            context_root: Some(context_root),
            user_config_root: Some(user_config_root),
            auth_binding: None,
            model: Some(default_agent_model().to_string()),
            provider: None,
            max_tokens: None,
            system_prompt: None,
            output_schema: None,
            structured_output_retries: None,
            hooks_override: None,
            provider_params: None,
            enable_shell: Some(true),
            enable_builtins: Some(false),
            enable_mob: Some(true),
            enable_memory: None,
            keep_alive: true,
            max_sessions: 4096,
            comms_name: None,
            peer_meta: None,
            budget_limits: None,
            preload_skills: None,
            skill_refs: None,
            labels: None,
            additional_instructions: None,
            app_context: None,
            shell_env: None,
            flow_tool_overlay: None,
            first_turn_activation_prompt: "Begin.".to_string(),
            turn_timeout_ms: 60_000,
            read_timeout_ms: 60_000,
            stall_timeout_ms: -1,
        }
    }

    fn tracker_with_key() -> TrackerConfig {
        TrackerConfig {
            jira: None,
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("test-linear-key".to_string()),
            project_slug: Some("test-project".to_string()),
            intake_states: vec!["Todo".to_string()],
            in_flight_states: vec!["Planning".to_string(), "Implementing".to_string()],
            active_states: vec!["Todo".to_string(), "Planning".to_string()],
            attachment_root: None,
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec!["Done".to_string(), "Canceled".to_string()],
        }
    }

    fn mcp_server(command: &[&str], env: &[(&str, &str)]) -> NativeMcpServerConfig {
        NativeMcpServerConfig {
            command: command.iter().map(|part| (*part).to_string()).collect(),
            url: None,
            env: env
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        }
    }

    fn native_agent_profile(tools: &[&str]) -> NativeAgentProfile {
        NativeAgentProfile {
            model: None,
            provider: None,
            auth_binding: None,
            max_tokens: None,
            system_prompt: None,
            output_schema: None,
            structured_output_retries: None,
            provider_params: None,
            tools: tools.iter().map(|tool| (*tool).to_string()).collect(),
            skills: Vec::new(),
            additional_instructions: None,
        }
    }

    fn test_template() -> MaterializedMobTemplate {
        MaterializedMobTemplate {
            backend: MobExecutionBackend::NativeExecution,
            flow: None,
            linear_projection: None,
            max_review_cycles: None,
            members: vec![
                MaterializedMobMember {
                    name: ISSUE_LEAD.to_string(),
                    profile: ISSUE_LEAD.to_string(),
                    agent: native_agent_profile(&["mob", "mcp:linear"]),
                },
                MaterializedMobMember {
                    name: "planner".to_string(),
                    profile: "planner".to_string(),
                    agent: native_agent_profile(&["mob", "mcp:linear"]),
                },
                MaterializedMobMember {
                    name: "coder".to_string(),
                    profile: "coder".to_string(),
                    agent: native_agent_profile(&[
                        "mob",
                        "shell",
                        "git",
                        "mcp:linear",
                        "mcp:github",
                    ]),
                },
            ],
        }
    }

    fn launch_plan() -> NativeMobLaunchPlan {
        NativeMobLaunchPlan {
            input: SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-900".to_string()),
                },
                workspace_key: WorkspaceKey("LUC-900".to_string()),
                worktree_path: Some("/tmp/fugue-test/LUC-900".to_string()),
                instructions: Some("Test issue mob launch".to_string()),
                template_id: crate::tools::MobTemplateId("implementation_mob".to_string()),
                idempotency_key: IdempotencyKey("native-execution:LUC-900".to_string()),
            },
            template: test_template(),
            mcp_servers: BTreeMap::new(),
            project_slug: None,
            repository: None,
        }
    }

    fn scoped_direct_tools(
        authority: HostToolAuthority,
        declared: Option<&[String]>,
    ) -> Arc<dyn AgentToolDispatcher> {
        direct_tool_dispatcher(
            &tracker_with_key(),
            None,
            None,
            authority,
            declared,
            None,
            Vec::new(),
            Vec::new(),
            WorkGraphTerminalGate::new("/unused/workgraph.sqlite3", Vec::new(), Vec::new()),
            true,
            None,
            None,
        )
    }

    #[tokio::test]
    async fn configured_review_launch_accepts_only_operator_visible_candidate_fields() {
        use axum::{Json, Router, routing::post};
        let payload = json!({
            "issue":{"kind":"identifier","identifier":"TEST-1"},
            "repo":{"owner":"example","name":"app"},
            "pull_request":7,"head_sha":"a".repeat(40),
            "idempotency_key":"review-start-1"
        });
        let expected = payload.clone();
        let router = Router::new().route(
            "/api/v1/kapellmeister/start-independent-review",
            post(move |Json(body): Json<Value>| {
                assert_eq!(body, expected);
                async { Json(json!({"result":{"accepted":true}})) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (error, response) =
            call_fugue_start_independent_review(Some(&endpoint), None, &payload).await;
        server.abort();
        assert!(!error, "{response}");
        let dispatcher = scoped_direct_tools(HostToolAuthority::Coordinator, None);
        let tools = dispatcher.tools();
        let tool = tools
            .iter()
            .find(|tool| tool.name == "fugue_start_independent_review")
            .unwrap();
        for field in [
            "issue",
            "repo",
            "pull_request",
            "head_sha",
            "idempotency_key",
        ] {
            assert!(
                tool.input_schema["properties"].get(field).is_some(),
                "missing {field}"
            );
        }
        for field in ["spawn_input", "orchestrator_profile", "worktree_path"] {
            let mut forged = payload.clone();
            forged[field] = json!("caller-selected");
            let (error, _) = call_fugue_start_independent_review(None, None, &forged).await;
            assert!(error);
        }
    }

    #[tokio::test]
    async fn disabled_independent_review_is_absent_from_host_tool_catalog() {
        let tools = direct_tool_dispatcher(
            &tracker_with_key(),
            None,
            None,
            HostToolAuthority::Coordinator,
            None,
            None,
            Vec::new(),
            Vec::new(),
            WorkGraphTerminalGate::new("/unused/workgraph.sqlite3", Vec::new(), Vec::new()),
            false,
            None,
            None,
        );
        assert!(!tools.tools().iter().any(|tool| {
            matches!(
                tool.name.as_str(),
                "fugue_start_independent_review"
                    | "fugue_complete_independent_review"
                    | "fugue_complete_final_pr_gate"
            )
        }));
        assert!(
            tools
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_run_deploy")
        );
    }

    #[tokio::test]
    async fn host_tools_enforce_native_roles_and_declared_adapters() {
        let coordinator = scoped_direct_tools(HostToolAuthority::Coordinator, None);
        assert!(coordinator.tool_catalog_capabilities().exact_catalog);
        assert_eq!(coordinator.tool_catalog().len(), coordinator.tools().len());
        assert!(
            coordinator
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_run_deploy")
        );
        assert!(
            coordinator
                .tools()
                .iter()
                .any(|tool| tool.name == "linear_move_issue")
        );
        assert!(
            !coordinator
                .tools()
                .iter()
                .any(|tool| tool.name == "github_merge_pr")
        );
        assert!(
            coordinator
                .tools()
                .iter()
                .any(|tool| tool.name == "github_create_pr")
        );
        let declared = vec!["mcp:linear".to_string()];
        let worker = scoped_direct_tools(HostToolAuthority::Worker, Some(&declared));
        assert!(
            worker
                .tools()
                .iter()
                .any(|tool| tool.name == "linear_inspect_issue")
        );
        assert!(
            !worker
                .tools()
                .iter()
                .any(|tool| tool.name == "github_inspect_pr"
                    || tool.name.starts_with("fugue_")
                    || tool.name == "linear_move_issue")
        );
        let args = serde_json::value::RawValue::from_string("{}".to_string()).expect("arguments");
        assert!(matches!(
            worker
                .dispatch(ToolCallView {
                    id: "forbidden",
                    name: "fugue_run_deploy",
                    args: &args
                })
                .await,
            Err(meerkat_core::error::ToolError::NotFound { .. })
        ));
        let reviewer = scoped_direct_tools(HostToolAuthority::ReviewOrchestrator, None);
        assert!(
            reviewer
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_admit_independent_review")
        );
        assert!(
            reviewer
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_complete_independent_review")
        );
        assert!(
            reviewer
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_complete_final_pr_gate")
        );
        assert!(
            !reviewer
                .tools()
                .iter()
                .any(|tool| tool.name == "fugue_run_deploy")
        );
    }

    #[test]
    fn issue_lead_prompt_projects_running_work_to_active_with_phase_evidence() {
        let prompt = issue_lead_prompt("LUC-900", None);

        assert!(prompt.contains("Queue is the intake lane"));
        assert!(prompt.contains("Active is the single running issue-mob lane"));
        assert!(prompt.contains("project the issue to Active"));
        assert!(prompt.contains("labels, WorkGraph, Workpad, and Meta runtime evidence"));
        assert!(!prompt.contains("ordinary issue-stage movement"));
    }

    #[test]
    fn jira_prompts_follow_configured_running_lane() {
        let mut tracker = tracker_with_key();
        tracker.kind = "jira".to_string();
        tracker.in_flight_states = vec!["Working".to_string()];
        let lead = issue_lead_prompt_for_tracker("TEST-1", None, &tracker, None);
        let reviewer =
            worker_prompt_with_lane("TEST-1", "reviewer", None, running_lane(&tracker, None));
        assert!(lead.contains("project the issue to Working"));
        assert!(reviewer.contains("project the issue to Working"));
        assert!(!lead.contains("Queue is the intake lane"));
        assert!(!reviewer.contains("project the issue to Active"));
        assert!(lead.contains("omit realm_id and namespace"));
        assert!(!lead.contains("Terminal success means accepted development work"));
        let projection = crate::config::MaterializedLinearProjection {
            id: "development".into(),
            node_lanes: BTreeMap::new(),
            node_phase_labels: BTreeMap::new(),
            aggregate: crate::config::NativeLinearProjectionAggregate {
                precedence: vec!["Working".into()],
                fallback: Some("Working".into()),
            },
            terminal_lanes: crate::config::NativeLinearProjectionTerminals {
                succeeded: "Awaiting approval".into(),
                failed: "Working".into(),
                cancelled: "Closed".into(),
            },
        };
        let lead = issue_lead_prompt_for_tracker("TEST-1", None, &tracker, Some(&projection));
        assert!(lead.contains("Successful development projects to Awaiting approval"));
    }

    #[test]
    fn reviewer_prompt_routes_red_review_to_active_remediation() {
        let prompt = worker_prompt("LUC-900", "reviewer", None);

        assert!(prompt.contains("project the issue to Active"));
        assert!(prompt.contains("focused replanning/remediation with phase evidence"));
        assert!(prompt.contains("fork_off with message_count: 0"));
        assert!(prompt.contains("verify its working directory first"));
        assert!(prompt.contains("it must not delegate further"));
    }

    #[test]
    fn hosted_acceptance_review_handoff_does_not_wait_for_its_own_review_result() {
        let mut tracker = tracker_with_key();
        tracker.kind = "jira".into();
        tracker.in_flight_states = vec!["Working".into()];
        let projection = crate::config::MaterializedLinearProjection {
            id: "development".into(),
            node_lanes: BTreeMap::new(),
            node_phase_labels: BTreeMap::new(),
            aggregate: crate::config::NativeLinearProjectionAggregate {
                precedence: vec![],
                fallback: Some("Working".into()),
            },
            terminal_lanes: crate::config::NativeLinearProjectionTerminals {
                succeeded: "Awaiting review".into(),
                failed: "Working".into(),
                cancelled: "Closed".into(),
            },
        };
        let prompt = issue_lead_prompt_for_tracker("TEST-1", None, &tracker, Some(&projection));
        assert!(prompt.contains("issue-team review"));
        assert!(prompt.contains("does not require the result of the subsequent repository review"));
        assert!(
            prompt
                .contains("Do not close future review or merge obligations to permit this handoff")
        );
        assert!(
            prompt.contains("only reviewer-role agents may close completed review obligations")
        );
    }

    #[test]
    fn issue_member_prompts_define_workgraph_scope() {
        let lead = issue_lead_prompt("LUC-900", None);
        let planner = worker_prompt("LUC-900", "planner", None);
        let coder = worker_prompt("LUC-900", "coder", None);
        let reviewer = worker_prompt("LUC-900", "reviewer", None);

        for prompt in [&lead, &planner, &coder, &reviewer] {
            assert!(prompt.contains("omit realm_id and namespace"));
            assert!(prompt.contains("never invent a realm or namespace"));
            assert!(prompt.contains("LUC-900"));
        }
    }

    #[test]
    fn final_gate_coordinator_receives_native_review_assignment() {
        let mut plan = launch_plan();
        plan.input.template_id = crate::tools::MobTemplateId("final-pr-gate-191".to_string());
        plan.input.instructions =
            Some("Review exact head SHA `abc123` using existing specialists.".to_string());

        let message = member_initial_message_for_plan(&plan, "review_orchestrator")
            .expect("final gate coordinator assignment");

        assert!(message.contains("Coordinator: review_orchestrator"));
        assert!(message.contains("fugue_complete_final_pr_gate"));
        assert!(message.contains("Do not merge"));
        assert!(message.contains("abc123"));
    }

    #[test]
    fn implementation_members_do_not_receive_bootstrap_assignment() {
        let plan = launch_plan();

        assert!(member_initial_message_for_plan(&plan, "coder").is_none());
    }

    #[test]
    fn direct_tool_catalog_hides_lane_mutation_for_worker_sessions() {
        let tool_names = fugue_tool_defs(&tracker_with_key(), false)
            .into_iter()
            .map(|tool| tool.name.as_str().to_string())
            .collect::<Vec<_>>();

        assert!(tool_names.iter().any(|name| name == "linear_inspect_issue"));
        assert!(
            !tool_names.iter().any(|name| name == "linear_move_issue"),
            "worker sessions can inspect/update evidence but must not own lane mutation"
        );
        assert!(
            !tool_names.iter().any(|name| name == "linear_claim_issue"),
            "claim can assign a terminal state and requires the same lifecycle authority"
        );
    }

    #[tokio::test]
    async fn claim_checks_obligations_before_jira_assignment_or_transition() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workgraph.sqlite3");
        let realm = meerkat_core::mob_realm_id("claim-test").unwrap();
        let graph = meerkat::WorkGraphService::with_scope(
            Arc::new(meerkat::SqliteWorkGraphStore::open(&path).unwrap()),
            realm.as_str(),
            meerkat::WorkNamespace::default(),
        );
        graph
            .create(meerkat::CreateWorkItemRequest {
                realm_id: None,
                namespace: None,
                title: "Unresolved review".into(),
                description: None,
                priority: meerkat::WorkPriority::High,
                completion_policy: meerkat::WorkCompletionPolicy::SelfAttest,
                failed_child_join_policy: Default::default(),
                cancelled_child_join_policy: Default::default(),
                labels: ["fugue", "blocking", "issue:TEST-1"]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                due_at: None,
                not_before: None,
                snoozed_until: None,
                external_refs: Vec::new(),
                evidence_refs: Vec::new(),
                status: Some(meerkat::WorkStatus::Open),
            })
            .await
            .unwrap();
        for (use_mcp, tool_name) in [
            (false, "linear_claim_issue"),
            (true, "linear_claim_issue"),
            (true, "linear.claim_issue"),
            (true, "linear_move_issue"),
            (true, "linear.move_issue"),
        ] {
            let is_claim = tool_name.parse::<FugueToolOperation>().unwrap()
                == FugueToolOperation::LinearClaimIssue;
            let state_field = if is_claim { "target_state" } else { "state" };
            for issue_ref in [
                json!({"kind":"identifier", "identifier":"TEST-1"}),
                json!({"kind":"id", "id":"10001"}),
            ] {
                let lookup_path = if issue_ref["kind"] == "id" {
                    "/rest/api/3/issue/10001"
                } else {
                    "/rest/api/3/issue/TEST-1"
                };
                let mock = mock_jira(vec![
                    reply("GET", lookup_path, issue("In Progress")),
                    reply(
                        "GET",
                        "/rest/api/3/issue/TEST-1/comment",
                        json!({
                            "comments": [], "total": 0, "startAt": 0, "maxResults": 100
                        }),
                    ),
                ])
                .await;
                let tracker = tracker(&mock.endpoint);
                let gate = WorkGraphTerminalGate::new(&path, vec!["Done".into()], Vec::new())
                    .for_mob("claim-test");
                let mcp = crate::mcp_server::LinearMcpHandler::projection_for_test(
                    tracker.clone(),
                    Some(gate.clone()),
                );
                let dispatcher = direct_tool_dispatcher(
                    &tracker,
                    None,
                    None,
                    HostToolAuthority::IssueLead,
                    None,
                    None,
                    Vec::new(),
                    Vec::new(),
                    gate.clone(),
                    false,
                    None,
                    None,
                );
                let mut input = json!({
                    "issue": issue_ref, "assignee": {"kind":"id", "id":"operator"},
                    "target_state": {"kind":"name", "name":"Done"},
                    "idempotency_key":"claim-test"
                });
                for (state, expected) in [
                    (
                        json!({"kind":"name", "name":"Done"}),
                        "unresolved_workgraph_obligations",
                    ),
                    (json!({"kind":"id", "id":"3"}), "noncanonical_public_lane"),
                ] {
                    input[state_field] = state;
                    if use_mcp {
                        let (failed, response) = mcp.execute(tool_name, input.clone()).await;
                        assert!(failed);
                        assert!(response.contains(expected), "{response}");
                    } else {
                        let args = serde_json::value::to_raw_value(&input).unwrap();
                        let outcome = dispatcher
                            .dispatch(ToolCallView {
                                id: "claim",
                                name: "linear_claim_issue",
                                args: &args,
                            })
                            .await
                            .unwrap();
                        assert!(outcome.result.is_error);
                        assert!(
                            serde_json::to_string(&outcome.result.content)
                                .unwrap()
                                .contains(expected)
                        );
                    }
                }
                assert!(mock.responses.lock().unwrap().is_empty());
                assert_eq!(
                    mock.requests.lock().unwrap().len(),
                    2,
                    "only inspection may reach Jira"
                );
                if !is_claim {
                    continue;
                }
                // An assignment without a lane change remains usable even with an open obligation.
                mock.responses.lock().unwrap().extend([
                    reply("GET", lookup_path, issue("In Progress")),
                    reply("PUT", "/rest/api/3/issue/TEST-1/assignee", json!({})),
                    reply("GET", lookup_path, issue("In Progress")),
                ]);
                input["target_state"] = Value::Null;
                if use_mcp {
                    let (failed, response) = mcp.execute(tool_name, input.clone()).await;
                    assert!(!failed, "{response}");
                } else {
                    let args = serde_json::value::to_raw_value(&input).unwrap();
                    let outcome = dispatcher
                        .dispatch(ToolCallView {
                            id: "assign",
                            name: "linear_claim_issue",
                            args: &args,
                        })
                        .await
                        .unwrap();
                    assert!(!outcome.result.is_error, "{:?}", outcome.result.content);
                }
                assert!(mock.responses.lock().unwrap().is_empty());
                assert_eq!(mock.requests.lock().unwrap().len(), 5);
            }
        }
    }

    #[test]
    fn direct_linear_move_rejects_legacy_public_phase_lanes() {
        let mut tracker = tracker_with_key();
        tracker.intake_states = vec!["Queue".to_string()];
        tracker.in_flight_states = vec!["Active".to_string()];
        tracker.active_states = vec![
            "Queue".to_string(),
            "Active".to_string(),
            "Review".to_string(),
            "Deploy".to_string(),
            "Blocked".to_string(),
        ];
        tracker.terminal_states = vec![
            "Done".to_string(),
            "Canceled".to_string(),
            "Duplicate".to_string(),
        ];

        for lane in [
            "In Progress",
            "Planning",
            "Implementing",
            "Reviewing",
            "Ready for review",
            "Rework",
        ] {
            let input = move_issue_input_for_lane(lane);
            let message =
                guard_direct_public_lane_move(&tracker, &["Blocked".to_string()], &[], &input)
                    .expect_err("legacy lane rejected");

            assert!(
                message.contains("legacy Fugue/Linear phase lane"),
                "unexpected message for {lane}: {message}"
            );
        }
    }

    #[test]
    fn direct_linear_move_allows_configured_public_lanes() {
        let mut tracker = tracker_with_key();
        tracker.intake_states = vec!["Queue".to_string()];
        tracker.in_flight_states = vec!["Active".to_string()];
        tracker.active_states = vec![
            "Queue".to_string(),
            "Active".to_string(),
            "Review".to_string(),
            "Deploy".to_string(),
            "Blocked".to_string(),
        ];
        tracker.terminal_states = vec![
            "Done".to_string(),
            "Canceled".to_string(),
            "Duplicate".to_string(),
        ];

        for lane in [
            "Queue",
            "Active",
            "Review",
            "Deploy",
            "Blocked",
            "Done",
            "Canceled",
            "Duplicate",
        ] {
            guard_direct_public_lane_move(
                &tracker,
                &["Blocked".to_string()],
                &[],
                &move_issue_input_for_lane(lane),
            )
            .unwrap_or_else(|message| panic!("{lane} should be allowed: {message}"));
        }
    }

    #[test]
    fn direct_linear_move_rejects_configured_action_lanes() {
        let mut tracker = tracker_with_key();
        tracker.intake_states = vec!["Queue".to_string()];
        tracker.in_flight_states = vec!["Active".to_string()];
        tracker.active_states = vec![
            "Queue".to_string(),
            "Active".to_string(),
            "Review".to_string(),
            "Deploy".to_string(),
            "Final Review & Merge".to_string(),
        ];
        tracker.terminal_states = vec!["Done".to_string(), "Canceled".to_string()];
        let action_states = vec![
            "Review".to_string(),
            "Deploy".to_string(),
            "Final Review & Merge".to_string(),
        ];

        for lane in ["Review", "Deploy", "Final Review & Merge"] {
            let message = guard_direct_public_lane_move(
                &tracker,
                &[],
                &action_states,
                &move_issue_input_for_lane(lane),
            )
            .expect_err("action lane rejected");

            assert!(
                message.contains("Fugue action lane"),
                "unexpected message for {lane}: {message}"
            );
        }

        let message = guard_direct_public_lane_move(
            &tracker,
            &[],
            &["Do Review".to_string()],
            &move_issue_input_for_lane("Do Review"),
        )
        .expect_err("legacy review action alias rejected");
        assert!(message.contains("legacy Fugue/Linear phase lane"));
        assert!(message.contains("Review"));

        let message = guard_direct_public_lane_move(
            &tracker,
            &[],
            &action_states,
            &move_issue_input_for_lane("Reviewish"),
        )
        .expect_err("unknown lane rejected");
        assert!(!message.contains("Review,"));
        assert!(!message.contains("Do Review"));
        assert!(!message.contains("Deploy"));
        assert!(!message.contains("Final Review & Merge"));
    }

    #[test]
    fn direct_linear_move_rejects_opaque_state_ids() {
        let input = MoveIssueInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-900".to_string()),
            },
            state: LinearStateRef::Id {
                id: crate::tools::LinearStateId("state-hidden-in-progress".to_string()),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("move:LUC-900:hidden".to_string()),
        };

        let message = guard_direct_public_lane_move(&tracker_with_key(), &[], &[], &input)
            .expect_err("opaque state ids are not allowed for agent lane moves");
        assert!(message.contains("named Fugue public lane"));
    }

    #[tokio::test]
    async fn workspace_context_exposes_meerkat_tools_bound_to_worktree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let worktree = temp.path().join("worktree");
        std::fs::create_dir_all(&worktree).expect("create worktree");
        let hook = FugueDirectToolSessionHook {
            connections: None,
            mob_id: MOB_ID.to_owned(),
            agent_profiles: BTreeMap::new(),
            default_auth_binding: None,
            review_orchestrator_profile: IndependentReviewConfig::default().orchestrator_profile,
            independent_review_enabled: false,
            tracker: tracker_with_key(),
            host_signal_binding: None,
            host_api_url: None,
            host_api_auth: None,
            github_repo: None,
            review_handoff_states: Vec::new(),
            action_states: Vec::new(),
            workgraph_gate: WorkGraphTerminalGate::new(
                temp.path().join("workgraph.sqlite3"),
                vec!["Done".to_string()],
                Vec::new(),
            ),
        };
        let mut req = CreateSessionRequest {
            model: default_agent_model().to_string(),
            prompt: meerkat_core::types::ContentInput::Text(
                "You are coder for Fugue issue LUC-900.".to_string(),
            ),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::Discard,
            build: Some(SessionBuildOptions {
                override_builtins: meerkat_core::ToolCategoryOverride::Enable,
                override_shell: meerkat_core::ToolCategoryOverride::Enable,
                app_context: Some(json!({
                    FUGUE_CONTEXT_KEY: {
                        FUGUE_CONTEXT_WORKTREE_PATH_KEY: worktree.display().to_string(),
                    }
                })),
                external_tools: Some(Arc::new(StaticToolDispatcher::with_tool(
                    "browser_navigate",
                ))),
                ..SessionBuildOptions::default()
            }),
            labels: None,
            injected_context: Vec::new(),
        };

        hook.before_create(&mut req).await.expect("hook succeeds");
        let build = req.build.expect("build options");
        assert_eq!(
            build.override_builtins,
            meerkat_core::ToolCategoryOverride::Disable
        );
        assert_eq!(
            build.override_shell,
            meerkat_core::ToolCategoryOverride::Disable
        );
        let dispatcher = build.external_tools.expect("workspace dispatcher");
        let tool_names = dispatcher
            .tools()
            .iter()
            .map(|tool| tool.name.as_str().to_string())
            .collect::<Vec<_>>();
        assert!(
            tool_names.iter().any(|name| name == "browser_navigate"),
            "SessionHook must preserve profile/module MCP tools installed by Meta"
        );
        assert!(tool_names.iter().any(|name| name == "shell"));
        assert!(tool_names.iter().any(|name| name == "apply_patch"));
        assert!(tool_names.iter().any(|name| name == "linear_inspect_issue"));
        let tools = dispatcher.tools();
        let filter = meerkat_core::tool_scope::ToolFilter::Allow(
            tools
                .iter()
                .filter(|tool| {
                    is_linear_fugue_tool_name(&tool.name) || is_github_fugue_tool_name(&tool.name)
                })
                .map(|tool| tool.name.clone())
                .collect(),
        );
        let definitions = tools
            .iter()
            .map(|tool| tool.as_ref().clone())
            .collect::<Vec<_>>();
        let witnesses =
            meerkat_core::tool_scope::filter_witnesses_for_tool_defs(&definitions, &filter);
        meerkat_core::tool_scope::validate_witnessed_filter_authority(&filter, &witnesses)
            .expect("native helper inheritance retains Fugue tool provenance");

        assert!(
            !tool_names.iter().any(|name| name == "linear_move_issue"),
            "worker sessions still must not get lane mutation tools"
        );

        let args = serde_json::value::RawValue::from_string(
            json!({
                "patch": "*** Begin Patch\n*** Add File: rooted.txt\n+ok\n*** End Patch\n"
            })
            .to_string(),
        )
        .expect("raw apply_patch args");
        let outcome = dispatcher
            .dispatch(ToolCallView {
                id: "apply-1",
                name: "apply_patch",
                args: &args,
            })
            .await
            .expect("apply_patch dispatch");

        assert!(
            !outcome.result.is_error,
            "apply_patch should succeed: {:?}",
            outcome.result.content
        );
        assert_eq!(
            std::fs::read_to_string(worktree.join("rooted.txt")).expect("patched file"),
            "ok\n"
        );
        assert!(
            !temp.path().join("rooted.txt").exists(),
            "workspace tools must not write relative to the runtime temp root"
        );
    }

    #[tokio::test]
    async fn session_hook_preserves_existing_external_tools() {
        let temp = tempfile::tempdir().expect("tempdir");
        let hook = FugueDirectToolSessionHook {
            connections: None,
            mob_id: MOB_ID.to_owned(),
            agent_profiles: BTreeMap::new(),
            default_auth_binding: None,
            review_orchestrator_profile: IndependentReviewConfig::default().orchestrator_profile,
            independent_review_enabled: false,
            tracker: tracker_with_key(),
            host_signal_binding: None,
            host_api_url: None,
            host_api_auth: None,
            github_repo: None,
            review_handoff_states: Vec::new(),
            action_states: Vec::new(),
            workgraph_gate: WorkGraphTerminalGate::new(
                temp.path().join("workgraph.sqlite3"),
                vec!["Done".to_string()],
                Vec::new(),
            ),
        };
        let mut req = CreateSessionRequest {
            model: default_agent_model().to_string(),
            prompt: meerkat_core::types::ContentInput::Text(
                "You are coder for Fugue issue LUC-900.".to_string(),
            ),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::Defer,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::Discard,
            build: Some(SessionBuildOptions {
                external_tools: Some(Arc::new(StaticToolDispatcher::with_tool(
                    "browser_navigate",
                ))),
                ..SessionBuildOptions::default()
            }),
            labels: None,
            injected_context: Vec::new(),
        };

        hook.before_create(&mut req).await.expect("hook succeeds");
        let dispatcher = req
            .build
            .and_then(|build| build.external_tools)
            .expect("external tools");
        let tool_names = dispatcher
            .tools()
            .iter()
            .map(|tool| tool.name.as_str().to_string())
            .collect::<Vec<_>>();
        assert!(
            tool_names.iter().any(|name| name == "browser_navigate"),
            "configured MCP tools must be composed into Fugue member sessions"
        );
        assert!(tool_names.iter().any(|name| name == "linear_inspect_issue"));
    }

    fn move_issue_input_for_lane(lane: &str) -> MoveIssueInput {
        MoveIssueInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-900".to_string()),
            },
            state: LinearStateRef::Name {
                name: lane.to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey(format!("move:LUC-900:{lane}")),
        }
    }

    #[test]
    fn quoted_coordinator_text_does_not_grant_mutation_authority() {
        let mut req = CreateSessionRequest {
            model: default_agent_model().to_string(),
            prompt: meerkat_core::types::ContentInput::Text(
                "You have been spawned as 'FUG-607/issue_lead' (role: issue_lead) in mob 'fugue'."
                    .to_string(),
            ),
            system_prompt: meerkat_core::config::SystemPromptOverride::Inherit,
            max_tokens: None,
            event_tx: None,
            initial_turn: meerkat_core::service::InitialTurnPolicy::RunImmediately,
            deferred_prompt_policy: meerkat_core::service::DeferredPromptPolicy::Discard,
            build: None,
            labels: None,
            injected_context: Vec::new(),
        };

        assert_eq!(
            HostToolAuthority::for_binding(
                req.build
                    .as_ref()
                    .and_then(|build| build.mob_member_binding.as_ref()),
                "review_orchestrator"
            ),
            HostToolAuthority::Worker
        );
        req.labels = Some(BTreeMap::from([(
            "role".to_string(),
            ISSUE_LEAD.to_string(),
        )]));
        assert_eq!(
            HostToolAuthority::for_binding(
                req.build
                    .as_ref()
                    .and_then(|build| build.mob_member_binding.as_ref()),
                "review_orchestrator"
            ),
            HostToolAuthority::Worker,
            "labels alone are not authority"
        );
        req.build = Some(SessionBuildOptions {
            mob_member_binding: Some(meerkat_core::MobMemberBinding {
                mob_id: MOB_ID.to_string(),
                role: ISSUE_LEAD.to_string(),
                member: "issue_LUC-900__issue_5Flead".to_string(),
            }),
            ..SessionBuildOptions::default()
        });
        let binding = req
            .build
            .as_ref()
            .and_then(|build| build.mob_member_binding.as_ref());
        assert_eq!(
            HostToolAuthority::for_binding(binding, "review_orchestrator"),
            HostToolAuthority::IssueLead
        );
        let mut binding = binding.cloned().unwrap();
        binding.role = KAPELLMEISTER.to_string();
        assert_eq!(
            HostToolAuthority::for_binding(Some(&binding), "review_orchestrator"),
            HostToolAuthority::Worker
        );
        binding.member = KAPELLMEISTER.to_string();
        assert_eq!(
            HostToolAuthority::for_binding(Some(&binding), "review_orchestrator"),
            HostToolAuthority::Coordinator
        );
        binding.mob_id = "child-mob".to_string();
        assert_eq!(
            HostToolAuthority::for_binding(Some(&binding), "review_orchestrator"),
            HostToolAuthority::Worker
        );
    }

    #[test]
    fn direct_github_tools_default_repo_from_meta_module_scope() {
        let servers = BTreeMap::from([(
            "github".to_string(),
            mcp_server(
                &["fugue-github-mcp"],
                &[
                    ("FUGUE_GITHUB_OWNER", "lukacf"),
                    ("FUGUE_GITHUB_REPO", "meerkat-fugue"),
                ],
            ),
        )]);
        let repo = github_repo_from_mcp_servers(&servers).expect("repo scope");

        let arguments = apply_direct_github_repo_scope(
            json!({
                "head_branch": "luka/luc-619-find-3-bugs-and-fix-them",
                "base_branch": "main",
                "title": "LUC-619 Fix three scoped bugs",
                "body": "Reviewer gate green.",
                "idempotency_key": "luc-619-create-pr",
                "draft": false,
                "existing_pr_match": "head_branch"
            }),
            repo.as_ref(),
        );

        assert_eq!(
            arguments.pointer("/repo/owner").and_then(Value::as_str),
            Some("lukacf")
        );
        assert_eq!(
            arguments.pointer("/repo/name").and_then(Value::as_str),
            Some("meerkat-fugue")
        );
        assert_eq!(
            arguments.pointer("/head_branch").and_then(Value::as_str),
            Some("luka/luc-619-find-3-bugs-and-fix-them")
        );
    }

    #[tokio::test]
    async fn embedded_tracker_dispatch_records_the_repository_journal() {
        let temp = tempfile::tempdir().unwrap();
        let definition = crate::config::WorkflowDefinition {
            config: json!({"tracker": {
                "kind": "fugue-ims", "endpoint": temp.path().join("issues.sqlite3"),
                "project_slug": "example/app", "active_states": ["Queued", "Active"],
                "terminal_states": ["Done"]
            }})
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: String::new(),
        };
        let config = crate::config::ServiceConfig::from_definition(
            temp.path().join("fugue.yaml"),
            &definition,
        )
        .unwrap();
        let store = crate::fugue_ims::FugueImsStore::from_config(&config.tracker).unwrap();
        store
            .create_issue(crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("APP-1".into()),
                repo: "example/app".into(),
                title: "Embedded journal dispatch".into(),
                description: None,
                stage: crate::ims::ImsStageName("Queued".into()),
                labels: Vec::new(),
            })
            .unwrap();
        let binding = HostSignalBinding::from_config(&config).unwrap();
        binding.record_internal_lane("APP-1", "Queued").unwrap();
        let before = binding.open().unwrap().revision().unwrap();
        let dispatcher = direct_tool_dispatcher(
            &config.tracker,
            None,
            None,
            HostToolAuthority::IssueLead,
            None,
            None,
            Vec::new(),
            Vec::new(),
            WorkGraphTerminalGate::for_service(&config),
            false,
            None,
            Some(binding.clone()),
        );
        let args = serde_json::value::to_raw_value(&json!({
            "issue": {"kind": "identifier", "identifier": "APP-1"},
            "state": {"kind": "name", "name": "Active"}, "idempotency_key": "embedded-move"
        }))
        .unwrap();
        let outcome = dispatcher
            .dispatch(ToolCallView {
                id: "move",
                name: "linear_move_issue",
                args: &args,
            })
            .await
            .unwrap();
        assert!(!outcome.result.is_error, "{:?}", outcome.result.content);
        let mut journal = binding.open().unwrap();
        assert_ne!(journal.revision().unwrap(), before);
        assert!(journal.pending(10).unwrap().is_empty());
        assert_eq!(
            journal.begin().unwrap().observe_lane("APP-1", "Active"),
            crate::linear_lane_state::LinearLaneObservation::Unchanged
        );
    }

    #[test]
    fn direct_linear_tools_inherit_review_handoff_states_from_module_scope() {
        let servers = BTreeMap::from([(
            "linear".to_string(),
            mcp_server(
                &["fugue-linear-mcp", "--stdio"],
                &[(
                    LINEAR_REVIEW_HANDOFF_STATES_ENV,
                    "Ready for review, Blocked",
                )],
            ),
        )]);

        assert_eq!(
            review_handoff_states_from_mcp_servers(&servers),
            vec!["Ready for review".to_string(), "Blocked".to_string()]
        );
    }

    #[test]
    fn direct_linear_tools_inherit_action_states_from_module_scope() {
        let servers = BTreeMap::from([(
            "linear".to_string(),
            mcp_server(
                &["fugue-linear-mcp", "--stdio"],
                &[(
                    LINEAR_ACTION_STATES_ENV,
                    "Review, Deploy, Final Review & Merge",
                )],
            ),
        )]);

        assert_eq!(
            action_states_from_mcp_servers(&servers),
            vec![
                "Review".to_string(),
                "Deploy".to_string(),
                "Final Review & Merge".to_string()
            ]
        );
    }

    #[test]
    fn direct_github_scope_preserves_explicit_repo() {
        let configured = GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        };

        let arguments = apply_direct_github_repo_scope(
            json!({
                "repo": {
                    "owner": "other",
                    "name": "repo"
                }
            }),
            Some(&configured),
        );

        assert_eq!(
            arguments.pointer("/repo/owner").and_then(Value::as_str),
            Some("other")
        );
        assert_eq!(
            arguments.pointer("/repo/name").and_then(Value::as_str),
            Some("repo")
        );
    }

    #[test]
    fn duplicate_jira_migration_preserves_unrelated_definition_policy() {
        let registry =
            MetaRuntimeRegistry::new(test_meerkat_config(PathBuf::from("/tmp/fugue-state")));
        let desired = registry.mob_definition().expect("definition");
        let mut previous = desired.clone();
        for binding in previous.profiles.values_mut() {
            if let ProfileBinding::Inline(profile) = binding {
                profile.tools.mcp_servers.push(McpServerConfig::stdio(
                    "jira",
                    "fugue-jira-mcp",
                    Vec::new(),
                    Default::default(),
                ));
            }
        }
        assert_eq!(
            migrate_duplicate_jira_mcp(&previous, &desired),
            Some(desired.clone())
        );
        assert!(migrate_duplicate_jira_mcp(&desired, &desired).is_none());
        let ProfileBinding::Inline(profile) =
            previous.profiles.get_mut(KAPELLMEISTER).expect("profile")
        else {
            panic!("inline profile")
        };
        profile.model = "different-model".into();
        assert!(migrate_duplicate_jira_mcp(&previous, &desired).is_none());
    }

    #[test]
    fn profiles_expose_configured_mcp_tools_to_meta() {
        let mcp_servers = BTreeMap::from([
            ("linear".to_string(), mcp_server(&["fugue-linear-mcp"], &[])),
            ("jira".to_string(), mcp_server(&["fugue-jira-mcp"], &[])),
            ("github".to_string(), mcp_server(&["fugue-github-mcp"], &[])),
            ("docs".to_string(), mcp_server(&["docs-mcp"], &[])),
        ]);
        let control = control_profile("repo coordinator", &mcp_servers, default_agent_model());
        let worker = agent_profile(
            &native_agent_profile(&[
                "mob",
                "shell",
                "mcp:linear",
                "mcp:jira",
                "mcp:github",
                "mcp:docs",
            ]),
            &mcp_servers,
        )
        .expect("valid profile");
        assert_eq!(
            control
                .tools
                .mcp_servers
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["docs"]
        );
        assert_eq!(
            worker
                .tools
                .mcp_servers
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["docs"]
        );
        assert!(worker.tools.mob);
        assert!(worker.tools.shell);
        assert!(control.tools.workgraph);
        assert!(worker.tools.workgraph);
    }

    #[test]
    fn issue_member_profiles_expose_playwright_mcp_by_default_when_configured() {
        let mcp_servers = BTreeMap::from([
            ("linear".to_string(), mcp_server(&["fugue-linear-mcp"], &[])),
            ("github".to_string(), mcp_server(&["fugue-github-mcp"], &[])),
            (
                "playwright".to_string(),
                mcp_server(&["npx", "-y", "@playwright/mcp@latest", "--headless"], &[]),
            ),
        ]);
        let worker = agent_profile(
            &native_agent_profile(&["mob", "shell", "mcp:linear", "mcp:github"]),
            &mcp_servers,
        )
        .expect("valid profile");

        assert_eq!(
            worker
                .tools
                .mcp_servers
                .iter()
                .map(|server| server.name.as_str())
                .collect::<Vec<_>>(),
            vec!["playwright"]
        );
    }

    #[test]
    fn review_coordinator_exposes_playwright_mcp_when_configured() {
        let mcp_servers = BTreeMap::from([
            ("linear".to_string(), mcp_server(&["fugue-linear-mcp"], &[])),
            ("github".to_string(), mcp_server(&["fugue-github-mcp"], &[])),
            (
                "playwright".to_string(),
                mcp_server(&["npx", "-y", "@playwright/mcp@latest", "--headless"], &[]),
            ),
            (
                "fugue_host".to_string(),
                mcp_server(&["fugue-host-mcp"], &[]),
            ),
        ]);

        let independent_orchestrator =
            independent_review_orchestrator_profile(&mcp_servers, default_agent_model());

        assert_eq!(
            independent_orchestrator
                .tools
                .mcp_servers
                .iter()
                .map(|server| server.name.as_str())
                .collect::<Vec<_>>(),
            vec!["playwright"]
        );
    }

    #[test]
    fn final_only_review_registers_coordinator_without_manufacturing_specialists() {
        let mcp_servers = BTreeMap::from([
            ("linear".to_string(), mcp_server(&["fugue-linear-mcp"], &[])),
            ("github".to_string(), mcp_server(&["fugue-github-mcp"], &[])),
        ]);
        let registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(PathBuf::from("/tmp/fugue-state")),
            mcp_servers,
            BTreeMap::new(),
        )
        .with_final_review_reviewers(vec![FinalReviewReviewerConfig {
            id: "architecture".to_string(),
            profile: "final_architecture_reviewer".to_string(),
            required: true,
        }]);
        let definition = registry.mob_definition().expect("valid definition");
        assert!(
            !definition
                .profiles
                .contains_key(&ProfileName::from("final_architecture_reviewer"))
        );
        let binding = definition
            .profiles
            .get(&ProfileName::from(
                IndependentReviewConfig::default().orchestrator_profile,
            ))
            .expect("shared review coordinator");
        let ProfileBinding::Inline(profile) = binding else {
            panic!("final reviewer profile must be inline")
        };

        assert!(profile.tools.shell);
        assert!(profile.tools.workgraph);
        assert!(profile.tools.mcp_servers.is_empty());
        assert_eq!(profile.runtime_mode, MobRuntimeMode::AutonomousHost);
    }

    #[test]
    fn coordinator_and_review_profiles_honor_the_workspace_model() {
        for model in [None, Some("gpt-5.5"), Some("custom-model")] {
            let mut config = test_meerkat_config(PathBuf::from("/tmp/fugue-state"));
            config.model = model.map(str::to_owned);
            let registry =
                MetaRuntimeRegistry::with_templates(config, BTreeMap::new(), BTreeMap::new())
                    .with_final_review_reviewers(vec![FinalReviewReviewerConfig {
                        id: "tests".into(),
                        profile: "final_tests".into(),
                        required: true,
                    }])
                    .with_independent_review_config(IndependentReviewConfig {
                        enabled: true,
                        orchestrator_profile: "review_orchestrator".into(),
                        ..Default::default()
                    });
            let definition = registry.mob_definition().expect("valid definition");
            for name in [KAPELLMEISTER, ISSUE_LEAD, "review_orchestrator"] {
                let ProfileBinding::Inline(profile) =
                    &definition.profiles[&ProfileName::from(name)]
                else {
                    panic!("generated profile must be inline")
                };
                assert_eq!(
                    profile.model,
                    model.unwrap_or_else(default_agent_model),
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn mob_definition_raises_native_orphaned_turn_limit() {
        let registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(PathBuf::from("/tmp/fugue-state")),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let definition = registry.mob_definition().expect("valid definition");

        assert_eq!(
            definition
                .limits
                .as_ref()
                .and_then(|limits| limits.max_orphaned_turns),
            Some(DEFAULT_NATIVE_MAX_ORPHANED_TURNS)
        );
        const { assert!(DEFAULT_NATIVE_MAX_ORPHANED_TURNS >= 100) };
    }

    #[test]
    fn mob_definition_registers_independent_review_orchestrator_profile() {
        let mcp_servers = BTreeMap::from([
            ("linear".to_string(), mcp_server(&["fugue-linear-mcp"], &[])),
            ("github".to_string(), mcp_server(&["fugue-github-mcp"], &[])),
        ]);
        let mut review = IndependentReviewConfig {
            enabled: true,
            ..IndependentReviewConfig::default()
        };
        review.orchestrator_profile = "review_orchestrator".to_string();
        let registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(PathBuf::from("/tmp/fugue-state")),
            mcp_servers,
            BTreeMap::new(),
        )
        .with_independent_review_config(review);
        let definition = registry.mob_definition().expect("valid definition");
        let binding = definition
            .profiles
            .get(&ProfileName::from("review_orchestrator"))
            .expect("independent review orchestrator profile");
        let ProfileBinding::Inline(profile) = binding else {
            panic!("independent review orchestrator profile must be inline")
        };

        assert!(profile.tools.mob);
        assert!(profile.tools.shell);
        assert!(profile.tools.workgraph);
        assert!(profile.tools.mcp_servers.is_empty());
        assert_eq!(profile.runtime_mode, MobRuntimeMode::AutonomousHost);
    }

    #[test]
    fn turn_limit_issue_lead_message_delegates_recovery_decision() {
        let message = turn_limit_issue_lead_message(
            "LUC-642",
            "LUC-642/coder",
            "Terminal failure: Failed (TurnLimitReached): turn limit reached",
        );

        assert!(message.contains("LUC-642"));
        assert!(message.contains("LUC-642/coder"));
        assert!(message.contains("fresh issue-lead turn"));
        assert!(message.contains("runtime delivery counter is reset"));
        assert!(message.contains("steer the member to continue"));
        assert!(message.contains("move the issue to Blocked"));
        assert!(message.contains("advance the pipeline"));
    }

    #[test]
    fn member_terminal_failure_issue_lead_message_delegates_recovery_decision() {
        let message = member_terminal_failure_issue_lead_message(
            "LUC-654",
            "LUC-654/coder",
            "Terminal failure: Failed (LlmFailure): LLM failure terminal turn",
        );

        assert!(message.contains("LUC-654"));
        assert!(message.contains("LUC-654/coder"));
        assert!(message.contains("not evidence that the issue work is complete"));
        assert!(message.contains("steer or respawn"));
        assert!(message.contains("move the issue to Blocked"));
        assert!(message.contains("Preserve existing workspace changes"));
    }

    #[test]
    fn console_turn_limit_detector_accepts_structured_terminal_cause() {
        let frame = meerkat_meta::ConsoleFrame {
            id: "frame-1".to_string(),
            cursor: meerkat_meta::ConsoleCursor::from("console:42"),
            dedupe_key: "frame-1".to_string(),
            timestamp_ms: 1,
            runtime_key: "runtime".to_string(),
            identity: "LUC-642/coder".to_string(),
            conversation_id: None,
            session_id: None,
            kind: "interaction_failed".to_string(),
            status: meerkat_meta::ConsoleFrameStatus::Completed,
            frame_version: 1,
            updated_at_ms: None,
            payload: serde_json::json!({
                "error_class": "max_turns",
                "terminal_cause_kind": "turn_limit_reached",
                "error": "Terminal failure: Failed (TurnLimitReached): turn limit reached"
            }),
            source: meerkat_meta::ConsoleFrameSource {
                kind: meerkat_meta::ConsoleFrameSourceKind::ConsoleEvent,
                source_cursor: None,
                member_provenance: None,
            },
            source_event_id: None,
            interaction_id: None,
            turn_id: None,
            run_id: None,
            parent_frame_id: None,
            caused_by_frame_id: None,
        };

        assert!(console_frame_is_turn_limit_failure(&frame));
    }

    #[test]
    fn console_terminal_failure_detector_accepts_llm_failure_frame() {
        let frame = meerkat_meta::ConsoleFrame {
            id: "frame-llm".to_string(),
            cursor: meerkat_meta::ConsoleCursor::from("console:43"),
            dedupe_key: "frame-llm".to_string(),
            timestamp_ms: 1,
            runtime_key: "runtime".to_string(),
            identity: "LUC-654/coder".to_string(),
            conversation_id: None,
            session_id: None,
            kind: "interaction_failed".to_string(),
            status: meerkat_meta::ConsoleFrameStatus::Completed,
            frame_version: 1,
            updated_at_ms: None,
            payload: serde_json::json!({
                "type": "run_failed",
                "error": "Terminal failure: Failed (LlmFailure): LLM failure terminal turn"
            }),
            source: meerkat_meta::ConsoleFrameSource {
                kind: meerkat_meta::ConsoleFrameSourceKind::ConsoleEvent,
                source_cursor: None,
                member_provenance: None,
            },
            source_event_id: None,
            interaction_id: None,
            turn_id: None,
            run_id: None,
            parent_frame_id: None,
            caused_by_frame_id: None,
        };

        assert_eq!(
            console_frame_member_failure(&frame),
            Some(MemberFailureKind::LlmFailure)
        );
        assert!(!console_frame_is_turn_limit_failure(&frame));
    }

    #[test]
    fn profiles_do_not_register_unconfigured_linear_mcp_module() {
        let worker = agent_profile(
            &native_agent_profile(&["mob", "mcp:linear"]),
            &BTreeMap::new(),
        )
        .expect("valid profile");

        assert!(worker.tools.mcp.is_empty());
        assert!(worker.tools.mob);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_runtime_bootstrap_is_single_flight() {
        if !isolated_runtime_test("concurrent_runtime_bootstrap_is_single_flight") {
            return;
        }
        let temp = tempfile::tempdir().expect("tempdir");
        let registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(temp.path().join("state")),
            BTreeMap::new(),
            BTreeMap::new(),
        );

        let runtime_call_registry = registry.clone();
        let bootstrap_registry = registry.clone();
        let duplicate_bootstrap_registry = registry.clone();
        let (runtime_result, bootstrap_result, duplicate_bootstrap_result, router_result) = tokio::join!(
            async move { runtime_call_registry.runtime().await },
            async move { bootstrap_registry.bootstrap_repo_kapellmeister().await },
            async move {
                duplicate_bootstrap_registry
                    .bootstrap_repo_kapellmeister()
                    .await
            },
            registry.reference_console_router::<()>(RuntimeDecisionState::local_console(
                meerkat_meta::ConsolePolicy::default(),
                None,
            ))
        );

        let runtime = runtime_result.expect("runtime initialization should not race");
        bootstrap_result.expect("Kapellmeister bootstrap should not hit SQLite locks");
        duplicate_bootstrap_result.expect("duplicate Kapellmeister bootstrap should be serialized");
        let _router = router_result.expect("console router should share the initialized runtime");

        let mut spawned = Vec::new();
        for _ in 0..500 {
            spawned = runtime
                .query_mob_events(&EventQuery {
                    mob_id: Some(MOB_ID.to_string()),
                    event_types: vec!["member_spawned".to_string()],
                    limit: Some(16),
                    ..EventQuery::default()
                })
                .await
                .expect("query structural events")
                .into_iter()
                .filter_map(|event| event.agent_identity)
                .collect::<Vec<_>>();
            if spawned.iter().any(|identity| identity == KAPELLMEISTER) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(
            spawned.iter().any(|identity| identity == KAPELLMEISTER),
            "repo Kapellmeister should be visible through the canonical Meta runtime"
        );
        registry.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn repo_host_signal_reaches_native_coordinator_history() {
        if !isolated_runtime_test("repo_host_signal_reaches_native_coordinator_history") {
            return;
        }
        let temp = tempfile::tempdir().expect("tempdir");
        let mut registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(temp.path().join("state")),
            BTreeMap::new(),
            BTreeMap::new(),
        );
        registry
            .bootstrap_repo_kapellmeister()
            .await
            .expect("initial coordinator");
        // A failed peer-wiring pass must not prevent subsequent diagnostics.
        let original_meerkat = registry.meerkat.clone();
        registry.meerkat.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
            repository: crate::ensemble::config::RepoId::try_from("unavailable-peer".to_owned())
                .expect("repo id"),
            state_path: temp.path().join("state"),
        });
        assert!(registry.bootstrap_repo_kapellmeister().await.is_err());
        registry.meerkat = original_meerkat;
        let message = "Fugue host signal fixture: worker completed fixture-123";
        let scope = crate::host_signal_journal::HostSignalScope {
            repository_path: temp.path().to_owned(),
            runtime_path: std::path::absolute(registry.state_path()).unwrap(),
            mob_id: registry.meerkat.mob_id().into_owned(),
            member: KAPELLMEISTER.to_owned(),
            tracker: crate::host_signal_journal::TrackerScope::Linear {
                endpoint: "https://api.linear.app/graphql".to_owned(),
                project: "fixture".to_owned(),
            },
        };
        registry.host_signal_binding = Some(HostSignalBinding {
            path: temp.path().join("signals.sqlite3"),
            scope: scope.clone(),
        });
        let signal = crate::host_signal_journal::PendingSignal {
            key: crate::host_signal_journal::DeliveryKey("fugue-history-fixture".to_owned()),
            envelope: crate::host_signal_journal::SignalEnvelope {
                content: message.to_owned(),
                mode: crate::host_signal_journal::SignalHandlingMode::Steer,
            },
        };
        let accepted = registry
            .deliver_repo_kapellmeister_signal(&scope, &signal)
            .await
            .expect("native signal acceptance");
        assert!(!accepted.delivery_id.is_empty());
        let runtime = registry.runtime().await.expect("runtime");
        let member = runtime
            .mob_handle()
            .get_member(&AgentIdentity::from(KAPELLMEISTER))
            .await
            .expect("member query")
            .expect("coordinator");
        let session = member.bridge_session_id().expect("coordinator session");
        let delivered = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let history = runtime
                    .mob_runtime()
                    .read_session_history(&session.to_string(), 0, None)
                    .await
                    .expect("canonical coordinator history");
                if serde_json::to_string(&history.messages)
                    .expect("serialize messages")
                    .contains(message)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        registry.shutdown().await.expect("shutdown");
        delivered.expect("host signal must reach the native coordinator session");
    }

    static HOST_SIGNAL_BLOCKING_CLIENT: OnceLock<Arc<HostSignalBlockingClient>> = OnceLock::new();

    pub(super) fn runtime_test_llm_client() -> Arc<dyn meerkat_llm_core::LlmClient> {
        match HOST_SIGNAL_BLOCKING_CLIENT.get() {
            Some(client) => Arc::clone(client) as Arc<dyn meerkat_llm_core::LlmClient>,
            None => Arc::new(meerkat_llm_core::TestClient::default()),
        }
    }

    #[derive(Default)]
    struct HostSignalBlockingClient {
        block: AtomicBool,
        entered: tokio::sync::Notify,
        normal: meerkat_llm_core::TestClient,
    }

    #[async_trait]
    impl meerkat_llm_core::LlmClient for HostSignalBlockingClient {
        fn project_replay_messages(
            &self,
            messages: &[meerkat_core::Message],
        ) -> Result<Vec<meerkat_core::Message>, meerkat_llm_core::LlmError> {
            self.normal.project_replay_messages(messages)
        }

        fn stream<'a>(
            &'a self,
            request: &'a meerkat_llm_core::LlmRequest,
        ) -> meerkat_llm_core::LlmStream<'a> {
            if self.block.load(Ordering::SeqCst) {
                self.entered.notify_one();
                Box::pin(futures::stream::pending())
            } else {
                self.normal.stream(request)
            }
        }

        fn provider(&self) -> meerkat_core::Provider {
            meerkat_core::Provider::Other
        }

        async fn health_check(&self) -> Result<(), meerkat_llm_core::LlmError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admitted_host_signal_queued_at_stop_remains_recoverable() {
        const NAME: &str = "admitted_host_signal_queued_at_stop_remains_recoverable";
        if !isolated_runtime_test(NAME) {
            return;
        }
        assert_queued_host_signal_recovery(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admitted_host_signal_acknowledged_at_stop_remains_recoverable() {
        const NAME: &str = "admitted_host_signal_acknowledged_at_stop_remains_recoverable";
        if !isolated_runtime_test(NAME) {
            return;
        }
        assert_queued_host_signal_recovery(true).await;
    }

    async fn assert_queued_host_signal_recovery(acknowledge_before_stop: bool) {
        use crate::host_signal_journal::{
            HostSignalScope, SignalEnvelope, SignalHandlingMode, TrackerScope,
        };
        use meerkat_contracts::WireContinuationStatus;
        use meerkat_runtime::{
            InputAbandonReason, InputLifecycleState, InputTerminalOutcome, SessionServiceRuntimeExt,
        };
        let evidence = host_signal_evidence("host-signal-queued-stop-");
        let root = evidence.0.as_ref().unwrap().path();
        let client = Arc::new(HostSignalBlockingClient::default());
        client.block.store(true, Ordering::SeqCst);
        assert!(HOST_SIGNAL_BLOCKING_CLIENT.set(Arc::clone(&client)).is_ok());
        let config = test_meerkat_config(root.join("state"));
        let scope = HostSignalScope {
            repository_path: root.to_owned(),
            runtime_path: meta_runtime_state_path(&config),
            mob_id: config.mob_id().into_owned(),
            member: KAPELLMEISTER.to_owned(),
            tracker: TrackerScope::Internal {
                database: root.join("issues.sqlite3"),
                repository: "fixture".into(),
            },
        };
        let binding = HostSignalBinding {
            path: root.join("signals.sqlite3"),
            scope: scope.clone(),
        };
        let registry =
            MetaRuntimeRegistry::with_templates(config.clone(), BTreeMap::new(), BTreeMap::new())
                .with_host_signal_binding(binding.clone());
        registry.bootstrap_repo_kapellmeister().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), client.entered.notified())
            .await
            .expect("bootstrap provider request must be in flight");
        let mut journal = binding.open().unwrap();
        let mut update = journal.begin().unwrap();
        update
            .enqueue_once("queued-stop-fixture", || {
                Ok(SignalEnvelope {
                    content: "notification queued during a provider request".into(),
                    mode: SignalHandlingMode::Steer,
                })
            })
            .unwrap();
        update.commit().unwrap();
        drop(journal);
        let saved = binding.open().unwrap().pending(1).unwrap().remove(0);
        let admitted = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let receipt = registry
                    .deliver_repo_kapellmeister_signal(&scope, &saved)
                    .await
                    .unwrap();
                match receipt.status {
                    WireContinuationStatus::Admitted { .. } => break receipt,
                    WireContinuationStatus::Pending => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    _ => panic!("unexpected terminal native admission: {receipt:?}"),
                }
            }
        })
        .await
        .expect("native admission while the bootstrap provider request is held");
        let (session_id, input_id) = match &admitted.status {
            WireContinuationStatus::Admitted {
                session_id,
                input_id,
            } => (
                meerkat_core::SessionId::parse(session_id).unwrap(),
                meerkat_core::lifecycle::InputId(uuid::Uuid::parse_str(input_id).unwrap()),
            ),
            other => panic!("expected native admission: {other:?}"),
        };
        let runtime = registry.runtime().await.unwrap();
        let machine = runtime
            .mob_runtime()
            .session_service()
            .unwrap()
            .acquire_runtime_adapter(None)
            .unwrap()
            .unwrap();
        let queued = machine
            .input_state(&session_id, &input_id)
            .await
            .unwrap()
            .unwrap();
        std::fs::write(
            root.join("native-queued-input.json"),
            serde_json::to_vec_pretty(&queued).unwrap(),
        )
        .unwrap();
        assert_eq!(queued.seed.phase, InputLifecycleState::Queued);
        assert_eq!(queued.seed.last_run_id, None);
        assert_eq!(queued.seed.attempt_count, 0);
        if acknowledge_before_stop {
            binding
                .open()
                .unwrap()
                .acknowledge(&saved.key, &admitted)
                .unwrap();
        }
        registry.shutdown().await.unwrap();
        let stopped = machine
            .input_state(&session_id, &input_id)
            .await
            .unwrap()
            .unwrap();
        std::fs::write(
            root.join("native-stopped-input.json"),
            serde_json::to_vec_pretty(&stopped).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("native-admission.json"),
            serde_json::to_vec_pretty(&admitted).unwrap(),
        )
        .unwrap();
        assert_eq!(stopped.seed.phase, InputLifecycleState::Abandoned);
        assert_eq!(
            stopped.seed.terminal_outcome,
            Some(InputTerminalOutcome::Abandoned {
                reason: InputAbandonReason::Stopped,
            })
        );
        assert_eq!(stopped.seed.last_run_id, None);
        assert_eq!(stopped.seed.attempt_count, 0);
        assert_eq!(stopped.state.stop_requeue, None);
        drop(machine);
        drop(runtime);
        drop(registry);
        client.block.store(false, Ordering::SeqCst);
        let registry =
            MetaRuntimeRegistry::with_templates(config, BTreeMap::new(), BTreeMap::new())
                .with_host_signal_binding(binding.clone());
        assert_eq!(
            registry
                .deliver_repo_kapellmeister_signal(&scope, &saved)
                .await
                .unwrap(),
            admitted,
            "native keyed replay still returns the original receipt after restart"
        );
        let result = binding
            .drain_pending(std::num::NonZeroU32::MIN, |scope, signal| {
                let registry = &registry;
                let admitted = admitted.clone();
                async move {
                    let replay = registry
                        .deliver_repo_kapellmeister_signal(&scope, &signal)
                        .await
                        .map_err(crate::error::FugueError::Workspace)?;
                    assert_eq!(replay, admitted);
                    Ok(replay)
                }
            })
            .await
            .unwrap();
        assert_eq!(
            result.acknowledged,
            u32::from(!acknowledge_before_stop),
            "native admission remains distinct from consumption"
        );
        let runtime = registry.runtime().await.unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let history = runtime
                    .mob_runtime()
                    .read_session_history(&session_id.to_string(), 0, None)
                    .await
                    .unwrap();
                if serde_json::to_string(&history.messages)
                    .unwrap()
                    .contains(&saved.envelope.content)
                {
                    assert_eq!(
                        history
                            .messages
                            .iter()
                            .filter(|entry| serde_json::to_string(entry)
                                .unwrap()
                                .contains(&saved.envelope.content))
                            .count(),
                        1
                    );
                    if binding.open().unwrap().pending(1).unwrap().is_empty()
                        && binding
                            .open()
                            .unwrap()
                            .admitted_for_reconciliation(1)
                            .unwrap()
                            .is_empty()
                    {
                        return;
                    }
                }
                reconcile_host_signal_fixture(&binding, &registry).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        registry.shutdown().await.unwrap();
        recovered.expect(
            "a queued notification must survive graceful stop and reach canonical history once",
        );
        let journal = rusqlite::Connection::open(&binding.path).unwrap();
        assert_eq!(
            journal
                .query_row("SELECT count(*) FROM replacements", [], |row| row
                    .get::<_, u32>(0))
                .unwrap(),
            1,
            "exactly one proven stopped input gets a successor key"
        );
        let final_receipt: String = journal
            .query_row("SELECT admitted_receipt FROM deliveries", [], |row| {
                row.get(0)
            })
            .unwrap();
        std::fs::write(root.join("native-recovered-admission.json"), &final_receipt).unwrap();
        let final_receipt: meerkat_contracts::WireContinuationReceipt =
            serde_json::from_str(&final_receipt).unwrap();
        assert_ne!(final_receipt.delivery_id, admitted.delivery_id);
    }

    async fn reconcile_host_signal_fixture(
        binding: &HostSignalBinding,
        registry: &MetaRuntimeRegistry,
    ) {
        let progress = binding
            .reconcile_admitted(std::num::NonZeroU32::MIN, |scope, receipt| async move {
                registry
                    .observe_repo_kapellmeister_input(&scope, &receipt)
                    .await
                    .map_err(crate::error::FugueError::Workspace)
            })
            .await
            .unwrap();
        assert!(
            progress.attention.is_empty(),
            "unexpected native terminal: {:?}",
            progress.attention
        );
        assert!(
            progress.unresolvable.is_empty(),
            "unresolvable native input: {:?}",
            progress.unresolvable
        );
        binding
            .drain_pending(std::num::NonZeroU32::MIN, |scope, signal| async move {
                registry
                    .deliver_repo_kapellmeister_signal(&scope, &signal)
                    .await
                    .map_err(crate::error::FugueError::Workspace)
            })
            .await
            .unwrap();
    }

    struct RuntimeFailureEvidence(Option<tempfile::TempDir>);
    impl Drop for RuntimeFailureEvidence {
        fn drop(&mut self) {
            // An external qualifier can reject a zero exit after inspecting
            // its transcript. It owns cleanup when an evidence root is set.
            if (std::thread::panicking() || std::env::var_os("FUGUE_TEST_EVIDENCE_DIR").is_some())
                && let Some(directory) = self.0.take()
            {
                eprintln!(
                    "Retained runtime state for qualification: {}",
                    directory.keep().display()
                );
            }
        }
    }
    fn host_signal_evidence(prefix: &str) -> RuntimeFailureEvidence {
        let directory = match std::env::var_os("FUGUE_TEST_EVIDENCE_DIR") {
            Some(parent) => tempfile::Builder::new().prefix(prefix).tempdir_in(parent),
            None => tempfile::Builder::new().prefix(prefix).tempdir(),
        }
        .expect("host signal evidence directory");
        RuntimeFailureEvidence(Some(directory))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admitted_host_signal_replays_after_restart_without_duplicate_input() {
        if !isolated_runtime_test(
            "admitted_host_signal_replays_after_restart_without_duplicate_input",
        ) {
            return;
        }
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let evidence = host_signal_evidence("host-signal-restart-");
        let directory = evidence.0.as_ref().expect("host signal evidence owner");
        assert_native_host_signal_replay(directory.path(), false, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn admitted_host_signal_replays_after_process_loss_without_duplicate_input() {
        const NAME: &str =
            "admitted_host_signal_replays_after_process_loss_without_duplicate_input";
        if !isolated_runtime_test(NAME) {
            return;
        }
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        const ROOT: &str = "FUGUE_SIGNAL_CRASH_ROOT";
        if let Some(root) = std::env::var_os(ROOT) {
            assert_native_host_signal_replay(Path::new(&root), true, false).await;
            panic!("child must exit before acknowledgement and runtime shutdown");
        }
        let evidence = host_signal_evidence("host-signal-process-loss-");
        let directory = evidence.0.as_ref().expect("host signal evidence owner");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env(ROOT, directory.path())
            .env("FUGUE_ISOLATED_RUNTIME_TEST", NAME)
            .arg(format!("meta_runtime::tests::{NAME}"))
            .args(["--exact", "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_native_host_signal_replay(directory.path(), false, true).await;
    }

    async fn assert_native_host_signal_replay(
        root: &Path,
        exit_before_ack: bool,
        restore_after_exit: bool,
    ) {
        use crate::host_signal_journal::{
            HostSignalScope, SignalEnvelope, SignalHandlingMode, TrackerScope,
        };
        use meerkat_contracts::WireContinuationStatus;
        let config = test_meerkat_config(root.join("state"));
        let scope = HostSignalScope {
            repository_path: root.to_owned(),
            runtime_path: meta_runtime_state_path(&config),
            mob_id: config.mob_id().into_owned(),
            member: KAPELLMEISTER.to_owned(),
            tracker: TrackerScope::Linear {
                endpoint: "https://api.linear.app/graphql".to_owned(),
                project: "fixture".to_owned(),
            },
        };
        let binding = HostSignalBinding {
            path: root.join("signals.sqlite3"),
            scope: scope.clone(),
        };
        let message = "host notification survives native admission before journal acknowledgement";
        {
            let mut journal = binding.open().unwrap();
            let mut update = journal.begin().unwrap();
            update
                .enqueue_once("fixture-completion", || {
                    Ok(SignalEnvelope {
                        content: message.to_owned(),
                        mode: SignalHandlingMode::Steer,
                    })
                })
                .unwrap();
            update.commit().unwrap();
        }
        let accepted: meerkat_contracts::WireContinuationReceipt = if restore_after_exit {
            serde_json::from_slice(&std::fs::read(root.join("native-admission.json")).unwrap())
                .unwrap()
        } else {
            let registry = MetaRuntimeRegistry::with_templates(
                config.clone(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .with_host_signal_binding(binding.clone());
            let saved = binding.open().unwrap().pending(1).unwrap().remove(0);
            let accepted = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let receipt = registry
                        .deliver_repo_kapellmeister_signal(&scope, &saved)
                        .await
                        .unwrap();
                    match receipt.status {
                        WireContinuationStatus::Admitted { .. } => break receipt,
                        WireContinuationStatus::Pending => {
                            tokio::time::sleep(Duration::from_millis(20)).await
                        }
                        _ => panic!("unexpected terminal admission: {receipt:?}"),
                    }
                }
            })
            .await
            .expect("native admission");
            // Native has admitted the input, but the host has not committed its ack.
            assert_eq!(
                binding.open().unwrap().pending(1).unwrap(),
                vec![saved.clone()]
            );
            let mut conflict = saved.clone();
            conflict.envelope.content.push_str(" changed");
            assert!(
                registry
                    .deliver_repo_kapellmeister_signal(&scope, &conflict)
                    .await
                    .is_err(),
                "the same key cannot commit changed content"
            );
            if exit_before_ack {
                let path = root.join("native-admission.json");
                let file = std::fs::File::create(path).unwrap();
                serde_json::to_writer(&file, &accepted).unwrap();
                file.sync_all().unwrap();
                std::fs::File::open(root).unwrap().sync_all().unwrap();
                std::process::exit(0);
            }
            registry.shutdown().await.unwrap();
            // Keep the original admission-to-shutdown timing; only persist this
            // diagnostic copy after the graceful shutdown under test.
            std::fs::write(
                root.join("native-admission.json"),
                serde_json::to_vec_pretty(&accepted).unwrap(),
            )
            .unwrap();
            drop(registry);
            accepted
        };
        let saved = binding.open().unwrap().pending(1).unwrap().remove(0);
        let registry =
            MetaRuntimeRegistry::with_templates(config, BTreeMap::new(), BTreeMap::new())
                .with_host_signal_binding(binding.clone());
        let replayed = Arc::new(Mutex::new(None));
        let progress = binding
            .drain_pending(std::num::NonZeroU32::MIN, |scope, signal| {
                let registry = &registry;
                let replayed = Arc::clone(&replayed);
                async move {
                    let receipt = registry
                        .deliver_repo_kapellmeister_signal(&scope, &signal)
                        .await
                        .map_err(crate::error::FugueError::Workspace)?;
                    *replayed.lock().unwrap() = Some(receipt.clone());
                    Ok(receipt)
                }
            })
            .await
            .unwrap();
        assert_eq!(
            replayed.lock().unwrap().as_ref(),
            Some(&accepted),
            "restart must reuse the original native receipt and input"
        );
        std::fs::write(
            root.join("native-replay.json"),
            serde_json::to_vec_pretty(&*replayed.lock().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(progress.acknowledged, 1);
        assert!(binding.open().unwrap().pending(1).unwrap().is_empty());
        let runtime = registry.runtime().await.unwrap();
        let session_id = match accepted.status {
            WireContinuationStatus::Admitted { session_id, .. } => session_id,
            _ => unreachable!(),
        };
        let mut last_history = None;
        let history_result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                reconcile_host_signal_fixture(&binding, &registry).await;
                let history = runtime
                    .mob_runtime()
                    .read_session_history(&session_id, 0, None)
                    .await
                    .unwrap();
                let serialized = serde_json::to_string(&history.messages).unwrap();
                let contains_input = serialized.contains(message);
                last_history = Some(serialized);
                if contains_input {
                    break history;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        std::fs::write(
            root.join("canonical-history.json"),
            last_history.as_deref().unwrap_or("null"),
        )
        .unwrap();
        let history = history_result.expect("admitted input in canonical history");
        assert_eq!(
            history
                .messages
                .iter()
                .filter(|entry| serde_json::to_string(entry).unwrap().contains(message))
                .count(),
            1
        );
        let mut wrong_scope = scope;
        wrong_scope.mob_id = "other-repository".into();
        assert!(
            registry
                .deliver_repo_kapellmeister_signal(&wrong_scope, &saved)
                .await
                .is_err()
        );
        registry.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_members_emit_canonical_structural_events() {
        if !isolated_runtime_test("runtime_members_emit_canonical_structural_events") {
            return;
        }
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let directory = match std::env::var_os("FUGUE_TEST_EVIDENCE_DIR") {
            Some(parent) => tempfile::Builder::new()
                .prefix("runtime-")
                .tempdir_in(parent),
            None => tempfile::tempdir(),
        }
        .expect("runtime evidence directory");
        let evidence = RuntimeFailureEvidence(Some(directory));
        let temp = evidence.0.as_ref().expect("runtime evidence owner");
        let registry = MetaRuntimeRegistry::with_templates(
            test_meerkat_config(temp.path().join("state")),
            BTreeMap::new(),
            BTreeMap::from([("implementation_mob".to_string(), test_template())]),
        );
        let runtime = registry.runtime().await.expect("runtime");
        let host = MetaRuntimeNativeMobExecutionHost::from_registry(registry.clone());

        let result = host.spawn_or_resume_plan(launch_plan());
        assert!(matches!(result, FugueToolResult::Ok { .. }), "{result:?}");

        let mut spawned = Vec::new();
        for _ in 0..500 {
            spawned = runtime
                .query_mob_events(&EventQuery {
                    mob_id: Some(MOB_ID.to_string()),
                    event_types: vec!["member_spawned".to_string()],
                    limit: Some(64),
                    ..EventQuery::default()
                })
                .await
                .expect("query structural events")
                .into_iter()
                .filter_map(|event| event.agent_identity)
                .collect::<Vec<_>>();
            let has_identity = |expected: &str| spawned.iter().any(|identity| identity == expected);
            if has_identity(KAPELLMEISTER)
                && has_identity("issue_LUC-900__issue_5Flead")
                && has_identity("issue_LUC-900__planner")
                && has_identity("issue_LUC-900__coder")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(spawned.iter().any(|identity| identity == KAPELLMEISTER));
        assert!(
            spawned
                .iter()
                .any(|identity| identity == "issue_LUC-900__issue_5Flead")
        );
        assert!(
            spawned
                .iter()
                .any(|identity| identity == "issue_LUC-900__planner")
        );
        assert!(
            spawned
                .iter()
                .any(|identity| identity == "issue_LUC-900__coder")
        );

        let mut wiring_events = Vec::new();
        for _ in 0..500 {
            wiring_events = runtime
                .query_mob_events(&EventQuery {
                    mob_id: Some(MOB_ID.to_string()),
                    event_types: vec![
                        "members_wired".to_string(),
                        "members_wired_batch".to_string(),
                    ],
                    limit: Some(64),
                    ..EventQuery::default()
                })
                .await
                .expect("query wiring events");
            if !wiring_events.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !wiring_events.is_empty(),
            "Meta should own and emit topology wiring events"
        );

        assert_eq!(
            host.cleanup_execution(&MobRunId("native-execution:LUC-900".to_string()), "test")
                .expect("cleanup retires issue members"),
            3
        );
        let mut retired = Vec::new();
        for _ in 0..500 {
            retired = runtime
                .query_mob_events(&EventQuery {
                    mob_id: Some(MOB_ID.to_string()),
                    event_types: vec!["member_retired".to_string()],
                    limit: Some(64),
                    ..EventQuery::default()
                })
                .await
                .expect("query retired events")
                .into_iter()
                .filter_map(|event| event.agent_identity)
                .collect::<Vec<_>>();
            let has_retired = [
                "issue_LUC-900__issue_5Flead",
                "issue_LUC-900__planner",
                "issue_LUC-900__coder",
            ]
            .into_iter()
            .all(|expected| retired.iter().any(|identity| identity.as_str() == expected));
            if has_retired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for retired_identity in [
            "issue_LUC-900__issue_5Flead",
            "issue_LUC-900__planner",
            "issue_LUC-900__coder",
        ] {
            assert!(
                retired.iter().any(|identity| identity == retired_identity),
                "{retired_identity} should be retired through Meta"
            );
        }
        registry.shutdown().await.expect("shutdown");
    }
    #[test]
    fn issue_run_mapping_survives_registry_recreation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = test_meerkat_config(temp.path().join("state"));
        let registry = MetaRuntimeRegistry::new(config.clone());
        let plan = launch_plan();
        let run_id = MobRunId(plan.input.idempotency_key.0.clone());
        let job = FugueMobJob::new(
            meta_runtime_run_snapshot_from_plan(&run_id, &plan, Utc::now()),
            member_identities_for_plan(&plan),
        );
        registry
            .persist_jobs(&BTreeMap::from([(run_id.0.clone(), job)]))
            .expect("persist");
        let restarted = MetaRuntimeRegistry::new(config);
        restarted.restore_jobs().expect("restore");
        let jobs = restarted.jobs.lock().expect("jobs");
        let restored = jobs.get(&run_id.0).expect("run preserved");
        assert_eq!(
            restored.member_identities,
            member_identities_for_plan(&plan)
        );
        assert_eq!(restored.run.run_id, run_id);
        assert_eq!(restored.run.status, MobRunStatus::Running);
    }

    fn failure_notification() -> (String, MobEventSnapshot) {
        (
            "failure-cursor-42".to_owned(),
            MobEventSnapshot {
                sequence: 42,
                occurred_at: Utc::now(),
                kind: MobEventKind::MemberKickoffUpdated {
                    member: "issue_LUC-900__coder".to_owned(),
                    phase: MEMBER_TERMINAL_FAILURE_PHASE.to_owned(),
                },
                message: Some("member terminal failure".to_owned()),
            },
        )
    }

    fn persisted_failure_test_host(
        config: MeerkatConfig,
    ) -> (MetaRuntimeNativeMobExecutionHost, MobRunId) {
        let registry = MetaRuntimeRegistry::new(config);
        let plan = launch_plan();
        let run_id = MobRunId(plan.input.idempotency_key.0.clone());
        let job = FugueMobJob::new(
            meta_runtime_run_snapshot_from_plan(&run_id, &plan, Utc::now()),
            member_identities_for_plan(&plan),
        );
        registry
            .persist_jobs(&BTreeMap::from([(run_id.0.clone(), job)]))
            .expect("persist run");
        registry.restore_jobs().expect("load run");
        (
            MetaRuntimeNativeMobExecutionHost::from_registry(registry),
            run_id,
        )
    }

    #[test]
    fn launch_failure_restart_listing_reads_past_first_page_and_keeps_terminal_evidence() {
        let temp = tempfile::tempdir().expect("tempdir");
        let registry = MetaRuntimeRegistry::new(test_meerkat_config(temp.path().join("state")));
        let plan = launch_plan();
        let jobs = (0..205)
            .map(|index| {
                let run_id = MobRunId(format!("run-{index:03}"));
                let mut run = meta_runtime_run_snapshot_from_plan(&run_id, &plan, Utc::now());
                run.status = if index == 204 {
                    MobRunStatus::Succeeded
                } else {
                    MobRunStatus::Failed
                };
                (
                    run_id.0,
                    FugueMobJob::new(run, member_identities_for_plan(&plan)),
                )
            })
            .collect();
        registry.persist_jobs(&jobs).expect("persist pages");
        let host = MetaRuntimeNativeMobExecutionHost::from_registry(registry);
        let runs = crate::orchestrator::list_native_runs_for_restart(&host).expect("all pages");
        assert_eq!(runs.len(), 205);
        assert_eq!(runs.last().expect("last page").run_id.0, "run-204");
        assert_eq!(
            runs.last().expect("terminal evidence").status,
            MobRunStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn launch_failure_from_async_admission_is_inspectable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = test_meerkat_config(temp.path().join("state"));
        config.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
            repository: crate::ensemble::config::RepoId::try_from("not-attached".to_owned())
                .expect("repo id"),
            state_path: temp.path().join("state"),
        });
        let registry = MetaRuntimeRegistry::new(config);
        let host = MetaRuntimeNativeMobExecutionHost::from_registry(registry.clone());
        let plan = launch_plan();
        let run_id = MobRunId(plan.input.idempotency_key.0.clone());
        assert!(matches!(
            host.spawn_or_resume_plan(plan),
            FugueToolResult::Ok { .. }
        ));
        let failure = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let FugueToolResult::Ok {
                    output: FugueToolOutput::MobInspectRun(output),
                    ..
                } = host.status(InspectRunInput {
                    run_id: run_id.clone(),
                }) && let Some(failure) = output.launch_failure
                {
                    assert_eq!(output.run.status, MobRunStatus::Failed);
                    break failure;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("async failure should be published");
        assert_eq!(
            failure.message,
            "Repository formation has not attached its shared mob yet"
        );
        assert!(
            registry.runtime.lock().await.is_none(),
            "no replacement runtime"
        );
    }

    #[test]
    fn launch_failure_write_error_preserves_last_durable_status() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = test_meerkat_config(temp.path().join("state"));
        let (host, run_id) = persisted_failure_test_host(config.clone());
        let db = rusqlite::Connection::open(host.registry.state_path().join("fugue-runs.sqlite3"))
            .expect("store");
        db.execute_batch("CREATE TRIGGER reject_update BEFORE DELETE ON fugue_runs BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").expect("inject failure");
        assert!(
            host.set_launch_failure(&run_id, "wiring failed".to_owned())
                .is_err()
        );
        let job = host.registry.jobs.lock().expect("jobs")[&run_id.0].clone();
        assert_eq!(job.run.status, MobRunStatus::Running);
        assert!(job.launch_failure.is_none());
        let restarted = MetaRuntimeRegistry::new(config);
        restarted.restore_jobs().expect("restore");
        assert!(
            restarted.jobs.lock().expect("jobs")[&run_id.0]
                .launch_failure
                .is_none()
        );
    }

    #[test]
    fn launch_failure_survives_restart_with_its_original_detail() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = test_meerkat_config(temp.path().join("state"));
        let (host, run_id) = persisted_failure_test_host(config.clone());
        let detail = "wire planner -> specialist: wire requires comms runtime";
        host.set_launch_failure(&run_id, detail.to_owned())
            .expect("persist failure");
        let restarted =
            MetaRuntimeNativeMobExecutionHost::from_registry(MetaRuntimeRegistry::new(config));
        let FugueToolResult::Ok {
            output: FugueToolOutput::MobInspectRun(output),
            ..
        } = restarted.status(InspectRunInput { run_id })
        else {
            panic!("inspect restored run")
        };
        assert_eq!(output.run.status, MobRunStatus::Failed);
        let failure = output
            .launch_failure
            .expect("admission error must remain inspectable");
        assert_eq!(failure.message, detail);
        assert_eq!(output.run.completed_at, Some(failure.occurred_at));
    }

    #[test]
    fn member_failure_notification_is_durable_before_return_and_deduplicated_after_restart() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = test_meerkat_config(temp.path().join("state"));
        let (host, run_id) = persisted_failure_test_host(config.clone());
        assert_eq!(
            host.persist_unreported_member_failure_events(&run_id, vec![failure_notification()])
                .expect("persist notification")
                .len(),
            1
        );
        // No intervening status update, shutdown, or other job persistence.
        let restarted = MetaRuntimeRegistry::new(config);
        restarted.restore_jobs().expect("restore");
        assert!(
            restarted.jobs.lock().expect("jobs")[&run_id.0]
                .notified_member_failure_cursors
                .contains("failure-cursor-42")
        );
        let host = MetaRuntimeNativeMobExecutionHost::from_registry(restarted);
        assert!(
            host.persist_unreported_member_failure_events(&run_id, vec![failure_notification()])
                .expect("reobserve persisted failure")
                .is_empty()
        );
    }

    #[test]
    fn failed_member_failure_persistence_does_not_consume_notification() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (host, run_id) =
            persisted_failure_test_host(test_meerkat_config(temp.path().join("state")));
        let db = rusqlite::Connection::open(host.registry.state_path().join("fugue-runs.sqlite3"))
            .expect("open run store");
        db.execute_batch(
            "CREATE TRIGGER reject_update BEFORE DELETE ON fugue_runs
             BEGIN SELECT RAISE(ABORT, 'injected persistence failure'); END;",
        )
        .expect("inject failure");
        assert!(
            host.persist_unreported_member_failure_events(&run_id, vec![failure_notification()])
                .expect_err("failed persistence must refuse notification")
                .contains("injected persistence failure")
        );
        assert!(
            host.registry.jobs.lock().expect("jobs")[&run_id.0]
                .notified_member_failure_cursors
                .is_empty()
        );
        db.execute_batch("DROP TRIGGER reject_update;")
            .expect("restore persistence");
        assert_eq!(
            host.persist_unreported_member_failure_events(&run_id, vec![failure_notification()])
                .expect("retry the same failure")
                .len(),
            1
        );
    }

    #[test]
    fn invalid_durable_run_record_refuses_recovery() {
        let temp = tempfile::tempdir().expect("tempdir");
        let registry = MetaRuntimeRegistry::new(test_meerkat_config(temp.path().join("state")));
        registry
            .persist_jobs(&BTreeMap::new())
            .expect("create store");
        let db = rusqlite::Connection::open(registry.state_path().join("fugue-runs.sqlite3"))
            .expect("open");
        db.execute("INSERT INTO fugue_runs VALUES ('broken', 'not json')", [])
            .expect("corrupt fixture");
        assert!(
            registry
                .restore_jobs()
                .expect_err("must fail closed")
                .contains("decode Fugue run record")
        );
    }

    #[test]
    fn profile_connects_only_its_selected_mcp_servers() {
        let servers = BTreeMap::from([
            ("selected".to_string(), mcp_server(&["selected-mcp"], &[])),
            ("private".to_string(), mcp_server(&["private-mcp"], &[])),
        ]);
        let profile =
            agent_profile(&native_agent_profile(&["mcp:selected"]), &servers).expect("profile");
        assert_eq!(profile.tools.mcp_servers.len(), 1);
        assert_eq!(profile.tools.mcp_servers[0].name, "selected");
    }
    #[test]
    fn workspace_context_does_not_grant_disabled_tool_categories() {
        let temp = tempfile::tempdir().expect("tempdir");
        let tools = workspace_bounded_tool_dispatcher(
            temp.path(),
            Arc::new(StaticToolDispatcher::with_tool("linear_inspect_issue")),
            false,
            false,
            None,
        )
        .expect("dispatcher");
        let names = tools.tools();
        assert!(names.iter().any(|tool| tool.name == "linear_inspect_issue"));
        assert!(
            !names
                .iter()
                .any(|tool| tool.name == "shell" || tool.name == "apply_patch")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persistent_native_member_is_restored_before_new_spawn() {
        if !isolated_runtime_test("persistent_native_member_is_restored_before_new_spawn") {
            return;
        }
        let temp = tempfile::tempdir().expect("tempdir");
        let config = test_meerkat_config(temp.path().join("state"));
        let registry = MetaRuntimeRegistry::new(config.clone());
        registry
            .bootstrap_repo_kapellmeister()
            .await
            .expect("bootstrap");
        let runtime = registry.runtime().await.expect("runtime");
        let before = runtime
            .mob_handle()
            .get_member(&AgentIdentity::from(KAPELLMEISTER))
            .await
            .expect("member query")
            .expect("member");
        registry.shutdown().await.expect("shutdown");
        drop(runtime);
        drop(registry);
        let restarted = MetaRuntimeRegistry::new(config);
        let runtime = restarted.runtime().await.expect("restart");
        let after = runtime
            .mob_handle()
            .get_member(&AgentIdentity::from(KAPELLMEISTER))
            .await
            .expect("restored member query")
            .expect("member restored from journal");
        assert_eq!(before.agent_identity, after.agent_identity);
        assert!(before.bridge_session_id().is_some());
        assert_eq!(before.bridge_session_id(), after.bridge_session_id());
        restarted.shutdown().await.expect("restart shutdown");
    }
}
