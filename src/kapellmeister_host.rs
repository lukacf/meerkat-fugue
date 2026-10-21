//! Guarded Kapellmeister host-decision execution.
//!
//! This module is intentionally narrow. It only executes the Stage 4B
//! Kapellmeister decision contract when guarded authority is enabled, the
//! issue/repository context matches explicit safe selectors, and the selected
//! action is one of the idempotent host mutations allowed for this stage.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};

use crate::config::{MaterializedMobTemplate, MobExecutionBackend};
use crate::error::{FugueError, Result};
use crate::final_pr_gate::FinalPrGateStartRequest;
use crate::github_tools::GitHubToolHost;
use crate::independent_review::IndependentReviewStartRequest;
use crate::kapellmeister::{
    KapellmeisterAuthorityMode, KapellmeisterDecisionContext, KapellmeisterDecisionResponse,
    KapellmeisterHostDecision, KapellmeisterSafeSelectors, KapellmeisterToolAuthority,
};
use crate::linear_tools::{LinearToolExecutor, LinearToolFailure};
use crate::mob_tools::{NativeMobLaunchPlan, NativeMobToolHost};
use crate::tools::{
    ClaimIssueInput, FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolOutput,
    FugueToolResult, GitHubRepoRef, IdempotencyKey, LinearIssueRef, MergePrInput, MoveIssueInput,
    RetryAdvice, SpawnOrResumeExecutionInput, ToolExecutionMetadata, UpsertWorkpadInput,
};
use crate::tracker::LinearClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KapellmeisterExecutionMode {
    Guarded,
    ShadowOnly { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KapellmeisterHostExecutionOutcome {
    pub mode: KapellmeisterExecutionMode,
    pub operation: Option<FugueToolOperation>,
    pub result: Option<FugueToolResult>,
}

impl KapellmeisterHostExecutionOutcome {
    fn guarded(operation: FugueToolOperation, result: FugueToolResult) -> Self {
        Self {
            mode: KapellmeisterExecutionMode::Guarded,
            operation: Some(operation),
            result: Some(result),
        }
    }

    fn shadow_only(reason: impl Into<String>) -> Self {
        Self {
            mode: KapellmeisterExecutionMode::ShadowOnly {
                reason: reason.into(),
            },
            operation: None,
            result: None,
        }
    }
}

#[async_trait]
pub trait KapellmeisterHostToolExecutor {
    async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult;
    async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult;
    async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult;
    async fn spawn_or_resume_execution(
        &mut self,
        input: SpawnOrResumeExecutionInput,
    ) -> FugueToolResult;
    async fn spawn_or_resume_final_pr_gate(
        &mut self,
        request: FinalPrGateStartRequest,
    ) -> FugueToolResult;
    async fn spawn_or_resume_independent_review(
        &mut self,
        request: IndependentReviewStartRequest,
    ) -> FugueToolResult;
    async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult;
}

pub struct NativeKapellmeisterHostToolExecutor<'a> {
    linear: LinearToolExecutor<'a>,
    mob: NativeMobToolHost,
    github: GitHubToolHost,
}

impl<'a> NativeKapellmeisterHostToolExecutor<'a> {
    pub fn new(linear_client: &'a LinearClient) -> Self {
        Self {
            linear: LinearToolExecutor::new(linear_client),
            mob: NativeMobToolHost::new(),
            github: GitHubToolHost::new(),
        }
    }
}

#[async_trait]
impl KapellmeisterHostToolExecutor for NativeKapellmeisterHostToolExecutor<'_> {
    async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult {
        let key = input.idempotency_key.clone();
        wrap_linear_result(
            FugueToolOperation::LinearClaimIssue,
            Some(key),
            self.linear
                .claim_issue(input)
                .await
                .map(FugueToolOutput::LinearClaimIssue),
        )
    }

    async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult {
        let key = input.idempotency_key.clone();
        wrap_linear_result(
            FugueToolOperation::LinearMoveIssue,
            Some(key),
            self.linear
                .move_issue(input)
                .await
                .map(FugueToolOutput::LinearMoveIssue),
        )
    }

    async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult {
        let key = input.idempotency_key.clone();
        wrap_linear_result(
            FugueToolOperation::LinearUpsertWorkpad,
            Some(key),
            self.linear
                .upsert_workpad(input)
                .await
                .map(FugueToolOutput::LinearUpsertWorkpad),
        )
    }

    async fn spawn_or_resume_execution(
        &mut self,
        input: SpawnOrResumeExecutionInput,
    ) -> FugueToolResult {
        self.mob.spawn_or_resume_execution(input)
    }

    async fn spawn_or_resume_final_pr_gate(
        &mut self,
        request: FinalPrGateStartRequest,
    ) -> FugueToolResult {
        let template = final_pr_gate_native_launch_template(&request, &[]);
        let plan = match NativeMobLaunchPlan::new(
            request.spawn_input.clone(),
            template,
            BTreeMap::new(),
            BTreeSet::new(),
            None,
            Some(request.pr.repo.clone()),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                return FugueToolResult::Err {
                    error,
                    metadata: ToolExecutionMetadata::default(),
                };
            }
        };
        self.mob.spawn_or_resume_plan(plan)
    }

    async fn spawn_or_resume_independent_review(
        &mut self,
        request: IndependentReviewStartRequest,
    ) -> FugueToolResult {
        let template = independent_review_native_launch_template(&request, &[]);
        let plan = match NativeMobLaunchPlan::new(
            request.spawn_input.clone(),
            template,
            BTreeMap::new(),
            BTreeSet::new(),
            None,
            Some(request.pull_request.repo.clone()),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                return FugueToolResult::Err {
                    error,
                    metadata: ToolExecutionMetadata::default(),
                };
            }
        };
        self.mob.spawn_or_resume_plan(plan)
    }

    async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult {
        self.github.merge_pr(input)
    }
}

fn final_pr_gate_native_launch_template(
    request: &FinalPrGateStartRequest,
    mcp_tools: &[String],
) -> MaterializedMobTemplate {
    let members = request
        .reviewers
        .iter()
        .map(|reviewer| crate::config::MaterializedMobMember {
            name: reviewer.id.clone(),
            profile: reviewer.profile.clone(),
            agent: crate::config::NativeAgentProfile {
                model: None,
                provider: None,
                auth_binding: None,
                max_tokens: None,
                system_prompt: None,
                output_schema: None,
                structured_output_retries: None,
                provider_params: None,
                tools: review_agent_tools(["mob", "shell", "comms"], mcp_tools),
                skills: Vec::new(),
                additional_instructions: Some(vec![
                    "You are a repo-level final PR reviewer. Emit typed final-review verdicts only. Do not merge.".to_string(),
                ]),
            },
        })
        .collect::<Vec<_>>();
    MaterializedMobTemplate {
        backend: MobExecutionBackend::NativeExecution,
        members,
        flow: None,
        linear_projection: None,
        max_review_cycles: None,
    }
}

pub fn independent_review_native_launch_template(
    request: &IndependentReviewStartRequest,
    mcp_tools: &[String],
) -> MaterializedMobTemplate {
    let member = crate::config::MaterializedMobMember {
        name: "review_orchestrator".to_string(),
        profile: request.orchestrator_profile.clone(),
        agent: crate::config::NativeAgentProfile {
            model: None,
            provider: None,
            auth_binding: None,
            max_tokens: None,
            system_prompt: None,
            output_schema: None,
            structured_output_retries: None,
            provider_params: None,
            tools: review_agent_tools(["mob", "shell", "comms", "workgraph"], mcp_tools),
            skills: Vec::new(),
            additional_instructions: Some(vec![format!(
                "You are the Fugue independent review orchestrator for PR #{} ({}). Inspect the issue, Workpad, PR metadata, touched modules, and WorkGraph obligations yourself. Choose exactly three independent reviewer roles/focus areas, record the choices and rationale, then create reviewer obligation items in one WorkGraph scope and keep the returned realm_id and namespace. Spawn those reviewers using Meta native mob/delegate tooling. Do not pre-chew the diff for reviewers; instruct each reviewer to inspect the diff against base using shell/GitHub tools and emit a typed verdict whose schema_version is exactly {:?}. When you have all three verdicts or hit the bounded timeout, call fugue_complete_independent_review with an IndependentReviewCompletionRequest; include workgraph_scope copied exactly from the reviewer obligation items so Fugue records any red-review blockers in the same realm/namespace. Fugue then records the Workpad summary, review comment, WorkGraph blockers, and configured lane move exactly once. Do not use any alternate schema string, and do not call completion without workgraph_scope.",
                request.pull_request.number.0,
                request.pull_request.url,
                crate::independent_review::INDEPENDENT_REVIEW_SCHEMA_VERSION
            )]),
        },
    };
    MaterializedMobTemplate {
        backend: MobExecutionBackend::NativeExecution,
        members: vec![member],
        flow: None,
        linear_projection: None,
        max_review_cycles: None,
    }
}

fn review_agent_tools<const N: usize>(base_tools: [&str; N], mcp_tools: &[String]) -> Vec<String> {
    let mut tools = base_tools
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for tool in mcp_tools {
        if !tools.iter().any(|existing| existing == tool) {
            tools.push(tool.clone());
        }
    }
    tools
}

fn wrap_linear_result(
    operation: FugueToolOperation,
    _idempotency_key: Option<IdempotencyKey>,
    result: std::result::Result<FugueToolOutput, LinearToolFailure>,
) -> FugueToolResult {
    match result {
        Ok(output) => FugueToolResult::Ok {
            output,
            metadata: ToolExecutionMetadata::default(),
        },
        Err(error) => FugueToolResult::Err {
            error: error.into_error(operation),
            metadata: ToolExecutionMetadata::default(),
        },
    }
}

pub async fn execute_guarded_kapellmeister_decision(
    authority: &KapellmeisterToolAuthority,
    context: &KapellmeisterDecisionContext,
    response: KapellmeisterDecisionResponse,
    executor: &mut impl KapellmeisterHostToolExecutor,
) -> Result<KapellmeisterHostExecutionOutcome> {
    let Some(guard) = GuardedAuthority::evaluate(authority, context) else {
        if matches!(authority.mode, KapellmeisterAuthorityMode::ShadowOnly) {
            return Ok(KapellmeisterHostExecutionOutcome::shadow_only(
                "Kapellmeister authority is shadow_only",
            ));
        }
        return Ok(KapellmeisterHostExecutionOutcome::shadow_only(
            "Kapellmeister authority did not match guarded selectors",
        ));
    };

    let input = match HostDecisionInput::from_decision(response.decision) {
        HostDecisionInput::Execute(input) => input,
        HostDecisionInput::Noop(reason) => {
            return Ok(KapellmeisterHostExecutionOutcome::guarded(
                FugueToolOperation::LinearInspectIssue,
                no_mutation_result(reason),
            ));
        }
    };

    guard.ensure_permitted(input.operation())?;
    input.ensure_safe_for_context(context)?;
    let operation = input.operation();
    let result = input.execute(executor).await;
    Ok(KapellmeisterHostExecutionOutcome::guarded(
        operation, result,
    ))
}

pub async fn execute_guarded_kapellmeister_decision_with_native_tools(
    authority: &KapellmeisterToolAuthority,
    context: &KapellmeisterDecisionContext,
    response: KapellmeisterDecisionResponse,
    linear_client: &LinearClient,
) -> Result<KapellmeisterHostExecutionOutcome> {
    let mut executor = NativeKapellmeisterHostToolExecutor::new(linear_client);
    execute_guarded_kapellmeister_decision(authority, context, response, &mut executor).await
}

struct GuardedAuthority<'a> {
    authority: &'a KapellmeisterToolAuthority,
}

impl<'a> GuardedAuthority<'a> {
    fn evaluate(
        authority: &'a KapellmeisterToolAuthority,
        context: &KapellmeisterDecisionContext,
    ) -> Option<Self> {
        if !matches!(authority.mode, KapellmeisterAuthorityMode::GuardedLabeled) {
            return None;
        }
        selectors_match(&authority.safe_selectors, context).then_some(Self { authority })
    }

    fn ensure_permitted(&self, operation: FugueToolOperation) -> Result<()> {
        if !self.authority.permits(operation) || !operation.mutates() {
            return Err(FugueError::InvalidConfig(format!(
                "Kapellmeister guarded host execution blocked operation {operation}"
            )));
        }
        Ok(())
    }
}

fn selectors_match(
    selectors: &KapellmeisterSafeSelectors,
    context: &KapellmeisterDecisionContext,
) -> bool {
    repository_matches(&selectors.repositories, context.repository.as_ref())
        || label_matches(&selectors.linear_labels, &context.linear_labels)
}

fn repository_matches(selectors: &[GitHubRepoRef], repository: Option<&GitHubRepoRef>) -> bool {
    let Some(repository) = repository else {
        return false;
    };
    selectors.iter().any(|safe| safe == repository)
}

fn label_matches(selectors: &[String], labels: &[String]) -> bool {
    selectors.iter().any(|safe| {
        labels
            .iter()
            .any(|label| label.eq_ignore_ascii_case(safe.as_str()))
    })
}

#[allow(
    clippy::large_enum_variant,
    reason = "retain the typed application contract without incidental boxing"
)]
enum HostDecisionInput {
    Execute(ExecutableDecision),
    Noop(String),
}

impl HostDecisionInput {
    fn from_decision(decision: KapellmeisterHostDecision) -> Self {
        match decision {
            KapellmeisterHostDecision::Claim { input } => {
                Self::Execute(ExecutableDecision::Claim(input))
            }
            KapellmeisterHostDecision::Move { input } => {
                Self::Execute(ExecutableDecision::Move(input))
            }
            KapellmeisterHostDecision::WorkpadUpdate { input } => {
                Self::Execute(ExecutableDecision::WorkpadUpdate(input))
            }
            KapellmeisterHostDecision::WorkerStart { input } => {
                Self::Execute(ExecutableDecision::WorkerStart(input))
            }
            KapellmeisterHostDecision::FinalPrGateStart { request } => {
                Self::Execute(ExecutableDecision::FinalPrGateStart(request))
            }
            KapellmeisterHostDecision::IndependentReviewStart { request } => {
                Self::Execute(ExecutableDecision::IndependentReviewStart(request))
            }
            KapellmeisterHostDecision::Merge { input } => {
                Self::Execute(ExecutableDecision::Merge(input))
            }
            KapellmeisterHostDecision::ReviewRequest { issue, reason } => {
                let input = UpsertWorkpadInput {
                    issue,
                    heading: "## Fugue Workpad".to_string(),
                    body_markdown: format!(
                        "### Reviewer gate feedback\n- [ ] Kapellmeister requested focused replanning from reviewer feedback: {reason}"
                    ),
                    create_if_missing: true,
                    idempotency_key: IdempotencyKey(format!("kapellmeister-review:{reason}")),
                };
                Self::Execute(ExecutableDecision::WorkpadUpdate(input))
            }
            KapellmeisterHostDecision::Stop { reason }
            | KapellmeisterHostDecision::Noop { reason } => Self::Noop(reason),
        }
    }
}

enum ExecutableDecision {
    Claim(ClaimIssueInput),
    Move(MoveIssueInput),
    WorkpadUpdate(UpsertWorkpadInput),
    WorkerStart(SpawnOrResumeExecutionInput),
    FinalPrGateStart(FinalPrGateStartRequest),
    IndependentReviewStart(IndependentReviewStartRequest),
    Merge(MergePrInput),
}

impl ExecutableDecision {
    fn operation(&self) -> FugueToolOperation {
        match self {
            Self::Claim(_) => FugueToolOperation::LinearClaimIssue,
            Self::Move(_) => FugueToolOperation::LinearMoveIssue,
            Self::WorkpadUpdate(_) => FugueToolOperation::LinearUpsertWorkpad,
            Self::WorkerStart(_) => FugueToolOperation::MobSpawnOrResumeExecution,
            Self::FinalPrGateStart(_) => FugueToolOperation::MobSpawnOrResumeExecution,
            Self::IndependentReviewStart(_) => FugueToolOperation::MobSpawnOrResumeExecution,
            Self::Merge(_) => FugueToolOperation::GitHubMergePr,
        }
    }

    fn ensure_safe_for_context(&self, context: &KapellmeisterDecisionContext) -> Result<()> {
        match self {
            Self::Claim(input) => ensure_issue_matches_context(&input.issue, context),
            Self::Move(input) => ensure_issue_matches_context(&input.issue, context),
            Self::WorkpadUpdate(input) => ensure_issue_matches_context(&input.issue, context),
            Self::WorkerStart(input) => ensure_issue_matches_context(&input.issue, context),
            Self::FinalPrGateStart(request) => {
                ensure_issue_matches_context(&request.issue, context)?;
                ensure_issue_matches_context(&request.spawn_input.issue, context)?;
                if context.repository.as_ref() != Some(&request.pr.repo) {
                    return Err(FugueError::InvalidConfig(
                        "Kapellmeister guarded host execution blocked final PR gate outside guarded repository context".to_string(),
                    ));
                }
                Ok(())
            }
            Self::IndependentReviewStart(request) => {
                ensure_issue_matches_context(&request.issue, context)?;
                ensure_issue_matches_context(&request.spawn_input.issue, context)?;
                if context.repository.as_ref() != Some(&request.pull_request.repo) {
                    return Err(FugueError::InvalidConfig(
                        "Kapellmeister guarded host execution blocked independent review outside guarded repository context".to_string(),
                    ));
                }
                Ok(())
            }
            Self::Merge(input) => ensure_merge_safe_for_context(input, context),
        }
    }

    async fn execute(self, executor: &mut impl KapellmeisterHostToolExecutor) -> FugueToolResult {
        match self {
            Self::Claim(input) => executor.claim_issue(input).await,
            Self::Move(input) => executor.move_issue(input).await,
            Self::WorkpadUpdate(input) => executor.upsert_workpad(input).await,
            Self::WorkerStart(input) => executor.spawn_or_resume_execution(input).await,
            Self::FinalPrGateStart(request) => {
                executor.spawn_or_resume_final_pr_gate(request).await
            }
            Self::IndependentReviewStart(request) => {
                executor.spawn_or_resume_independent_review(request).await
            }
            Self::Merge(input) => executor.merge_pr(input).await,
        }
    }
}

fn ensure_issue_matches_context(
    input_issue: &LinearIssueRef,
    context: &KapellmeisterDecisionContext,
) -> Result<()> {
    if context.issue.as_ref() == Some(input_issue) {
        return Ok(());
    }
    Err(FugueError::InvalidConfig(
        "Kapellmeister guarded host execution blocked decision for issue outside guarded context"
            .to_string(),
    ))
}

fn ensure_merge_safe_for_context(
    input: &MergePrInput,
    context: &KapellmeisterDecisionContext,
) -> Result<()> {
    if context.repository.as_ref() != Some(&input.repo) {
        return Err(FugueError::InvalidConfig(
            "Kapellmeister guarded host execution blocked merge outside guarded repository context"
                .to_string(),
        ));
    }
    if input.expected_head_sha.is_none() {
        return Err(FugueError::InvalidConfig(
            "Kapellmeister guarded host execution blocked merge without expected_head_sha"
                .to_string(),
        ));
    }
    Ok(())
}

fn no_mutation_result(reason: String) -> FugueToolResult {
    let now = Utc::now();
    FugueToolResult::Err {
        error: FugueToolError {
            operation: FugueToolOperation::LinearInspectIssue,
            code: FugueToolErrorCode::UnsupportedOperation,
            message: format!("Kapellmeister no-op decision: {reason}"),
            retry: RetryAdvice::DoNotRetry,
            idempotency_key: None,
        },
        metadata: ToolExecutionMetadata {
            started_at: Some(now),
            completed_at: Some(now),
            ..ToolExecutionMetadata::default()
        },
    }
}

pub fn guarded_mode_allows_mutation(
    authority: &KapellmeisterToolAuthority,
    context: &KapellmeisterDecisionContext,
    operation: FugueToolOperation,
) -> bool {
    GuardedAuthority::evaluate(authority, context)
        .is_some_and(|guard| guard.ensure_permitted(operation).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kapellmeister::{KapellmeisterAuthorityConfig, ShadowDecisionId};
    use crate::tools::{
        BranchName, FugueToolOutput, GitHubOwner, GitHubPullRequestNumber, GitHubPullRequestRef,
        GitHubPullRequestSnapshot, GitHubRepoName, GitSha, LinearIssueIdentifier,
        LinearIssueSnapshot, LinearStateRef, LinearStateSnapshot, LinearStateType, LinearTeamId,
        LinearUserRef, MergeMethod, MobRunSnapshot, MobRunStatus, MobTemplateId, PullRequestState,
        UpsertWorkpadOutput, WorkspaceKey,
    };

    #[derive(Default)]
    struct RecordingExecutor {
        calls: Vec<FugueToolOperation>,
        keys: Vec<IdempotencyKey>,
        workpad_bodies: Vec<String>,
    }

    #[async_trait]
    impl KapellmeisterHostToolExecutor for RecordingExecutor {
        async fn claim_issue(&mut self, input: ClaimIssueInput) -> FugueToolResult {
            self.calls.push(FugueToolOperation::LinearClaimIssue);
            self.keys.push(input.idempotency_key.clone());
            ok(
                FugueToolOperation::LinearClaimIssue,
                Some(input.idempotency_key),
            )
        }

        async fn move_issue(&mut self, input: MoveIssueInput) -> FugueToolResult {
            self.calls.push(FugueToolOperation::LinearMoveIssue);
            self.keys.push(input.idempotency_key.clone());
            ok(
                FugueToolOperation::LinearMoveIssue,
                Some(input.idempotency_key),
            )
        }

        async fn upsert_workpad(&mut self, input: UpsertWorkpadInput) -> FugueToolResult {
            self.calls.push(FugueToolOperation::LinearUpsertWorkpad);
            self.keys.push(input.idempotency_key.clone());
            self.workpad_bodies.push(input.body_markdown);
            FugueToolResult::Ok {
                output: FugueToolOutput::LinearUpsertWorkpad(UpsertWorkpadOutput {
                    comment_id: crate::tools::LinearCommentId("comment-1".to_string()),
                    url: None,
                    created: true,
                    updated: false,
                }),
                metadata: metadata(Some(input.idempotency_key)),
            }
        }

        async fn spawn_or_resume_execution(
            &mut self,
            input: SpawnOrResumeExecutionInput,
        ) -> FugueToolResult {
            self.calls
                .push(FugueToolOperation::MobSpawnOrResumeExecution);
            self.keys.push(input.idempotency_key.clone());
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(
                    crate::tools::SpawnOrResumeExecutionOutput {
                        run: MobRunSnapshot {
                            run_id: crate::tools::MobRunId("run-1".to_string()),
                            meta_flow_run_id: None,
                            flow_id: None,
                            issue: Some(input.issue),
                            workspace_key: Some(input.workspace_key),
                            template_id: Some(input.template_id),
                            status: MobRunStatus::Running,
                            started_at: Some(Utc::now()),
                            updated_at: Utc::now(),
                            completed_at: None,
                        },
                        spawned: true,
                        resumed: false,
                    },
                ),
                metadata: metadata(Some(input.idempotency_key)),
            }
        }

        async fn spawn_or_resume_final_pr_gate(
            &mut self,
            request: FinalPrGateStartRequest,
        ) -> FugueToolResult {
            self.calls
                .push(FugueToolOperation::MobSpawnOrResumeExecution);
            self.keys.push(request.spawn_input.idempotency_key.clone());
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(
                    crate::tools::SpawnOrResumeExecutionOutput {
                        run: MobRunSnapshot {
                            run_id: crate::tools::MobRunId("run-1".to_string()),
                            meta_flow_run_id: None,
                            flow_id: None,
                            issue: Some(request.spawn_input.issue),
                            workspace_key: Some(request.spawn_input.workspace_key),
                            template_id: Some(request.spawn_input.template_id),
                            status: MobRunStatus::Running,
                            started_at: Some(Utc::now()),
                            updated_at: Utc::now(),
                            completed_at: None,
                        },
                        spawned: true,
                        resumed: false,
                    },
                ),
                metadata: metadata(Some(request.spawn_input.idempotency_key)),
            }
        }

        async fn spawn_or_resume_independent_review(
            &mut self,
            request: IndependentReviewStartRequest,
        ) -> FugueToolResult {
            self.calls
                .push(FugueToolOperation::MobSpawnOrResumeExecution);
            self.keys.push(request.spawn_input.idempotency_key.clone());
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(
                    crate::tools::SpawnOrResumeExecutionOutput {
                        run: MobRunSnapshot {
                            run_id: crate::tools::MobRunId("review-run-1".to_string()),
                            meta_flow_run_id: None,
                            flow_id: None,
                            issue: Some(request.spawn_input.issue),
                            workspace_key: Some(request.spawn_input.workspace_key),
                            template_id: Some(request.spawn_input.template_id),
                            status: MobRunStatus::Running,
                            started_at: Some(Utc::now()),
                            updated_at: Utc::now(),
                            completed_at: None,
                        },
                        spawned: true,
                        resumed: false,
                    },
                ),
                metadata: metadata(Some(request.spawn_input.idempotency_key)),
            }
        }

        async fn merge_pr(&mut self, input: MergePrInput) -> FugueToolResult {
            self.calls.push(FugueToolOperation::GitHubMergePr);
            self.keys.push(input.idempotency_key.clone());
            ok(
                FugueToolOperation::GitHubMergePr,
                Some(input.idempotency_key),
            )
        }
    }

    fn ok(
        operation: FugueToolOperation,
        idempotency_key: Option<IdempotencyKey>,
    ) -> FugueToolResult {
        FugueToolResult::Err {
            error: FugueToolError {
                operation,
                code: FugueToolErrorCode::UnsupportedOperation,
                message: "recorded".to_string(),
                retry: RetryAdvice::DoNotRetry,
                idempotency_key: idempotency_key.clone(),
            },
            metadata: metadata(idempotency_key),
        }
    }

    fn metadata(idempotency_key: Option<IdempotencyKey>) -> ToolExecutionMetadata {
        ToolExecutionMetadata {
            request_id: idempotency_key.map(|key| key.0),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            usage: None,
        }
    }

    fn repo_ref() -> GitHubRepoRef {
        GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }
    }

    fn issue_ref(identifier: &str) -> LinearIssueRef {
        LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier(identifier.to_string()),
        }
    }

    fn pr_snapshot() -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: repo_ref(),
            number: GitHubPullRequestNumber(187),
            title: "Feature".to_string(),
            url: "https://github.com/lukacf/meerkat-fugue/pull/187".to_string(),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName("codex/feature".to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha("sha-a".to_string()),
        }
    }

    fn authority() -> KapellmeisterToolAuthority {
        KapellmeisterToolAuthority::from_config(KapellmeisterAuthorityConfig {
            mode: KapellmeisterAuthorityMode::GuardedLabeled,
            safe_selectors: KapellmeisterSafeSelectors {
                repositories: vec![repo_ref()],
                linear_labels: vec!["kapellmeister-safe".to_string()],
            },
        })
        .unwrap()
    }

    fn context() -> KapellmeisterDecisionContext {
        KapellmeisterDecisionContext {
            issue: Some(issue_ref("LUC-167")),
            repository: Some(repo_ref()),
            linear_labels: vec!["kapellmeister-safe".to_string()],
            workspace_key: Some(WorkspaceKey("LUC-167".to_string())),
            observations: Vec::new(),
        }
    }

    fn response(decision: KapellmeisterHostDecision) -> KapellmeisterDecisionResponse {
        KapellmeisterDecisionResponse {
            request_id: ShadowDecisionId("decision-1".to_string()),
            decided_at: Utc::now(),
            decision,
            rationale: None,
        }
    }

    fn claim_decision(key: &str, issue: LinearIssueRef) -> KapellmeisterDecisionResponse {
        response(KapellmeisterHostDecision::Claim {
            input: ClaimIssueInput {
                issue,
                assignee: LinearUserRef::Me,
                target_state: Some(LinearStateRef::Name {
                    name: "In Progress".to_string(),
                }),
                idempotency_key: IdempotencyKey(key.to_string()),
            },
        })
    }

    #[tokio::test]
    async fn guarded_decisions_execute_only_for_matching_safe_selectors() {
        let mut executor = RecordingExecutor::default();
        let outcome = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            claim_decision("claim:LUC-167", issue_ref("LUC-167")),
            &mut executor,
        )
        .await
        .unwrap();

        assert_eq!(outcome.mode, KapellmeisterExecutionMode::Guarded);
        assert_eq!(
            outcome.operation,
            Some(FugueToolOperation::LinearClaimIssue)
        );
        assert_eq!(executor.calls, vec![FugueToolOperation::LinearClaimIssue]);
        assert_eq!(
            executor.keys,
            vec![IdempotencyKey("claim:LUC-167".to_string())]
        );
    }

    #[tokio::test]
    async fn non_matching_issues_use_kapellmeister_policy() {
        let mut executor = RecordingExecutor::default();
        let non_matching = KapellmeisterDecisionContext {
            issue: Some(issue_ref("LUC-168")),
            repository: Some(GitHubRepoRef {
                host: None,
                owner: GitHubOwner("other".to_string()),
                name: GitHubRepoName("repo".to_string()),
            }),
            linear_labels: vec!["triage".to_string()],
            workspace_key: Some(WorkspaceKey("LUC-168".to_string())),
            observations: Vec::new(),
        };

        let outcome = execute_guarded_kapellmeister_decision(
            &authority(),
            &non_matching,
            claim_decision("claim:LUC-168", issue_ref("LUC-168")),
            &mut executor,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome.mode,
            KapellmeisterExecutionMode::ShadowOnly { .. }
        ));
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn duplicate_decisions_preserve_idempotency_keys_for_host_tools() {
        let mut executor = RecordingExecutor::default();
        let first = claim_decision("claim:LUC-167:stable", issue_ref("LUC-167"));
        let duplicate = claim_decision("claim:LUC-167:stable", issue_ref("LUC-167"));

        execute_guarded_kapellmeister_decision(&authority(), &context(), first, &mut executor)
            .await
            .unwrap();
        execute_guarded_kapellmeister_decision(&authority(), &context(), duplicate, &mut executor)
            .await
            .unwrap();

        assert_eq!(executor.calls.len(), 2);
        assert_eq!(
            executor.keys,
            vec![
                IdempotencyKey("claim:LUC-167:stable".to_string()),
                IdempotencyKey("claim:LUC-167:stable".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn mutating_tool_calls_are_blocked_outside_guarded_mode() {
        let mut executor = RecordingExecutor::default();
        let shadow_authority =
            KapellmeisterToolAuthority::from_config(KapellmeisterAuthorityConfig::default())
                .unwrap();

        let outcome = execute_guarded_kapellmeister_decision(
            &shadow_authority,
            &context(),
            claim_decision("claim:LUC-167", issue_ref("LUC-167")),
            &mut executor,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome.mode,
            KapellmeisterExecutionMode::ShadowOnly { .. }
        ));
        assert!(executor.calls.is_empty());
        assert!(!guarded_mode_allows_mutation(
            &shadow_authority,
            &context(),
            FugueToolOperation::LinearClaimIssue,
        ));
    }

    #[tokio::test]
    async fn guarded_decision_cannot_mutate_a_different_issue() {
        let mut executor = RecordingExecutor::default();
        let err = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            claim_decision("claim:LUC-999", issue_ref("LUC-999")),
            &mut executor,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("outside guarded context"));
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn review_request_uses_idempotent_workpad_instead_of_duplicate_comment() {
        let mut executor = RecordingExecutor::default();
        let outcome = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            response(KapellmeisterHostDecision::ReviewRequest {
                issue: issue_ref("LUC-167"),
                reason: "needs operator".to_string(),
            }),
            &mut executor,
        )
        .await
        .unwrap();

        assert_eq!(outcome.mode, KapellmeisterExecutionMode::Guarded);
        assert_eq!(
            outcome.operation,
            Some(FugueToolOperation::LinearUpsertWorkpad)
        );
        assert_eq!(
            executor.calls,
            vec![FugueToolOperation::LinearUpsertWorkpad]
        );
        assert_eq!(
            executor.keys,
            vec![IdempotencyKey(
                "kapellmeister-review:needs operator".to_string()
            )]
        );
        assert!(executor.workpad_bodies[0].contains("focused replanning"));
        assert!(!executor.workpad_bodies[0].contains("Human review"));
    }

    #[tokio::test]
    async fn guarded_kapellmeister_can_execute_fresh_sha_merge_decision() {
        let mut executor = RecordingExecutor::default();
        let outcome = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            response(KapellmeisterHostDecision::Merge {
                input: MergePrInput {
                    repo: repo_ref(),
                    pull_request: GitHubPullRequestRef::Number {
                        number: GitHubPullRequestNumber(187),
                    },
                    method: MergeMethod::Squash,
                    expected_head_sha: Some(GitSha("sha-a".to_string())),
                    idempotency_key: IdempotencyKey("final-gate-merge:LUC-623:sha-a".to_string()),
                },
            }),
            &mut executor,
        )
        .await
        .unwrap();

        assert_eq!(outcome.mode, KapellmeisterExecutionMode::Guarded);
        assert_eq!(outcome.operation, Some(FugueToolOperation::GitHubMergePr));
        assert_eq!(executor.calls, vec![FugueToolOperation::GitHubMergePr]);
        assert_eq!(
            executor.keys,
            vec![IdempotencyKey("final-gate-merge:LUC-623:sha-a".to_string())]
        );
    }

    #[tokio::test]
    async fn guarded_kapellmeister_can_start_independent_review_for_safe_repo() {
        let mut executor = RecordingExecutor::default();
        let outcome = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            response(KapellmeisterHostDecision::IndependentReviewStart {
                request: IndependentReviewStartRequest {
                    issue: issue_ref("LUC-167"),
                    spawn_input: SpawnOrResumeExecutionInput {
                        issue: issue_ref("LUC-167"),
                        workspace_key: WorkspaceKey("LUC-167-review".to_string()),
                        template_id: MobTemplateId("independent_review".to_string()),
                        worktree_path: Some("/tmp/fugue/LUC-167".to_string()),
                        instructions: Some("Review PR independently".to_string()),
                        idempotency_key: IdempotencyKey(
                            "independent-review:LUC-167:sha-a".to_string(),
                        ),
                    },
                    pull_request: pr_snapshot(),
                    orchestrator_profile: "review_orchestrator".to_string(),
                },
            }),
            &mut executor,
        )
        .await
        .unwrap();

        assert_eq!(outcome.mode, KapellmeisterExecutionMode::Guarded);
        assert_eq!(
            outcome.operation,
            Some(FugueToolOperation::MobSpawnOrResumeExecution)
        );
        assert_eq!(
            executor.calls,
            vec![FugueToolOperation::MobSpawnOrResumeExecution]
        );
        assert_eq!(
            executor.keys,
            vec![IdempotencyKey(
                "independent-review:LUC-167:sha-a".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn guarded_kapellmeister_blocks_merge_without_fresh_sha_guard() {
        let mut executor = RecordingExecutor::default();
        let err = execute_guarded_kapellmeister_decision(
            &authority(),
            &context(),
            response(KapellmeisterHostDecision::Merge {
                input: MergePrInput {
                    repo: repo_ref(),
                    pull_request: GitHubPullRequestRef::Number {
                        number: GitHubPullRequestNumber(187),
                    },
                    method: MergeMethod::Squash,
                    expected_head_sha: None,
                    idempotency_key: IdempotencyKey("final-gate-merge:LUC-623:no-sha".to_string()),
                },
            }),
            &mut executor,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("expected_head_sha"));
        assert!(executor.calls.is_empty());
    }

    #[test]
    fn guarded_mode_permits_only_stage_4b_mutations_for_safe_context() {
        let authority = authority();
        let context = context();
        assert!(guarded_mode_allows_mutation(
            &authority,
            &context,
            FugueToolOperation::LinearMoveIssue,
        ));
        assert!(guarded_mode_allows_mutation(
            &authority,
            &context,
            FugueToolOperation::MobSpawnOrResumeExecution,
        ));
        assert!(guarded_mode_allows_mutation(
            &authority,
            &context,
            FugueToolOperation::GitHubMergePr,
        ));
        assert!(!guarded_mode_allows_mutation(
            &authority,
            &context,
            FugueToolOperation::LinearAddComment,
        ));
        assert!(!guarded_mode_allows_mutation(
            &authority,
            &context,
            FugueToolOperation::GitHubCreatePr,
        ));
    }

    #[allow(dead_code)]
    fn issue_snapshot(identifier: &str) -> LinearIssueSnapshot {
        LinearIssueSnapshot {
            id: crate::tools::LinearIssueId(format!("id-{identifier}")),
            identifier: LinearIssueIdentifier(identifier.to_string()),
            title: "issue".to_string(),
            description: None,
            url: None,
            state: LinearStateSnapshot {
                id: crate::tools::LinearStateId("state".to_string()),
                name: "Todo".to_string(),
                state_type: LinearStateType::Unstarted,
            },
            assignee: None,
            branch_name: Some(BranchName(format!("codex/{identifier}"))),
            labels: vec!["kapellmeister-safe".to_string()],
            updated_at: Utc::now(),
            team_id: LinearTeamId("team".to_string()),
        }
    }

    #[test]
    fn worker_start_preserves_idempotency_key() {
        let decision = HostDecisionInput::from_decision(KapellmeisterHostDecision::WorkerStart {
            input: SpawnOrResumeExecutionInput {
                issue: issue_ref("LUC-167"),
                workspace_key: WorkspaceKey("LUC-167".to_string()),
                template_id: MobTemplateId("solo".to_string()),
                worktree_path: None,
                instructions: None,
                idempotency_key: IdempotencyKey("worker:LUC-167".to_string()),
            },
        });

        match decision {
            HostDecisionInput::Execute(ExecutableDecision::WorkerStart(input)) => {
                assert_eq!(
                    input.idempotency_key,
                    IdempotencyKey("worker:LUC-167".to_string())
                );
            }
            _ => panic!("worker start should remain an executable host decision"),
        }
    }
}
