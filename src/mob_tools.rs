use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use chrono::Utc;

use crate::config::{
    MaterializedMobTemplate, MobExecutionBackend, NativeMcpServerConfig, NativeMobFlow,
};
use crate::tools::{
    CancelExecutionInput, Cursor, FugueToolError, FugueToolErrorCode, FugueToolOperation,
    FugueToolOutput, FugueToolResult, GitHubRepoRef, InspectRunInput, ListRunsInput,
    ListRunsOutput, MobRunId, MobRunSnapshot, MobRunStatus, ObserveEventsInput, RetryAdvice,
    SpawnOrResumeExecutionInput, SpawnOrResumeExecutionOutput, ToolExecutionMetadata,
};

const PLANNER_CODER_REVIEWER_STEP_TIMEOUT_MS: u64 = 1_200_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaStructuralEventCursor(pub Cursor);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserveStructuralEventsInput {
    pub run_id: MobRunId,
    pub cursor: Option<MetaStructuralEventCursor>,
    pub limit: u16,
}

/// Typed Stage 5A contract for a native Meta execution host.
///
/// Implementations must be idempotent for spawn/resume and cancel operations and
/// must fail before dispatch when native execution is disabled or misconfigured.
pub trait NativeMobExecutionHost {
    fn restore_persisted_runtimes(
        &self,
        _runtime_keys: Vec<String>,
        _max_successes: usize,
    ) -> std::result::Result<(), String> {
        Ok(())
    }
    fn list_runs(&self, input: ListRunsInput) -> FugueToolResult;
    fn status(&self, input: InspectRunInput) -> FugueToolResult;
    fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult;
    fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
        self.spawn_or_resume_execution(plan.input)
    }
    fn cancel_execution(&self, input: CancelExecutionInput) -> FugueToolResult;
    fn cleanup_execution(
        &self,
        _run_id: &MobRunId,
        _reason: &str,
    ) -> std::result::Result<usize, String> {
        Ok(0)
    }
    fn observe_structural_events(&self, input: ObserveStructuralEventsInput) -> FugueToolResult;
    fn notify_issue_lead_member_turn_limit(
        &self,
        _issue_identifier: &str,
        _run_id: &MobRunId,
        _member_identity: &str,
        _detail: &str,
    ) -> std::result::Result<(), String> {
        Ok(())
    }
    fn notify_issue_lead_member_terminal_failure(
        &self,
        _issue_identifier: &str,
        _run_id: &MobRunId,
        _member_identity: &str,
        _detail: &str,
    ) -> std::result::Result<(), String> {
        Ok(())
    }
}

pub fn native_template_step_timeout_floor_ms(template: &MaterializedMobTemplate) -> Option<i64> {
    match template.flow {
        Some(NativeMobFlow::PlannerCoderReviewer) => {
            i64::try_from(PLANNER_CODER_REVIEWER_STEP_TIMEOUT_MS).ok()
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMobLaunchPlan {
    pub input: SpawnOrResumeExecutionInput,
    pub template: MaterializedMobTemplate,
    pub mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
    pub project_slug: Option<String>,
    pub repository: Option<GitHubRepoRef>,
}

impl NativeMobLaunchPlan {
    pub fn new(
        input: SpawnOrResumeExecutionInput,
        template: MaterializedMobTemplate,
        mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
        direct_tool_servers: BTreeSet<String>,
        project_slug: Option<String>,
        repository: Option<GitHubRepoRef>,
    ) -> std::result::Result<Self, FugueToolError> {
        if template.backend != MobExecutionBackend::NativeExecution {
            return Err(invalid_spawn_input(
                input.idempotency_key.clone(),
                "native execution mob launch requires a native-execution template",
            ));
        }
        if input.worktree_path.as_deref().is_none_or(str::is_empty) {
            return Err(invalid_spawn_input(
                input.idempotency_key.clone(),
                "native execution mob launch requires an explicit worktree_path",
            ));
        }
        if input.instructions.as_deref().is_none_or(str::is_empty) {
            return Err(invalid_spawn_input(
                input.idempotency_key.clone(),
                "native execution mob launch requires explicit issue instructions",
            ));
        }
        if template.flow == Some(NativeMobFlow::PlannerCoderReviewer)
            && template
                .linear_projection
                .as_ref()
                .is_none_or(|projection| {
                    !projection.node_lanes.contains_key("planner")
                        || !projection.node_lanes.contains_key("coder")
                        || !projection.node_lanes.contains_key("reviewer")
                })
        {
            return Err(invalid_spawn_input(
                input.idempotency_key.clone(),
                "native planner-coder-reviewer launch requires a linear projection for planner, coder, and reviewer nodes",
            ));
        }
        for member in &template.members {
            for server in member
                .agent
                .tools
                .iter()
                .filter_map(|tool| tool.strip_prefix("mcp:"))
            {
                if !mcp_servers.contains_key(server) && !direct_tool_servers.contains(server) {
                    let message = format!(
                        "native execution member {} references unknown MCP server {server:?} before native member startup",
                        member.name
                    );
                    return Err(invalid_spawn_input(input.idempotency_key.clone(), &message));
                }
            }
        }
        validate_required_member_tools(&input, &template, &mcp_servers, &direct_tool_servers)?;
        Ok(Self {
            input,
            template,
            mcp_servers,
            project_slug,
            repository,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryNativeMobExecutionHost {
    runs: Arc<Mutex<BTreeMap<String, MobRunSnapshot>>>,
    events: Arc<Mutex<BTreeMap<String, Vec<crate::tools::MobEventSnapshot>>>>,
}

impl InMemoryNativeMobExecutionHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
        self.spawn_or_resume_execution(plan.input)
    }

    pub fn append_structural_event(
        &self,
        run_id: &MobRunId,
        kind: crate::tools::MobEventKind,
        message: Option<String>,
    ) -> std::result::Result<crate::tools::MobEventSnapshot, FugueToolError> {
        match self.events.lock() {
            Ok(mut events) => {
                let run_events = events.entry(run_id.0.clone()).or_default();
                let event = crate::tools::MobEventSnapshot {
                    sequence: run_events
                        .last()
                        .map(|event| event.sequence.saturating_add(1))
                        .unwrap_or(1),
                    occurred_at: Utc::now(),
                    kind,
                    message,
                };
                run_events.push(event.clone());
                Ok(event)
            }
            Err(_) => Err(FugueToolError {
                operation: FugueToolOperation::MobObserveEvents,
                code: FugueToolErrorCode::BackendUnavailable,
                message: "native mob structural event registry lock is poisoned".to_string(),
                retry: RetryAdvice::SafeToRetry,
                idempotency_key: None,
            }),
        }
    }

    fn with_runs(
        &self,
        operation: FugueToolOperation,
        idempotency_key: Option<crate::tools::IdempotencyKey>,
        f: impl FnOnce(&mut BTreeMap<String, MobRunSnapshot>) -> FugueToolResult,
    ) -> FugueToolResult {
        match self.runs.lock() {
            Ok(mut runs) => f(&mut runs),
            Err(_) => tool_error(
                operation,
                FugueToolErrorCode::BackendUnavailable,
                "native mob execution registry lock is poisoned",
                RetryAdvice::SafeToRetry,
                idempotency_key,
            ),
        }
    }
}

impl NativeMobExecutionHost for InMemoryNativeMobExecutionHost {
    fn list_runs(&self, input: ListRunsInput) -> FugueToolResult {
        self.with_runs(FugueToolOperation::MobListRuns, None, |runs| {
            let mut snapshots = runs
                .values()
                .filter(|run| {
                    input
                        .issue
                        .as_ref()
                        .is_none_or(|issue| run.issue.as_ref() == Some(issue))
                        && input.status.is_none_or(|status| run.status == status)
                })
                .take(input.limit.into())
                .cloned()
                .collect::<Vec<_>>();
            snapshots.sort_by(|left, right| left.run_id.0.cmp(&right.run_id.0));
            FugueToolResult::Ok {
                output: FugueToolOutput::MobListRuns(ListRunsOutput {
                    runs: snapshots,
                    next_cursor: None,
                }),
                metadata: ToolExecutionMetadata::default(),
            }
        })
    }

    fn status(&self, input: InspectRunInput) -> FugueToolResult {
        self.with_runs(FugueToolOperation::MobInspectRun, None, |runs| {
            let Some(run) = runs.get_mut(&input.run_id.0) else {
                return tool_error(
                    FugueToolOperation::MobInspectRun,
                    FugueToolErrorCode::NotFound,
                    "native mob execution run was not found",
                    RetryAdvice::DoNotRetry,
                    None,
                );
            };
            if matches!(
                run.status,
                MobRunStatus::Pending | MobRunStatus::Running | MobRunStatus::WaitingForInput
            ) {
                run.updated_at = Utc::now();
            }
            let run = run.clone();
            FugueToolResult::Ok {
                output: FugueToolOutput::MobInspectRun(crate::tools::InspectRunOutput { run }),
                metadata: ToolExecutionMetadata::default(),
            }
        })
    }

    fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult {
        if input.worktree_path.as_deref().is_none_or(str::is_empty) {
            return tool_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                FugueToolErrorCode::InvalidInput,
                "native execution mob launch requires an explicit worktree_path",
                RetryAdvice::DoNotRetry,
                Some(input.idempotency_key),
            );
        }
        if input.instructions.as_deref().is_none_or(str::is_empty) {
            return tool_error(
                FugueToolOperation::MobSpawnOrResumeExecution,
                FugueToolErrorCode::InvalidInput,
                "native execution mob launch requires explicit issue instructions",
                RetryAdvice::DoNotRetry,
                Some(input.idempotency_key),
            );
        }
        let run_id = MobRunId(format!("native-execution:{}", input.idempotency_key.0));
        self.with_runs(
            FugueToolOperation::MobSpawnOrResumeExecution,
            Some(input.idempotency_key),
            |runs| {
                let now = Utc::now();
                let (run, spawned, resumed) = if let Some(existing) = runs.get_mut(&run_id.0) {
                    existing.updated_at = now;
                    (existing.clone(), false, true)
                } else {
                    let run = MobRunSnapshot {
                        run_id: run_id.clone(),
                        meta_flow_run_id: None,
                        flow_id: None,
                        issue: Some(input.issue),
                        workspace_key: Some(input.workspace_key),
                        template_id: Some(input.template_id),
                        status: MobRunStatus::Running,
                        started_at: Some(now),
                        updated_at: now,
                        completed_at: None,
                    };
                    runs.insert(run_id.0.clone(), run.clone());
                    (run, true, false)
                };
                FugueToolResult::Ok {
                    output: FugueToolOutput::MobSpawnOrResumeExecution(
                        SpawnOrResumeExecutionOutput {
                            run,
                            spawned,
                            resumed,
                        },
                    ),
                    metadata: ToolExecutionMetadata::default(),
                }
            },
        )
    }

    fn cancel_execution(&self, input: CancelExecutionInput) -> FugueToolResult {
        self.with_runs(
            FugueToolOperation::MobCancelExecution,
            Some(input.idempotency_key),
            |runs| {
                let Some(run) = runs.get_mut(&input.run_id.0) else {
                    return tool_error(
                        FugueToolOperation::MobCancelExecution,
                        FugueToolErrorCode::NotFound,
                        "native mob execution run was not found",
                        RetryAdvice::DoNotRetry,
                        None,
                    );
                };
                run.status = MobRunStatus::Cancelled;
                run.updated_at = Utc::now();
                run.completed_at = Some(run.updated_at);
                FugueToolResult::Ok {
                    output: FugueToolOutput::MobCancelExecution(
                        crate::tools::CancelExecutionOutput {
                            run: run.clone(),
                            cancelled: true,
                        },
                    ),
                    metadata: ToolExecutionMetadata::default(),
                }
            },
        )
    }

    fn observe_structural_events(&self, input: ObserveStructuralEventsInput) -> FugueToolResult {
        match self.events.lock() {
            Ok(events) => {
                let after = input
                    .cursor
                    .as_ref()
                    .and_then(|cursor| crate::mob_event_observer::cursor_sequence(&cursor.0))
                    .unwrap_or(0);
                let selected = events
                    .get(&input.run_id.0)
                    .map(|events| {
                        events
                            .iter()
                            .filter(|event| event.sequence > after)
                            .take(input.limit.into())
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let next_cursor = selected
                    .last()
                    .map(|event| Cursor(format!("sequence:{}", event.sequence)));
                FugueToolResult::Ok {
                    output: FugueToolOutput::MobObserveEvents(crate::tools::ObserveEventsOutput {
                        events: selected,
                        next_cursor,
                    }),
                    metadata: ToolExecutionMetadata::default(),
                }
            }
            Err(_) => tool_error(
                FugueToolOperation::MobObserveEvents,
                FugueToolErrorCode::BackendUnavailable,
                "native mob structural event registry lock is poisoned",
                RetryAdvice::SafeToRetry,
                None,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NativeMobToolHost;

impl NativeMobToolHost {
    pub const fn new() -> Self {
        Self
    }

    pub fn list_runs(&self, _input: ListRunsInput) -> FugueToolResult {
        unsupported(FugueToolOperation::MobListRuns, None)
    }

    pub fn inspect_run(&self, _input: InspectRunInput) -> FugueToolResult {
        unsupported(FugueToolOperation::MobInspectRun, None)
    }

    pub fn status(&self, input: InspectRunInput) -> FugueToolResult {
        self.inspect_run(input)
    }

    pub fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult {
        unsupported(
            FugueToolOperation::MobSpawnOrResumeExecution,
            Some(input.idempotency_key),
        )
    }

    pub fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
        unsupported(
            FugueToolOperation::MobSpawnOrResumeExecution,
            Some(plan.input.idempotency_key),
        )
    }

    pub fn cancel_execution(&self, input: CancelExecutionInput) -> FugueToolResult {
        unsupported(
            FugueToolOperation::MobCancelExecution,
            Some(input.idempotency_key),
        )
    }

    pub fn observe_events(&self, _input: ObserveEventsInput) -> FugueToolResult {
        unsupported(FugueToolOperation::MobObserveEvents, None)
    }

    pub fn observe_structural_events(
        &self,
        _input: ObserveStructuralEventsInput,
    ) -> FugueToolResult {
        unsupported(FugueToolOperation::MobObserveEvents, None)
    }
}

impl NativeMobExecutionHost for NativeMobToolHost {
    fn list_runs(&self, input: ListRunsInput) -> FugueToolResult {
        Self::list_runs(self, input)
    }

    fn status(&self, input: InspectRunInput) -> FugueToolResult {
        Self::status(self, input)
    }

    fn spawn_or_resume_execution(&self, input: SpawnOrResumeExecutionInput) -> FugueToolResult {
        Self::spawn_or_resume_execution(self, input)
    }

    fn spawn_or_resume_plan(&self, plan: NativeMobLaunchPlan) -> FugueToolResult {
        Self::spawn_or_resume_plan(self, plan)
    }

    fn cancel_execution(&self, input: CancelExecutionInput) -> FugueToolResult {
        Self::cancel_execution(self, input)
    }

    fn observe_structural_events(&self, input: ObserveStructuralEventsInput) -> FugueToolResult {
        Self::observe_structural_events(self, input)
    }
}

fn unsupported(
    operation: FugueToolOperation,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
) -> FugueToolResult {
    let now = Utc::now();
    FugueToolResult::Err {
        error: FugueToolError {
            operation,
            code: FugueToolErrorCode::UnsupportedOperation,
            message: format!(
                "native mob-control surface for {operation} requires a configured native execution host"
            ),
            retry: RetryAdvice::DoNotRetry,
            idempotency_key,
        },
        metadata: ToolExecutionMetadata {
            started_at: Some(now),
            completed_at: Some(now),
            ..ToolExecutionMetadata::default()
        },
    }
}

fn validate_required_member_tools(
    input: &SpawnOrResumeExecutionInput,
    template: &MaterializedMobTemplate,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
    direct_tool_servers: &BTreeSet<String>,
) -> std::result::Result<(), FugueToolError> {
    if !matches!(template.flow, Some(NativeMobFlow::PlannerCoderReviewer)) {
        return Ok(());
    }

    for required in ["linear", "github"] {
        if !mcp_servers.contains_key(required) && !direct_tool_servers.contains(required) {
            let message = format!(
                "native execution flow {} requires configured MCP server {required:?} before native member startup",
                template
                    .flow
                    .map(|flow| flow.canonical_name())
                    .unwrap_or("unknown")
            );
            return Err(invalid_spawn_input(input.idempotency_key.clone(), &message));
        }
    }

    for member in &template.members {
        let tools = &member.agent.tools;
        if !tools.iter().any(|tool| tool == "mob") {
            let message = format!(
                "native execution member {} is missing required tool \"mob\"; planner/coder/reviewer agents must have agent-side mob tools so delegate() is available before native member startup",
                member.name
            );
            return Err(invalid_spawn_input(input.idempotency_key.clone(), &message));
        }
        for required in ["mcp:linear", "mcp:github"] {
            if !tools.iter().any(|tool| tool == required) {
                let message = format!(
                    "native execution member {} is missing required tool {required:?}; required MCP tools must be available before native member startup",
                    member.name
                );
                return Err(invalid_spawn_input(input.idempotency_key.clone(), &message));
            }
        }
    }
    Ok(())
}

fn invalid_spawn_input(
    idempotency_key: crate::tools::IdempotencyKey,
    message: &str,
) -> FugueToolError {
    FugueToolError {
        operation: FugueToolOperation::MobSpawnOrResumeExecution,
        code: FugueToolErrorCode::InvalidInput,
        message: message.to_string(),
        retry: RetryAdvice::DoNotRetry,
        idempotency_key: Some(idempotency_key),
    }
}

fn tool_error(
    operation: FugueToolOperation,
    code: FugueToolErrorCode,
    message: &str,
    retry: RetryAdvice,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
) -> FugueToolResult {
    let now = Utc::now();
    FugueToolResult::Err {
        error: FugueToolError {
            operation,
            code,
            message: message.to_string(),
            retry,
            idempotency_key,
        },
        metadata: ToolExecutionMetadata {
            started_at: Some(now),
            completed_at: Some(now),
            ..ToolExecutionMetadata::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{
        IdempotencyKey, LinearIssueIdentifier, LinearIssueRef, MobTemplateId, WorkspaceKey,
    };

    fn planner_coder_reviewer_template() -> crate::config::MaterializedMobTemplate {
        let agent_with_repo_mcp = crate::config::NativeAgentProfile {
            model: Some("claude-sonnet-4-5".to_string()),
            provider: None,
            auth_binding: None,
            max_tokens: None,
            system_prompt: None,
            output_schema: None,
            structured_output_retries: None,
            provider_params: None,
            tools: vec![
                "mob".to_string(),
                "shell".to_string(),
                "git".to_string(),
                "mcp:linear".to_string(),
                "mcp:github".to_string(),
            ],
            skills: Vec::new(),
            additional_instructions: None,
        };
        crate::config::MaterializedMobTemplate {
            backend: crate::config::MobExecutionBackend::NativeExecution,
            members: vec![
                crate::config::MaterializedMobMember {
                    name: "planner".to_string(),
                    profile: "planner".to_string(),
                    agent: agent_with_repo_mcp.clone(),
                },
                crate::config::MaterializedMobMember {
                    name: "coder".to_string(),
                    profile: "coder".to_string(),
                    agent: agent_with_repo_mcp.clone(),
                },
                crate::config::MaterializedMobMember {
                    name: "reviewer".to_string(),
                    profile: "reviewer".to_string(),
                    agent: agent_with_repo_mcp,
                },
            ],
            flow: Some(crate::config::NativeMobFlow::PlannerCoderReviewer),
            linear_projection: Some(crate::config::MaterializedLinearProjection {
                id: "planner_coder_reviewer".to_string(),
                node_lanes: BTreeMap::from([
                    ("planner".to_string(), "Planning".to_string()),
                    ("coder".to_string(), "Implementing".to_string()),
                    ("reviewer".to_string(), "Reviewing".to_string()),
                ]),
                node_phase_labels: BTreeMap::from([
                    ("planner".to_string(), "fugue:phase:planning".to_string()),
                    ("coder".to_string(), "fugue:phase:implementing".to_string()),
                    ("reviewer".to_string(), "fugue:phase:reviewing".to_string()),
                ]),
                aggregate: crate::config::NativeLinearProjectionAggregate {
                    precedence: vec![
                        "Reviewing".to_string(),
                        "Implementing".to_string(),
                        "Planning".to_string(),
                    ],
                    fallback: Some("Reviewing".to_string()),
                },
                terminal_lanes: crate::config::NativeLinearProjectionTerminals {
                    succeeded: "Done".to_string(),
                    failed: "Reviewing".to_string(),
                    cancelled: "Canceled".to_string(),
                },
            }),
            max_review_cycles: Some(2),
        }
    }

    fn prompt_driven_template_with_tools(tools: &[&str]) -> crate::config::MaterializedMobTemplate {
        let agent = crate::config::NativeAgentProfile {
            model: Some("claude-sonnet-4-5".to_string()),
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
        };
        crate::config::MaterializedMobTemplate {
            backend: crate::config::MobExecutionBackend::NativeExecution,
            members: vec![crate::config::MaterializedMobMember {
                name: "issue_lead".to_string(),
                profile: "issue_lead".to_string(),
                agent,
            }],
            flow: None,
            linear_projection: None,
            max_review_cycles: None,
        }
    }

    #[test]
    fn list_runs_returns_typed_unsupported_error() {
        let result = NativeMobToolHost::new().list_runs(ListRunsInput {
            issue: None,
            status: Some(MobRunStatus::Running),
            cursor: None,
            limit: 25,
        });

        match result {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.operation, FugueToolOperation::MobListRuns);
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
                assert_eq!(error.retry, RetryAdvice::DoNotRetry);
                assert!(
                    error
                        .message
                        .contains("requires a configured native execution host")
                );
            }
            FugueToolResult::Ok { .. } => panic!("mob list_runs must not silently succeed"),
        }
    }

    #[test]
    fn mutation_stubs_preserve_idempotency_key_in_error() {
        let key = IdempotencyKey("spawn:LUC-139".to_string());
        let result =
            NativeMobToolHost::new().spawn_or_resume_execution(SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-139".to_string()),
                },
                workspace_key: WorkspaceKey("LUC-139".to_string()),
                template_id: MobTemplateId("default".to_string()),
                worktree_path: None,
                instructions: None,
                idempotency_key: key.clone(),
            });

        match result {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(
                    error.operation,
                    FugueToolOperation::MobSpawnOrResumeExecution
                );
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
                assert_eq!(error.idempotency_key, Some(key));
            }
            FugueToolResult::Ok { .. } => {
                panic!("mob spawn_or_resume_execution must not silently succeed")
            }
        }
    }

    #[test]
    fn cancel_execution_returns_typed_unsupported_error() {
        let key = IdempotencyKey("cancel:run-1".to_string());
        let result = NativeMobToolHost::new().cancel_execution(CancelExecutionInput {
            run_id: MobRunId("run-1".to_string()),
            reason: "test".to_string(),
            idempotency_key: key.clone(),
        });

        match result {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.operation, FugueToolOperation::MobCancelExecution);
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
                assert_eq!(error.idempotency_key, Some(key));
            }
            FugueToolResult::Ok { .. } => panic!("mob cancel_execution must not silently succeed"),
        }
    }

    #[test]
    fn native_host_contract_exposes_status_and_structural_event_cursor() {
        let host: &dyn NativeMobExecutionHost = &NativeMobToolHost::new();
        let status = host.status(InspectRunInput {
            run_id: MobRunId("run-1".to_string()),
        });
        match status {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.operation, FugueToolOperation::MobInspectRun);
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
            }
            FugueToolResult::Ok { .. } => panic!("native status stub must not silently succeed"),
        }

        let events = host.observe_structural_events(ObserveStructuralEventsInput {
            run_id: MobRunId("run-1".to_string()),
            cursor: None,
            limit: 50,
        });
        match events {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.operation, FugueToolOperation::MobObserveEvents);
                assert_eq!(error.code, FugueToolErrorCode::UnsupportedOperation);
            }
            FugueToolResult::Ok { .. } => {
                panic!("native structural event stub must not silently succeed")
            }
        }
    }

    #[test]
    fn in_memory_native_host_spawns_once_then_resumes_for_duplicate_dispatch_key() {
        let host = InMemoryNativeMobExecutionHost::new();
        let input = SpawnOrResumeExecutionInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-171".to_string()),
            },
            workspace_key: WorkspaceKey("LUC-171".to_string()),
            template_id: MobTemplateId("implementation_mob".to_string()),
            worktree_path: Some("/tmp/worktrees/LUC-171".to_string()),
            instructions: Some("Work only in /tmp/worktrees/LUC-171".to_string()),
            idempotency_key: IdempotencyKey("native-execution:LUC-171".to_string()),
        };

        let first = host.spawn_or_resume_execution(input.clone());
        let second = host.spawn_or_resume_execution(input);

        let first = match first {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(output),
                ..
            } => output,
            other => panic!("first native spawn failed: {other:?}"),
        };
        let second = match second {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobSpawnOrResumeExecution(output),
                ..
            } => output,
            other => panic!("second native spawn failed: {other:?}"),
        };

        assert!(first.spawned);
        assert!(!first.resumed);
        assert!(!second.spawned);
        assert!(second.resumed);
        assert_eq!(first.run.run_id, second.run.run_id);

        let runs = match host.list_runs(ListRunsInput {
            issue: None,
            status: None,
            cursor: None,
            limit: 10,
        }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::MobListRuns(output),
                ..
            } => output.runs,
            other => panic!("list runs failed: {other:?}"),
        };
        assert_eq!(runs.len(), 1);
    }

    #[test]
    fn native_launch_plan_blocks_missing_required_linear_mcp_before_flow_run() {
        let key = IdempotencyKey("native-execution:LUC-277".to_string());
        let mut mcp_servers = BTreeMap::new();
        mcp_servers.insert(
            "github".to_string(),
            crate::config::NativeMcpServerConfig {
                command: vec!["fugue-github-mcp".to_string()],
                url: None,
                env: BTreeMap::new(),
            },
        );

        let error = NativeMobLaunchPlan::new(
            SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-277".to_string()),
                },
                workspace_key: WorkspaceKey("LUC-277".to_string()),
                template_id: MobTemplateId("implementation_mob".to_string()),
                worktree_path: Some("/tmp/worktrees/LUC-277".to_string()),
                instructions: Some("Implement the issue.".to_string()),
                idempotency_key: key.clone(),
            },
            planner_coder_reviewer_template(),
            mcp_servers,
            BTreeSet::new(),
            Some("meerkat-fugue-queue".to_string()),
            None,
        )
        .unwrap_err();

        assert_eq!(error.code, FugueToolErrorCode::InvalidInput);
        assert_eq!(error.idempotency_key, Some(key));
        assert!(error.message.contains("linear"));
        assert!(error.message.contains("before native member startup"));
    }

    #[test]
    fn native_launch_plan_accepts_host_provided_direct_linear_tools() {
        let key = IdempotencyKey("native-execution:FUG-277".to_string());
        let mut direct_tool_servers = BTreeSet::new();
        direct_tool_servers.insert("linear".to_string());

        let plan = NativeMobLaunchPlan::new(
            SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("FUG-277".to_string()),
                },
                workspace_key: WorkspaceKey("FUG-277".to_string()),
                template_id: MobTemplateId("implementation_mob".to_string()),
                worktree_path: Some("/tmp/worktrees/FUG-277".to_string()),
                instructions: Some("Exercise Fugue IMS issue tools.".to_string()),
                idempotency_key: key,
            },
            prompt_driven_template_with_tools(&["mob", "mcp:linear"]),
            BTreeMap::new(),
            direct_tool_servers,
            Some("fugue-ims".to_string()),
            None,
        )
        .unwrap_or_else(|error| panic!("direct linear tools should be accepted: {error:?}"));

        assert!(plan.mcp_servers.is_empty());
    }

    #[test]
    fn native_launch_plan_blocks_member_missing_required_repo_mcp_tool() {
        let key = IdempotencyKey("native-execution:LUC-277".to_string());
        let mut template = planner_coder_reviewer_template();
        template.members[0]
            .agent
            .tools
            .retain(|tool| tool != "mcp:linear");
        let mut mcp_servers = BTreeMap::new();
        for server in ["linear", "github"] {
            mcp_servers.insert(
                server.to_string(),
                crate::config::NativeMcpServerConfig {
                    command: vec![format!("fugue-{server}-mcp")],
                    url: None,
                    env: BTreeMap::new(),
                },
            );
        }

        let error = NativeMobLaunchPlan::new(
            SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-277".to_string()),
                },
                workspace_key: WorkspaceKey("LUC-277".to_string()),
                template_id: MobTemplateId("implementation_mob".to_string()),
                worktree_path: Some("/tmp/worktrees/LUC-277".to_string()),
                instructions: Some("Implement the issue.".to_string()),
                idempotency_key: key,
            },
            template,
            mcp_servers,
            BTreeSet::new(),
            Some("meerkat-fugue-queue".to_string()),
            None,
        )
        .unwrap_err();

        assert!(error.message.contains("planner"));
        assert!(error.message.contains("mcp:linear"));
        assert!(error.message.contains("before native member startup"));
    }

    #[test]
    fn native_launch_plan_blocks_member_missing_delegate_tool_before_flow_run() {
        let key = IdempotencyKey("native-execution:LUC-277".to_string());
        let mut template = planner_coder_reviewer_template();
        template.members[0].agent.tools.retain(|tool| tool != "mob");
        let mut mcp_servers = BTreeMap::new();
        for server in ["linear", "github"] {
            mcp_servers.insert(
                server.to_string(),
                crate::config::NativeMcpServerConfig {
                    command: vec![format!("fugue-{server}-mcp")],
                    url: None,
                    env: BTreeMap::new(),
                },
            );
        }

        let error = NativeMobLaunchPlan::new(
            SpawnOrResumeExecutionInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-277".to_string()),
                },
                workspace_key: WorkspaceKey("LUC-277".to_string()),
                template_id: MobTemplateId("implementation_mob".to_string()),
                worktree_path: Some("/tmp/worktrees/LUC-277".to_string()),
                instructions: Some("Implement the issue.".to_string()),
                idempotency_key: key,
            },
            template,
            mcp_servers,
            BTreeSet::new(),
            Some("meerkat-fugue-queue".to_string()),
            None,
        )
        .unwrap_err();

        assert!(error.message.contains("planner"));
        assert!(error.message.contains("\"mob\""));
        assert!(error.message.contains("delegate()"));
        assert!(error.message.contains("before native member startup"));
    }

    #[test]
    fn native_launch_plan_requires_explicit_worktree_and_instructions() {
        let key = IdempotencyKey("native-execution:LUC-171".to_string());
        let input = SpawnOrResumeExecutionInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-171".to_string()),
            },
            workspace_key: WorkspaceKey("LUC-171".to_string()),
            template_id: MobTemplateId("implementation_mob".to_string()),
            worktree_path: None,
            instructions: Some("instructions".to_string()),
            idempotency_key: key.clone(),
        };
        let template = crate::config::MaterializedMobTemplate {
            backend: crate::config::MobExecutionBackend::NativeExecution,
            members: vec![crate::config::MaterializedMobMember {
                name: "coder".to_string(),
                profile: "coder".to_string(),
                agent: crate::config::NativeAgentProfile {
                    model: None,
                    provider: None,
                    auth_binding: None,
                    max_tokens: None,
                    system_prompt: None,
                    output_schema: None,
                    structured_output_retries: None,
                    provider_params: None,
                    tools: Vec::new(),
                    skills: Vec::new(),
                    additional_instructions: None,
                },
            }],
            flow: Some(crate::config::NativeMobFlow::Solo),
            linear_projection: None,
            max_review_cycles: None,
        };

        let error = NativeMobLaunchPlan::new(
            input,
            template,
            BTreeMap::new(),
            BTreeSet::new(),
            None,
            None,
        )
        .unwrap_err();

        assert_eq!(error.code, FugueToolErrorCode::InvalidInput);
        assert_eq!(error.idempotency_key, Some(key));
        assert!(error.message.contains("worktree_path"));
    }
}
