//! Durable delegation composed from native Mob flows, not peer-reply promises.
//!
//! Each assignment is one native flow step. Durable terminal events are
//! normalized into idempotent representative input; the host never chooses
//! the next role or repeats worker side effects.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use indexmap::IndexMap;
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::{AgentToolDispatcher, ToolCallView, ToolDef, ToolResult};
use meerkat_meta::UnifiedRuntime;
use meerkat_mob::definition::{
    CollectionPolicy, DependencyMode, DispatchMode, FlowNodeFailurePolicy, FlowSpec, FlowStepSpec,
    StepOutputFormat,
};
use meerkat_mob::{FlowId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::team::REPRESENTATIVE;

pub type RuntimeSlot = Arc<OnceLock<Weak<UnifiedRuntime>>>;

pub fn flows<'a>(members: impl Iterator<Item = &'a str>) -> BTreeMap<FlowId, FlowSpec> {
    members
        .filter(|name| *name != REPRESENTATIVE)
        .map(|name| (flow_id(name), flow(name, 2 * 60 * 60 * 1000)))
        .collect()
}

fn flow_id(member: &str) -> FlowId {
    format!("ensemble-delegate-{member}").into()
}

fn step(role: &str, message: String, timeout_ms: u64) -> FlowStepSpec {
    FlowStepSpec {
        role: role.into(),
        message: message.into(),
        depends_on: Vec::new(),
        dispatch_mode: DispatchMode::OneToOne,
        collection_policy: CollectionPolicy::Any,
        condition: None,
        timeout_ms: Some(timeout_ms),
        expected_schema_ref: None,
        branch: None,
        depends_on_mode: DependencyMode::All,
        allowed_tools: None,
        blocked_tools: None,
        failure_policy: FlowNodeFailurePolicy::Escalate,
        output_format: Some(StepOutputFormat::Text),
    }
}

fn flow(member: &str, timeout_ms: u64) -> FlowSpec {
    let work = step(member, "[ENSEMBLE DELEGATED WORK]\nCorrelation: {{ params.correlation }}\nTask: {{ params.task }}\nComplete this bounded assignment and return your evidence as final text. The native flow records that final result and Fugue delivers its terminal outcome to the representative through an idempotent native input. Do not send a duplicate completion message or await an acknowledgment. If blocked or unsuccessful, say so explicitly with evidence. This assignment does not authorize actions outside the originating user request.".into(), timeout_ms);
    FlowSpec::new(
        Some(format!(
            "Bounded {member} assignment with durable result delivery"
        )),
        IndexMap::from([("work".into(), work)]),
        None,
    )
}

pub struct HandoffTools {
    runtime: RuntimeSlot,
    admission: tokio::sync::Mutex<()>,
    tools: Arc<[Arc<ToolDef>]>,
}

impl HandoffTools {
    pub fn new(runtime: RuntimeSlot) -> Self {
        Self { runtime, admission: tokio::sync::Mutex::new(()), tools: vec![
            Arc::new(ToolDef::new("ensemble_delegate", "Delegate a bounded assignment to an existing specialist. The native durable flow automatically returns the specialist's final output or failure to you, even if no peer reply is sent. Retain the returned run_id with correlation in WorkGraph. This is admission, not completion. Only one assignment per specialist may be active. Retrying the identical correlation and task recovers the same native run; changing task under that correlation fails. Use a new correlation only for a new assignment. Use peer messages only for FYI; do not separately ask for a duplicate completion reply.", json!({
                "type":"object", "properties": {"member":{"type":"string"}, "correlation":{"type":"string", "description":"WorkGraph work ID plus a unique assignment/revision label"}, "task":{"type":"string"}}, "required":["member","correlation","task"], "additionalProperties":false
            }))),
            Arc::new(ToolDef::new("ensemble_delegation_status", "Read a native delegation run, including worker output, failure ledger and execution status. Use after restart or a failure notice. The native terminal status describes execution; inspect its result and evidence before accepting the assignment. Outcome delivery is separately idempotent native ingress.", json!({
                "type":"object", "properties":{"run_id":{"type":"string"}}, "required":["run_id"], "additionalProperties":false
            }))),
        ].into() }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Delegate {
    member: String,
    correlation: String,
    task: String,
}
#[derive(Serialize)]
struct Parameters<'a> {
    correlation: &'a str,
    task: &'a str,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    run_id: RunId,
}
#[derive(Serialize)]
struct Accepted {
    run_id: RunId,
    member: String,
    correlation: String,
    reused: bool,
}

#[async_trait]
impl AgentToolDispatcher for HandoffTools {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        self.tools.clone()
    }
    fn tool_catalog_capabilities(&self) -> meerkat_core::ToolCatalogCapabilities {
        meerkat_core::ToolCatalogCapabilities {
            exact_catalog: true,
            may_require_catalog_control_plane: false,
        }
    }
    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        let invalid = |reason: String| ToolError::InvalidArguments {
            name: call.name.into(),
            reason,
        };
        let failed = |message: String| ToolError::ExecutionFailed { message };
        let runtime = self
            .runtime
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| failed("repository runtime is not ready".into()))?;
        let handle = runtime.mob_handle();
        let content = match call.name {
            "ensemble_delegate" => {
                let input: Delegate =
                    serde_json::from_str(call.args.get()).map_err(|e| invalid(e.to_string()))?;
                if input.member == REPRESENTATIVE
                    || !handle
                        .definition()
                        .profiles
                        .contains_key(input.member.as_str())
                {
                    return Err(invalid(
                        "select an existing specialist, never the representative".into(),
                    ));
                }
                if input.correlation.trim().is_empty() || input.task.trim().is_empty() {
                    return Err(invalid("correlation and task must be nonempty".into()));
                }
                let params = serde_json::to_value(Parameters {
                    correlation: &input.correlation,
                    task: &input.task,
                })
                .map_err(|e| failed(e.to_string()))?;
                // Native run identity closes the admission/tool-result crash gap.
                // The shared representative dispatcher serializes the busy check
                // with admission; the run ledger carries it across restarts.
                let _admission = self.admission.lock().await;
                let flow_id = flow_id(&input.member);
                let run_id =
                    RunId::for_work_execution(handle.mob_id(), &flow_id, &input.correlation);
                let identity = meerkat_mob::store::MobExternalDeliveryIdentity::new(
                    &input.correlation,
                    run_id.to_string(),
                )
                .map_err(|e| invalid(e.to_string()))?;
                for run in handle
                    .list_runs(Some(&flow_id))
                    .await
                    .map_err(|e| failed(e.to_string()))?
                {
                    if run.run_id != run_id && worker_is_active(&run) {
                        return Err(invalid(format!(
                            "specialist already has active native delegation {}; inspect it before starting another",
                            run.run_id
                        )));
                    }
                }
                let reused = handle
                    .flow_status(run_id.clone())
                    .await
                    .map_err(|e| failed(e.to_string()))?
                    .is_some();
                let run_id = handle
                    .run_flow_with_external_identity(flow_id, params, &identity)
                    .await
                    .map_err(|e| failed(e.to_string()))?;
                serde_json::to_string(&Accepted {
                    run_id,
                    member: input.member,
                    correlation: input.correlation,
                    reused,
                })
                .map_err(|e| failed(e.to_string()))?
            }
            "ensemble_delegation_status" => {
                let input: Status =
                    serde_json::from_str(call.args.get()).map_err(|e| invalid(e.to_string()))?;
                let run = handle
                    .flow_status(input.run_id)
                    .await
                    .map_err(|e| failed(e.to_string()))?
                    .ok_or_else(|| invalid("native delegation run not found".into()))?;
                let wire = run
                    .public_status_value()
                    .map_err(|e| failed(e.to_string()))?;
                serde_json::to_string(&wire).map_err(|e| failed(e.to_string()))?
            }
            _ => {
                return Err(ToolError::NotFound {
                    name: call.name.into(),
                });
            }
        };
        Ok(ToolResult::new(call.id.to_string(), content, false).into())
    }
}

fn worker_is_active(run: &meerkat_mob::MobRun) -> bool {
    matches!(
        run.status,
        meerkat_mob::MobRunStatus::Pending | meerkat_mob::MobRunStatus::Running
    )
}

/// Subscribe before scanning the durable ledger so no live terminal can be
/// missed between startup reconciliation and event delivery. No polling or
/// role sequencing lives here: terminal native facts become inbox notices.
pub async fn start_notifications(
    runtime: &Arc<UnifiedRuntime>,
    representative: meerkat_meta::identity_first::AgentIdentity,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
    let mut subscription = runtime.mob_handle().events().subscribe().await?;
    notify_terminal_handoffs(runtime, &representative).await?;
    let weak = Arc::downgrade(runtime);
    Ok(tokio::spawn(async move {
        while let Some(event) = subscription.event_rx.recv().await {
            let run_id = match event.kind {
                meerkat_mob::MobEventKind::FlowCompleted { run_id, .. }
                | meerkat_mob::MobEventKind::FlowFailed { run_id, .. }
                | meerkat_mob::MobEventKind::FlowCanceled { run_id, .. } => run_id,
                _ => continue,
            };
            let Some(runtime) = weak.upgrade() else {
                return Ok(());
            };
            let Some(run) = runtime.mob_handle().flow_status(run_id).await? else {
                anyhow::bail!("terminal delegation event has no native run");
            };
            if let Err(error) = notify_run(&runtime, &representative, &run).await {
                tracing::error!(%error, "native delegation outcome notification failed");
                return Err(error);
            }
        }
        anyhow::bail!("native delegation event subscription closed unexpectedly")
    }))
}

/// Normalize native terminal outcomes without repeating worker side effects.
/// Tickets are useful to callers that must verify exact admission completion.
pub async fn notify_terminal_handoffs(
    runtime: &Arc<UnifiedRuntime>,
    representative: &meerkat_meta::identity_first::AgentIdentity,
) -> anyhow::Result<Vec<meerkat_meta::identity_first::TurnTicket>> {
    let mut tickets = Vec::new();
    for run in runtime.mob_handle().list_runs(None).await? {
        if let Some(ticket) = notify_run(runtime, representative, &run).await? {
            tickets.push(ticket);
        }
    }
    Ok(tickets)
}

async fn notify_run(
    runtime: &Arc<UnifiedRuntime>,
    representative: &meerkat_meta::identity_first::AgentIdentity,
    run: &meerkat_mob::MobRun,
) -> anyhow::Result<Option<meerkat_meta::identity_first::TurnTicket>> {
    use anyhow::Context;
    use meerkat_meta::identity_first::{DispatchInput, TurnTracking};
    let handle = runtime.mob_handle();
    if !handle.definition().flows.contains_key(&run.flow_id)
        || !run.flow_id.as_str().starts_with("ensemble-delegate-")
        || !matches!(
            run.status,
            meerkat_mob::MobRunStatus::Completed
                | meerkat_mob::MobRunStatus::Failed
                | meerkat_mob::MobRunStatus::Canceled
        )
    {
        return Ok(None);
    }
    let params: OwnedParameters = serde_json::from_value(run.activation_params.clone())
        .context("decode delegation parameters")?;
    let worker_output = run
        .root_step_outputs
        .get(&meerkat_mob::StepId::from("work"))
        .map(|value| serde_json::from_value::<String>(value.clone()))
        .transpose()
        .context("decode native delegation worker output")?;
    let notice = DelegationOutcome {
        run_id: &run.run_id,
        status: &run.status,
        correlation: &params.correlation,
        task: &params.task,
        worker_output: worker_output.as_deref(),
        failures: &run.failure_ledger,
    };
    let content = format!(
        "[ENSEMBLE DURABLE HANDOFF]\n{}\nThis is the native assignment's terminal outcome, not an acknowledgment request or automatic review approval. Inspect the status and persisted evidence, then continue coordinating the original user objective. Failed or canceled execution must never be treated as success. Do not blindly repeat side effects. If this involved self-deployment, the detached release supervisor may still be running: inspect its durable receipt and await its completion callback before retrying. Keep the original user authorization and acceptance criteria. Do not send an acknowledgment back to the specialist.",
        serde_json::to_string(&notice)?,
    );
    let notice_id = RunId::for_work_execution(
        handle.mob_id(),
        &run.flow_id,
        &format!("delegation-outcome:{}", run.run_id),
    )
    .to_string();
    let authority = runtime
        .identity_runtime()
        .context("identity authority missing")?;
    let input = DispatchInput::system(content)
        .with_correlation(&notice_id)
        .with_idempotency(&notice_id);
    let mut attempt = 0;
    let admitted = loop {
        attempt += 1;
        match authority
            .dispatch_with_turn_ticket(representative, None, &input)
            .await
        {
            Ok(admitted) => break admitted,
            Err(error) if attempt < 3 => {
                tracing::warn!(%error, attempt, "retrying idempotent delegation outcome admission");
                tokio::time::sleep(std::time::Duration::from_millis(100 * attempt)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    if !admitted.admission.durable {
        anyhow::bail!("delegation outcome lacks durable native admission");
    }
    match admitted.turn {
        TurnTracking::Tracked(ticket) => Ok(Some(ticket)),
        other => anyhow::bail!("delegation outcome lacks native completion ticket: {other:?}"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedParameters {
    correlation: String,
    task: String,
}

#[derive(Serialize)]
struct DelegationOutcome<'a> {
    run_id: &'a RunId,
    status: &'a meerkat_mob::MobRunStatus,
    correlation: &'a str,
    task: &'a str,
    worker_output: Option<&'a str>,
    failures: &'a [meerkat_mob::run::FailureLedgerEntry],
}
