use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::{MeerkatConfig, ServiceConfig};
use crate::domain::{AgentUpdate, Issue, LiveSession, TokenTotals, WorkerExit, WorkerOutcome};
use crate::error::{FugueError, Result};
use crate::github_tools::GitHubToolHost;
use crate::kapellmeister::{
    KapellmeisterShadowSignal, shadow_signals_from_add_comment_result,
    shadow_signals_from_check_ci_result, shadow_signals_from_tool_result,
};
use crate::linear_tools::{execute_linear_fugue_tool, is_linear_fugue_tool_name};
use crate::tool_layer::{FugueToolLayer, after_run_if_workspace_exists};
use crate::tools::FugueToolOperation;
use crate::tracker::{IssueTracker, LinearClient};
use crate::workspace::WorkspaceManager;

#[derive(Debug)]
pub enum WorkerMessage {
    AgentUpdate {
        issue_id: String,
        update: Box<AgentUpdate>,
    },
    WorkerFinished {
        issue_id: String,
        outcome: Box<WorkerOutcome>,
    },
    ShadowSignals {
        issue_id: String,
        signals: Vec<KapellmeisterShadowSignal>,
    },
}

pub async fn run_agent_worker(
    mut issue: Issue,
    attempt: Option<u32>,
    config: ServiceConfig,
    tx: mpsc::Sender<WorkerMessage>,
    cancel: CancellationToken,
) {
    let issue_id = issue.id.clone();
    let outcome = match run_agent_worker_inner(&mut issue, attempt, &config, &tx, cancel).await {
        Ok(session) => WorkerOutcome {
            exit: WorkerExit::Normal,
            error: None,
            session,
            final_issue: issue.clone(),
        },
        Err(err) => WorkerOutcome {
            exit: classify_error(&err),
            error: Some(err.to_string()),
            session: LiveSession::default(),
            final_issue: issue.clone(),
        },
    };
    let _ = tx
        .send(WorkerMessage::WorkerFinished {
            issue_id,
            outcome: Box::new(outcome),
        })
        .await;
}

async fn run_agent_worker_inner(
    issue: &mut Issue,
    attempt: Option<u32>,
    config: &ServiceConfig,
    tx: &mpsc::Sender<WorkerMessage>,
    cancel: CancellationToken,
) -> Result<LiveSession> {
    let workspace_manager = WorkspaceManager::new(&config.workspace, &config.hooks);
    let tool_layer = FugueToolLayer::new(config);
    let workspace = match tool_layer.create_workspace_for_issue(issue).await {
        Ok(workspace) => workspace,
        Err(err) => {
            after_run_if_workspace_exists(config, &issue.identifier).await;
            return Err(err);
        }
    };
    let result = run_agent_after_workspace(
        issue,
        attempt,
        config,
        tx,
        cancel,
        &tool_layer,
        &workspace.path,
    )
    .await;
    workspace_manager
        .after_run_best_effort(&workspace.path)
        .await;
    result
}

async fn run_agent_after_workspace(
    issue: &mut Issue,
    attempt: Option<u32>,
    config: &ServiceConfig,
    tx: &mpsc::Sender<WorkerMessage>,
    cancel: CancellationToken,
    tool_layer: &FugueToolLayer,
    workspace_path: &Path,
) -> Result<LiveSession> {
    tool_layer.run_repository_setup_for_issue(issue).await?;
    let first_prompt = crate::config::render_prompt(&config.prompt_template, issue, attempt)?;

    let tracker = LinearClient::from_config(&config.tracker)?;
    let mut rpc = RpcProcess::start(
        &config.meerkat,
        &config.tracker,
        workspace_path,
        first_prompt,
        tx.clone(),
        issue.id.clone(),
        issue.identifier.clone(),
    )
    .await?;

    let max_turns = config.agent.max_turns;
    let mut session = rpc.live_session.clone();
    let mut turn_number = if rpc.first_turn_deferred {
        1usize
    } else {
        let refreshed = match tracker
            .fetch_issue_states_by_ids(std::slice::from_ref(&issue.id))
            .await
            .map_err(|err| FugueError::TurnFailed(format!("issue state refresh failed: {err}")))
        {
            Ok(refreshed) => refreshed,
            Err(err) => {
                rpc.stop("worker_end").await;
                return Err(err);
            }
        };
        if let Some(next_issue) = refreshed
            .into_iter()
            .find(|candidate| candidate.id == issue.id)
        {
            *issue = next_issue;
        }
        if !config.tracker.is_active_state(&issue.state) || max_turns <= 1 {
            rpc.stop("worker_end").await;
            return Ok(session);
        }
        2usize
    };
    let result = loop {
        if cancel.is_cancelled() {
            break Err(FugueError::TurnCancelled(
                "worker canceled before turn start".to_string(),
            ));
        }

        let prompt = if turn_number == 1 && rpc.first_turn_deferred {
            config.meerkat.first_turn_activation_prompt.clone()
        } else {
            continuation_prompt(issue, turn_number, max_turns)
        };
        let turn_result = rpc
            .run_turn(&config.meerkat, prompt, turn_number == 1, &cancel)
            .await;
        session = rpc.live_session.clone();
        if let Err(err) = turn_result {
            break Err(err);
        }

        let refreshed = match tracker
            .fetch_issue_states_by_ids(std::slice::from_ref(&issue.id))
            .await
            .map_err(|err| FugueError::TurnFailed(format!("issue state refresh failed: {err}")))
        {
            Ok(refreshed) => refreshed,
            Err(err) => break Err(err),
        };
        if let Some(next_issue) = refreshed
            .into_iter()
            .find(|candidate| candidate.id == issue.id)
        {
            *issue = next_issue;
        }
        if !config.tracker.is_active_state(&issue.state) || turn_number >= max_turns {
            break Ok(());
        }
        turn_number += 1;
    };

    rpc.stop("worker_end").await;
    result.map(|_| session)
}

fn continuation_prompt(issue: &Issue, turn_number: usize, max_turns: usize) -> String {
    format!(
        "Continue working on Linear issue {}: {}.\nThis is continuation turn {turn_number}/{max_turns}. Re-check the issue state, update the Workpad with substantive evidence, and follow the workflow-defined authority for handoff state. Do not repeat the full original task prompt; use the existing session history.",
        issue.identifier, issue.title
    )
}

fn classify_error(err: &FugueError) -> WorkerExit {
    match err {
        FugueError::TurnTimeout(_) => WorkerExit::TimedOut,
        FugueError::TurnCancelled(_) => WorkerExit::CanceledByReconciliation,
        _ => WorkerExit::Failed,
    }
}

struct RpcProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
    issue_id: String,
    issue_identifier: String,
    updates: mpsc::Sender<WorkerMessage>,
    live_session: LiveSession,
    methods: Vec<String>,
    first_turn_deferred: bool,
    tracker_config: crate::config::TrackerConfig,
}

impl RpcProcess {
    async fn start(
        meerkat: &MeerkatConfig,
        tracker_config: &crate::config::TrackerConfig,
        workspace: &Path,
        first_prompt: String,
        updates: mpsc::Sender<WorkerMessage>,
        issue_id: String,
        issue_identifier: String,
    ) -> Result<Self> {
        let command_line = meerkat.command_line();
        let mut child = Command::new("bash")
            .arg("-lc")
            .arg(&command_line)
            .current_dir(workspace)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| FugueError::MeerkatRpcNotFound(format!("{command_line}: {err}")))?;

        let pid = child.id().map(|pid| pid.to_string());
        if let Some(stderr) = child.stderr.take() {
            let issue_for_log = issue_id.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    debug!(issue_id = %issue_for_log, stderr = %line, "meerkat_rpc stderr");
                }
            });
        }
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| FugueError::ProcessExit("rkat-rpc stdin unavailable".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| FugueError::ProcessExit("rkat-rpc stdout unavailable".to_string()))?;

        let mut rpc = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
            issue_id,
            issue_identifier,
            updates,
            live_session: LiveSession {
                meerkat_rpc_pid: pid,
                ..LiveSession::default()
            },
            methods: Vec::new(),
            first_turn_deferred: false,
            tracker_config: tracker_config.clone(),
        };

        let init = rpc
            .call(
                "initialize",
                json!({}),
                Duration::from_millis(meerkat.read_timeout_ms),
                None,
            )
            .await?;
        let methods = init
            .get("methods")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                FugueError::ResponseError("initialize result did not include methods".to_string())
            })?
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !methods.iter().any(|method| method == "session/create")
            || !methods.iter().any(|method| method == "turn/start")
        {
            return Err(FugueError::ResponseError(
                "rkat-rpc missing required session/create or turn/start".to_string(),
            ));
        }
        rpc.methods = methods;

        let streamable = rpc
            .methods
            .iter()
            .any(|method| method == "session/stream_open");
        let external_tools = external_tools_for_tracker(tracker_config);
        let mut create_params = meerkat.create_session_params(first_prompt, external_tools);
        if !streamable && let Some(obj) = create_params.as_object_mut() {
            obj.insert(
                "initial_turn".to_string(),
                Value::String("run_immediately".to_string()),
            );
        }
        let create = rpc
            .call(
                "session/create",
                create_params,
                if streamable {
                    Duration::from_millis(meerkat.read_timeout_ms)
                } else {
                    Duration::from_millis(meerkat.turn_timeout_ms)
                },
                None,
            )
            .await?;
        let session_id = create
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FugueError::ResponseError("session/create missing session_id".to_string())
            })?
            .to_string();
        let session_ref = create
            .get("session_ref")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        rpc.live_session.session_id = Some(session_id.clone());
        rpc.live_session.session_ref = session_ref.clone();
        let mut startup_usage = None;
        if !streamable {
            rpc.live_session.turn_count = 1;
            if let Some(usage) = extract_usage(&create) {
                rpc.live_session.meerkat_tokens = usage.clone();
                startup_usage = Some(usage);
            }
        }
        rpc.emit_update("session_started", None, startup_usage, None)
            .await;

        if streamable {
            rpc.first_turn_deferred = true;
            let open = rpc
                .call(
                    "session/stream_open",
                    json!({ "session_id": session_id }),
                    Duration::from_millis(meerkat.read_timeout_ms),
                    None,
                )
                .await
                .inspect_err(|err| {
                    rpc.live_session.stream_error = Some(err.to_string());
                });
            let open = match open {
                Ok(open) => open,
                Err(err) => {
                    rpc.emit_update("stream_open_failed", Some(err.to_string()), None, None)
                        .await;
                    return Ok(rpc);
                }
            };
            let stream_id = open
                .get("stream_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    FugueError::ResponseError("session/stream_open missing stream_id".to_string())
                })
                .inspect_err(|err| {
                    rpc.live_session.stream_error = Some(err.to_string());
                });
            let stream_id = match stream_id {
                Ok(stream_id) => stream_id,
                Err(err) => {
                    rpc.emit_update("stream_open_failed", Some(err.to_string()), None, None)
                        .await;
                    return Ok(rpc);
                }
            }
            .to_string();
            rpc.live_session.stream_id = Some(stream_id.clone());
            rpc.live_session.stream_opened_at = Some(Utc::now());
        }

        Ok(rpc)
    }

    async fn run_turn(
        &mut self,
        meerkat: &MeerkatConfig,
        prompt: String,
        pending_first_turn: bool,
        cancel: &CancellationToken,
    ) -> Result<Value> {
        let session_id = self.live_session.session_id.clone().ok_or_else(|| {
            FugueError::ResponseError("turn/start without session_id".to_string())
        })?;
        let materialized = self.live_session.turn_count > 0;
        let params =
            meerkat.turn_start_params(session_id, prompt, pending_first_turn, materialized);
        let request_id_number = self.alloc_id();
        let request_id = request_id_number.to_string();
        self.live_session.active_turn_request_id = Some(request_id.clone());
        self.live_session.turn_count += 1;
        self.emit_update(
            "turn_started",
            Some(format!("request_id={request_id}")),
            None,
            None,
        )
        .await;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let interrupt_session = self.live_session.session_id.clone();
                let _ = self.call("turn/interrupt", json!({"session_id": interrupt_session}), Duration::from_millis(meerkat.read_timeout_ms), None).await;
                Err(FugueError::TurnCancelled("worker cancellation requested".to_string()))
            }
            result = self.call("turn/start", params, Duration::from_millis(meerkat.turn_timeout_ms), Some(request_id_number)) => {
                let value = result?;
                if let Some(usage) = extract_usage(&value) {
                    self.live_session.meerkat_tokens = usage.clone();
                    self.emit_update("turn_completed", None, Some(usage), extract_rate_limits(&value))
                        .await;
                } else {
                    self.emit_update("turn_completed", None, None, extract_rate_limits(&value))
                        .await;
                }
                Ok(value)
            }
        }
    }

    async fn stop(&mut self, reason: &str) {
        let had_open_stream =
            self.live_session.stream_id.is_some() && self.live_session.stream_closed_at.is_none();
        if had_open_stream && let Some(stream_id) = self.live_session.stream_id.clone() {
            let close_result = self
                .call(
                    "session/stream_close",
                    json!({ "stream_id": stream_id }),
                    Duration::from_millis(2_000),
                    None,
                )
                .await;
            match close_result {
                Ok(value) => {
                    self.mark_stream_closed(reason);
                    self.emit_update("stream_closed", None, None, extract_rate_limits(&value))
                        .await;
                }
                Err(err) => {
                    self.live_session.stream_error = Some(err.to_string());
                    self.emit_update("stream_close_failed", Some(err.to_string()), None, None)
                        .await;
                }
            }
        }
        let _ = self.child.kill().await;
        if had_open_stream && self.live_session.stream_closed_at.is_none() {
            self.mark_stream_closed(reason);
            self.emit_update("stream_closed_after_exit", None, None, None)
                .await;
        }
    }

    fn mark_stream_closed(&mut self, reason: &str) {
        self.live_session.stream_closed_at = Some(Utc::now());
        self.live_session.stream_close_reason = Some(reason.to_string());
    }

    async fn call(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
        forced_id: Option<u64>,
    ) -> Result<Value> {
        let id = forced_id.unwrap_or_else(|| self.alloc_id());
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

        let deadline = Instant::now() + timeout;
        loop {
            let now = Instant::now();
            if now >= deadline {
                let category = if method == "turn/start" {
                    FugueError::TurnTimeout(format!("{method} timed out after {timeout:?}"))
                } else {
                    FugueError::ResponseTimeout(format!("{method} timed out after {timeout:?}"))
                };
                self.record_stream_read_error(&category).await;
                return Err(category);
            }
            let remaining = deadline.saturating_duration_since(now);
            let read_result = tokio::time::timeout(remaining, self.stdout.next_line()).await;
            let line = match read_result {
                Ok(Ok(Some(line))) => line,
                Ok(Ok(None)) => {
                    let err = FugueError::ProcessExit("rkat-rpc stdout closed".to_string());
                    self.record_stream_read_error(&err).await;
                    return Err(err);
                }
                Ok(Err(err)) => {
                    let err = FugueError::ProcessExit(format!("read failed: {err}"));
                    self.record_stream_read_error(&err).await;
                    return Err(err);
                }
                Err(_) => {
                    let err = if method == "turn/start" {
                        FugueError::TurnTimeout(format!("{method} timed out after {timeout:?}"))
                    } else {
                        FugueError::ResponseTimeout(format!("{method} timed out after {timeout:?}"))
                    };
                    self.record_stream_read_error(&err).await;
                    return Err(err);
                }
            };
            let value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(err) => {
                    self.emit_update("malformed", Some(format!("{err}: {line}")), None, None)
                        .await;
                    continue;
                }
            };

            if value.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = value.get("error") {
                    return Err(map_rpc_error(method, error));
                }
                return value
                    .get("result")
                    .cloned()
                    .ok_or_else(|| FugueError::ResponseError(format!("{method} missing result")));
            }

            self.handle_inbound(value).await?;
        }
    }

    async fn record_stream_read_error(&mut self, err: &FugueError) {
        if self.live_session.stream_id.is_some() && self.live_session.stream_closed_at.is_none() {
            self.live_session.stream_error = Some(err.to_string());
            self.emit_update("stream_read_failed", Some(err.to_string()), None, None)
                .await;
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    async fn handle_inbound(&mut self, value: Value) -> Result<()> {
        if value.get("method").and_then(Value::as_str) == Some("tool/execute") {
            self.handle_tool_execute(value).await?;
            return Ok(());
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("other_message");
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "session/event" => {
                if ignore_unscoped_session_event(&self.live_session) {
                    debug!(
                        issue_id = %self.issue_id,
                        issue_identifier = %self.issue_identifier,
                        session_id = self.live_session.session_id.as_deref().unwrap_or(""),
                        stream_id = self.live_session.stream_id.as_deref().unwrap_or(""),
                        "ignored unscoped session event while stream is open"
                    );
                    return Ok(());
                }
                self.handle_agent_event(&params, None).await
            }
            "session/stream_event" => {
                let stream_id = params.get("stream_id").and_then(Value::as_str);
                if stream_id != self.live_session.stream_id.as_deref() {
                    self.emit_update(
                        "other_message",
                        Some("ignored unrelated stream event".to_string()),
                        None,
                        None,
                    )
                    .await;
                    return Ok(());
                }
                let sequence = params.get("sequence").and_then(Value::as_u64);
                self.live_session.last_stream_sequence = sequence;
                self.handle_agent_event(&params, sequence).await
            }
            "session/stream_end" => {
                let stream_id = params.get("stream_id").and_then(Value::as_str);
                if stream_id == self.live_session.stream_id.as_deref() {
                    self.live_session.stream_closed_at = Some(Utc::now());
                    self.live_session.stream_close_reason = params
                        .get("outcome")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                    if let Some(error) = params.get("error") {
                        let error = summarize_error(error);
                        self.live_session.stream_error = Some(error.clone());
                        self.emit_update("stream_closed", None, None, extract_rate_limits(&params))
                            .await;
                        return Err(FugueError::TurnFailed(format!(
                            "stream ended with error: {error}"
                        )));
                    }
                    self.emit_update("stream_closed", None, None, extract_rate_limits(&params))
                        .await;
                }
                Ok(())
            }
            _ => {
                self.emit_update(
                    method,
                    Some(params.to_string()),
                    None,
                    extract_rate_limits(&params),
                )
                .await;
                Ok(())
            }
        }
    }

    async fn handle_agent_event(&mut self, params: &Value, sequence: Option<u64>) -> Result<()> {
        let event = parse_agent_event(params);
        let event_type = event.event_type;
        let message = event.message;
        let usage = event.usage;
        let rate_limits = event.rate_limits;
        if let Some(usage) = &usage {
            self.live_session.meerkat_tokens = usage.clone();
        }
        self.live_session.last_stream_sequence =
            sequence.or(self.live_session.last_stream_sequence);
        self.emit_update(&event_type, message.clone(), usage, rate_limits)
            .await;
        if is_input_required_event(&event_type) {
            return Err(FugueError::TurnInputRequired(
                message.unwrap_or_else(|| "agent requested user input".to_string()),
            ));
        }
        if event_type == "turn_cancelled" || event_type == "run_cancelled" {
            return Err(FugueError::TurnCancelled(
                message.unwrap_or_else(|| "turn cancelled".to_string()),
            ));
        }
        if event_type == "turn_failed" || event_type == "run_failed" {
            return Err(FugueError::TurnFailed(
                message.unwrap_or_else(|| "turn failed".to_string()),
            ));
        }
        Ok(())
    }

    async fn handle_tool_execute(&mut self, value: Value) -> Result<()> {
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
        let (is_error, content, shadow_signals) = if is_linear_fugue_tool_name(name) {
            let (is_error, content) =
                execute_linear_fugue_tool(&self.tracker_config, name, &arguments).await;
            let shadow_signals = shadow_signals_from_callback_content(name, &arguments, &content);
            (is_error, content, shadow_signals)
        } else if is_github_fugue_tool_name(name)
            || name.starts_with("github.")
            || name.starts_with("github_")
        {
            execute_github_fugue_tool(name, &arguments)
        } else {
            let (is_error, content) = unsupported_tool_callback(name);
            (is_error, content, Vec::new())
        };
        if !shadow_signals.is_empty() {
            let _ = self
                .updates
                .send(WorkerMessage::ShadowSignals {
                    issue_id: self.issue_id.clone(),
                    signals: shadow_signals,
                })
                .await;
        }
        let response = json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": content,
                "is_error": is_error,
            }
        });
        let mut encoded = serde_json::to_vec(&response)
            .map_err(|err| FugueError::ResponseError(err.to_string()))?;
        encoded.push(b'\n');
        self.stdin
            .write_all(&encoded)
            .await
            .map_err(|err| FugueError::ProcessExit(format!("tool response write failed: {err}")))?;
        self.stdin
            .flush()
            .await
            .map_err(|err| FugueError::ProcessExit(format!("tool response flush failed: {err}")))?;
        self.emit_update(
            "policy_auto_resolved",
            Some(format!("tool={name} is_error={is_error}")),
            None,
            None,
        )
        .await;
        Ok(())
    }

    async fn emit_update(
        &mut self,
        event: &str,
        message: Option<String>,
        absolute_usage: Option<TokenTotals>,
        rate_limits: Option<Value>,
    ) {
        let now = Utc::now();
        self.live_session.last_meerkat_event = Some(event.to_string());
        self.live_session.last_meerkat_timestamp = Some(now);
        if let Some(message) = &message {
            self.live_session.last_meerkat_message = Some(message.clone());
        }
        info!(
            issue_id = %self.issue_id,
            issue_identifier = %self.issue_identifier,
            session_id = self.live_session.session_id.as_deref().unwrap_or(""),
            session_ref = self.live_session.session_ref.as_deref().unwrap_or(""),
            event = event,
            message = message.as_deref().unwrap_or(""),
            "meerkat_session_event"
        );
        let update = AgentUpdate {
            event: event.to_string(),
            timestamp: now,
            session_id: self.live_session.session_id.clone(),
            session_ref: self.live_session.session_ref.clone(),
            active_turn_request_id: self.live_session.active_turn_request_id.clone(),
            stream_id: self.live_session.stream_id.clone(),
            stream_sequence: self.live_session.last_stream_sequence,
            stream_close_reason: self.live_session.stream_close_reason.clone(),
            stream_error: self.live_session.stream_error.clone(),
            meerkat_rpc_pid: self.live_session.meerkat_rpc_pid.clone(),
            message,
            absolute_usage,
            rate_limits,
            turn_count: Some(self.live_session.turn_count),
        };
        let _ = self
            .updates
            .send(WorkerMessage::AgentUpdate {
                issue_id: self.issue_id.clone(),
                update: Box::new(update),
            })
            .await;
    }
}

pub(crate) fn is_github_fugue_tool_name(name: &str) -> bool {
    matches!(
        name.parse::<FugueToolOperation>(),
        Ok(FugueToolOperation::GitHubInspectRepo
            | FugueToolOperation::GitHubInspectPr
            | FugueToolOperation::GitHubCreatePr
            | FugueToolOperation::GitHubCheckCi
            | FugueToolOperation::GitHubMergePr
            | FugueToolOperation::GitHubLinkIssue)
    )
}

pub fn execute_github_fugue_tool(
    name: &str,
    arguments: &Value,
) -> (bool, String, Vec<KapellmeisterShadowSignal>) {
    let operation = match name.parse::<FugueToolOperation>() {
        Ok(operation) => operation,
        Err(_) => {
            let (is_error, content) = unsupported_tool_callback(name);
            return (is_error, content, Vec::new());
        }
    };
    let host = GitHubToolHost::new();
    let mut source_signals = None;
    let result = match operation {
        FugueToolOperation::GitHubInspectRepo => serde_json::from_value(arguments.clone())
            .map(|input| host.inspect_repo(input))
            .unwrap_or_else(|err| github_parse_error(operation, err)),
        FugueToolOperation::GitHubInspectPr => serde_json::from_value(arguments.clone())
            .map(|input| host.inspect_pr(input))
            .unwrap_or_else(|err| github_parse_error(operation, err)),
        FugueToolOperation::GitHubCreatePr => serde_json::from_value(arguments.clone())
            .map(|input| host.create_pr(input))
            .unwrap_or_else(|err| github_parse_error(operation, err)),
        FugueToolOperation::GitHubCheckCi => match serde_json::from_value(arguments.clone()) {
            Ok(input) => {
                let input: crate::tools::CheckCiInput = input;
                let result = host.check_ci(input.clone());
                if let crate::tools::FugueToolResult::Ok {
                    output: crate::tools::FugueToolOutput::GitHubCheckCi(output),
                    metadata,
                } = &result
                {
                    let observed_at = metadata
                        .completed_at
                        .or(metadata.started_at)
                        .unwrap_or_else(Utc::now);
                    source_signals = Some(shadow_signals_from_check_ci_result(
                        &input,
                        output,
                        observed_at,
                    ));
                }
                result
            }
            Err(err) => github_parse_error(operation, err),
        },
        FugueToolOperation::GitHubMergePr => serde_json::from_value(arguments.clone())
            .map(|input| host.merge_pr(input))
            .unwrap_or_else(|err| github_parse_error(operation, err)),
        FugueToolOperation::GitHubLinkIssue => serde_json::from_value(arguments.clone())
            .map(|input| host.link_issue(input))
            .unwrap_or_else(|err| github_parse_error(operation, err)),
        _ => {
            let (is_error, content) = unsupported_tool_callback(name);
            return (is_error, content, Vec::new());
        }
    };
    let signals = source_signals.unwrap_or_else(|| shadow_signals_from_tool_result(&result));
    if !signals.is_empty() {
        info!(
            count = signals.len(),
            "kapellmeister shadow signals normalized from github tool result"
        );
    }
    let is_error = matches!(result, crate::tools::FugueToolResult::Err { .. });
    let content = serde_json::to_string(&result)
        .unwrap_or_else(|err| json!({ "error": err.to_string() }).to_string());
    (is_error, content, signals)
}

fn shadow_signals_from_callback_content(
    name: &str,
    arguments: &Value,
    content: &str,
) -> Vec<KapellmeisterShadowSignal> {
    let Ok(result) = serde_json::from_str::<crate::tools::FugueToolResult>(content) else {
        return Vec::new();
    };
    if matches!(
        name.parse::<FugueToolOperation>(),
        Ok(FugueToolOperation::LinearAddComment)
    ) && let crate::tools::FugueToolResult::Ok {
        output: crate::tools::FugueToolOutput::LinearAddComment(output),
        metadata,
    } = &result
        && let Ok(input) =
            serde_json::from_value::<crate::tools::AddCommentInput>(arguments.clone())
    {
        let observed_at = metadata
            .completed_at
            .or(metadata.started_at)
            .unwrap_or_else(Utc::now);
        return shadow_signals_from_add_comment_result(&input, output, observed_at);
    }
    shadow_signals_from_tool_result(&result)
}

fn github_parse_error(
    operation: FugueToolOperation,
    error: serde_json::Error,
) -> crate::tools::FugueToolResult {
    crate::tools::FugueToolResult::Err {
        error: crate::tools::FugueToolError {
            operation,
            code: crate::tools::FugueToolErrorCode::InvalidInput,
            message: error.to_string(),
            retry: crate::tools::RetryAdvice::DoNotRetry,
            idempotency_key: None,
        },
        metadata: crate::tools::ToolExecutionMetadata::default(),
    }
}

fn unsupported_tool_callback(name: &str) -> (bool, String) {
    (
        true,
        json!({"error": format!("unsupported tool: {name}")}).to_string(),
    )
}

pub fn external_tools_for_tracker(config: &crate::config::TrackerConfig) -> Option<Vec<Value>> {
    let linear_tool_surface_available =
        config.kind == "fugue-ims" || (config.kind == "linear" && config.api_key.is_some());
    if !linear_tool_surface_available {
        return None;
    }
    Some(vec![
        json!({
            "name": FugueToolOperation::LinearInspectIssue.provider_callback_name(),
            "description": "Inspect a Linear issue through Fugue's typed Linear tool layer, including the current Fugue Workpad comment body when present.",
            "input_schema": {
                "type": "object",
                "properties": { "issue": { "type": "object" } },
                "required": ["issue"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::LinearClaimIssue.provider_callback_name(),
            "description": "Assign a Linear issue, optionally moving it to a target state. Idempotent when already claimed/in target state.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "issue": { "type": "object" },
                    "assignee": { "type": "object" },
                    "target_state": { "type": ["object", "string", "null"] },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["issue", "assignee", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::LinearMoveIssue.provider_callback_name(),
            "description": "Move a Linear issue to a workflow state. Safe if the issue is already in the target state.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "issue": { "type": "object" },
                    "state": { "type": ["object", "string"] },
                    "expected_updated_at": { "type": ["string", "null"], "format": "date-time" },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["issue", "state", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::LinearUpsertWorkpad.provider_callback_name(),
            "description": "Create or update the single persistent Linear workpad comment headed by the requested heading.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "issue": { "type": "object" },
                    "heading": { "type": "string" },
                    "body_markdown": { "type": "string" },
                    "create_if_missing": { "type": "boolean" },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["issue", "heading", "body_markdown", "create_if_missing", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::LinearListProjectStates.provider_callback_name(),
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
        json!({
            "name": FugueToolOperation::LinearEnsureWorkflowStates.provider_callback_name(),
            "description": "Create missing Linear workflow states for a team. Projection authority only; idempotent when states already exist.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "team_id": { "type": "string" },
                    "states": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "state_type": {
                                    "type": "string",
                                    "enum": ["backlog", "unstarted", "started", "completed", "canceled", "duplicate"]
                                }
                            },
                            "required": ["name", "state_type"],
                            "additionalProperties": false
                        }
                    },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["team_id", "states", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::LinearListRecentIssueChanges.provider_callback_name(),
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
        json!({
            "name": FugueToolOperation::GitHubInspectRepo.provider_callback_name(),
            "description": "Inspect a GitHub repository through Fugue's typed GitHub tool layer.",
            "input_schema": {
                "type": "object",
                "properties": { "repo": { "type": "object" } },
                "required": ["repo"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::GitHubInspectPr.provider_callback_name(),
            "description": "Inspect a GitHub pull request through Fugue's typed GitHub tool layer.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "object" },
                    "pull_request": { "type": "object" }
                },
                "required": ["repo", "pull_request"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::GitHubCreatePr.provider_callback_name(),
            "description": "Create a GitHub pull request through Fugue's typed GitHub tool layer. Idempotent when an existing PR match is requested.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "object" },
                    "title": { "type": "string" },
                    "body_markdown": { "type": "string" },
                    "head_branch": { "type": "string" },
                    "base_branch": { "type": "string" },
                    "draft": { "type": "boolean" },
                    "existing_pr_match": { "type": "string", "enum": ["head_branch", "title_and_head_branch", "none"] },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["repo", "title", "body_markdown", "head_branch", "base_branch", "draft", "existing_pr_match", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::GitHubMergePr.provider_callback_name(),
            "description": "Merge a GitHub pull request through Fugue's typed GitHub tool layer after validating the current PR state and optional expected head SHA.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "object" },
                    "pull_request": { "type": "object" },
                    "method": { "type": "string", "enum": ["merge", "squash", "rebase"] },
                    "expected_head_sha": { "type": ["string", "null"] },
                    "idempotency_key": { "type": "string" }
                },
                "required": ["repo", "pull_request", "method", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": FugueToolOperation::GitHubCheckCi.provider_callback_name(),
            "description": "Check GitHub CI for a repository ref through Fugue's typed GitHub tool layer.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "repo": { "type": "object" },
                    "git_ref": { "type": "string" }
                },
                "required": ["repo", "git_ref"],
                "additionalProperties": false
            }
        }),
    ])
}

fn map_rpc_error(method: &str, error: &Value) -> FugueError {
    let text = error.to_string();
    if method == "turn/start" && text.contains("cancel") {
        FugueError::TurnCancelled(text)
    } else if method == "turn/start" {
        FugueError::TurnFailed(text)
    } else {
        FugueError::ResponseError(text)
    }
}

fn summarize_event(event: &Value) -> Option<String> {
    for key in ["message", "text", "delta", "content", "reason"] {
        if let Some(value) = event.get(key).and_then(Value::as_str) {
            return Some(value.chars().take(500).collect());
        }
    }
    event.get("error").map(summarize_error)
}

fn summarize_error(error: &Value) -> String {
    error
        .as_str()
        .map(ToString::to_string)
        .unwrap_or_else(|| error.to_string())
}

struct ParsedAgentEvent {
    event_type: String,
    message: Option<String>,
    usage: Option<TokenTotals>,
    rate_limits: Option<Value>,
}

fn parse_agent_event(params: &Value) -> ParsedAgentEvent {
    let envelope = params.get("event").unwrap_or(params);
    let payload = envelope.get("payload").unwrap_or(envelope);
    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| payload.get("event").and_then(Value::as_str))
        .or_else(|| envelope.get("type").and_then(Value::as_str))
        .or_else(|| envelope.get("event").and_then(Value::as_str))
        .unwrap_or("notification")
        .to_string();
    let message = summarize_event(payload).or_else(|| summarize_event(envelope));
    let usage = extract_event_usage(payload, &event_type)
        .or_else(|| extract_event_usage(envelope, &event_type));
    let rate_limits = extract_rate_limits(payload)
        .or_else(|| extract_rate_limits(envelope))
        .or_else(|| extract_rate_limits(params));

    ParsedAgentEvent {
        event_type,
        message,
        usage,
        rate_limits,
    }
}

fn extract_usage(value: &Value) -> Option<TokenTotals> {
    value
        .get("usage")
        .and_then(extract_token_totals)
        .or_else(|| extract_token_totals(value))
}

fn extract_event_usage(event: &Value, event_type: &str) -> Option<TokenTotals> {
    if is_cumulative_usage_event(event_type) {
        extract_usage(event)
    } else {
        None
    }
}

fn is_cumulative_usage_event(event_type: &str) -> bool {
    matches!(
        event_type.to_ascii_lowercase().as_str(),
        "run_completed"
            | "run_complete"
            | "run_result"
            | "run_succeeded"
            | "usage_totals"
            | "token_totals"
            | "session_usage"
            | "wire_run_result"
    )
}

fn is_input_required_event(event_type: &str) -> bool {
    event_type.contains("input_required")
        || event_type.contains("user_input")
        || event_type == "interaction_callback_pending"
        || event_type == "policy_callback_pending"
}

fn ignore_unscoped_session_event(session: &LiveSession) -> bool {
    session.stream_id.is_some() && session.stream_closed_at.is_none()
}

fn extract_token_totals(value: &Value) -> Option<TokenTotals> {
    let object = value.as_object()?;
    let input_tokens = first_u64(
        object,
        &[
            "input_tokens",
            "inputTokens",
            "prompt_tokens",
            "promptTokens",
            "inputTokenCount",
            "promptTokenCount",
        ],
    )
    .unwrap_or(0);
    let output_tokens = first_u64(
        object,
        &[
            "output_tokens",
            "outputTokens",
            "completion_tokens",
            "completionTokens",
            "generated_tokens",
            "generatedTokens",
            "outputTokenCount",
            "completionTokenCount",
        ],
    )
    .unwrap_or(0);
    let total_tokens = first_u64(
        object,
        &[
            "total_tokens",
            "totalTokens",
            "total_token_count",
            "totalTokenCount",
        ],
    )
    .or_else(|| (input_tokens > 0 || output_tokens > 0).then_some(input_tokens + output_tokens))?;

    Some(TokenTotals {
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

fn first_u64(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_u64))
}

fn extract_rate_limits(value: &Value) -> Option<Value> {
    for key in ["rate_limits", "rate_limit", "rateLimits"] {
        if let Some(rate_limits) = value.get(key) {
            return Some(rate_limits.clone());
        }
    }
    for key in ["payload", "data", "metadata"] {
        if let Some(rate_limits) = value.get(key).and_then(extract_rate_limits) {
            return Some(rate_limits);
        }
    }
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event_type.contains("rate_limit") || event_type.contains("rate-limit") {
        Some(value.clone())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ServiceConfig, TrackerConfig, WorkflowDefinition};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[test]
    fn advertised_linear_tools_are_provider_safe() {
        let tools = external_tools_for_tracker(&TrackerConfig {
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![],
        })
        .unwrap();

        for tool in &tools {
            let name = tool["name"].as_str().expect("tool has string name");
            assert!(
                name.chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'),
                "advertised tool name {name:?} must be provider-safe"
            );
        }
    }

    #[test]
    fn linear_tools_are_typed_and_workpad_first_for_linear() {
        let tools = external_tools_for_tracker(&TrackerConfig {
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![],
        })
        .unwrap();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            names.first().copied(),
            Some(FugueToolOperation::LinearInspectIssue.provider_callback_name())
        );
        assert!(names.contains(&FugueToolOperation::LinearUpsertWorkpad.provider_callback_name()));
        assert!(names.contains(&FugueToolOperation::LinearMoveIssue.provider_callback_name()));
        assert!(!names.contains(&"linear_graphql"));
        assert!(!names.contains(&FugueToolOperation::LinearAddComment.provider_callback_name()));
    }

    #[test]
    fn fugue_ims_exposes_linear_shaped_tool_surface_without_api_key() {
        let tools = external_tools_for_tracker(&TrackerConfig {
            kind: "fugue-ims".to_string(),
            endpoint: "/tmp/fugue-ims.sqlite".to_string(),
            api_key: None,
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![],
        })
        .unwrap();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert!(names.contains(&FugueToolOperation::LinearInspectIssue.provider_callback_name()));
        assert!(names.contains(&FugueToolOperation::LinearMoveIssue.provider_callback_name()));
        assert!(names.contains(&FugueToolOperation::LinearUpsertWorkpad.provider_callback_name()));
    }

    #[test]
    fn advertised_source_signal_tools_include_github_pr_and_ci_callbacks() {
        let tools = external_tools_for_tracker(&TrackerConfig {
            kind: "linear".to_string(),
            endpoint: "https://api.linear.app/graphql".to_string(),
            api_key: Some("token".to_string()),
            project_slug: Some("project".to_string()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            max_attachment_files: 20,
            max_attachment_bytes: 25 * 1024 * 1024,
            max_attachment_total_bytes: 100 * 1024 * 1024,
            terminal_states: vec![],
        })
        .unwrap();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert!(names.contains(&FugueToolOperation::GitHubCreatePr.provider_callback_name()));
        assert!(names.contains(&FugueToolOperation::GitHubMergePr.provider_callback_name()));
        assert!(names.contains(&FugueToolOperation::GitHubCheckCi.provider_callback_name()));
        assert!(!names.contains(&FugueToolOperation::GitHubLinkIssue.provider_callback_name()));
        assert!(is_github_fugue_tool_name("github_create_pr"));
        assert!(is_github_fugue_tool_name("github_merge_pr"));
        assert!(is_github_fugue_tool_name("github_check_ci"));
    }

    #[test]
    fn continuation_prompt_does_not_repeat_template() {
        let prompt = continuation_prompt(
            &Issue {
                id: "id".to_string(),
                identifier: "ABC-1".to_string(),
                title: "Fix it".to_string(),
                description: None,
                priority: None,
                state: "Todo".to_string(),
                branch_name: None,
                url: None,
                labels: vec![],
                blocked_by: vec![],
                attachments: vec![],
                created_at: None,
                updated_at: None,
            },
            2,
            5,
        );
        assert!(prompt.contains("Continue working"));
        assert!(prompt.contains("Do not repeat the full original task prompt"));
    }

    #[test]
    fn generic_event_usage_is_not_counted_as_cumulative() {
        let notification = json!({
            "type": "notification",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15
            }
        });
        assert!(extract_event_usage(&notification, "notification").is_none());

        let turn_completed = json!({
            "type": "turn_completed",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15
            }
        });
        assert!(extract_event_usage(&turn_completed, "turn_completed").is_none());

        let run_completed = json!({
            "type": "run_completed",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15
            }
        });
        assert_eq!(
            extract_event_usage(&run_completed, "run_completed")
                .unwrap()
                .total_tokens,
            15
        );

        let run_completed_with_delta = json!({
            "type": "run_completed",
            "last_token_usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 2
            },
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15
            }
        });
        assert_eq!(
            extract_event_usage(&run_completed_with_delta, "run_completed")
                .unwrap()
                .total_tokens,
            15
        );

        let warning = json!({
            "type": "usage_warning",
            "usage": {
                "input_tokens": 20,
                "output_tokens": 4,
                "total_tokens": 24
            }
        });
        assert!(extract_event_usage(&warning, "usage_warning").is_none());

        let delta = json!({
            "type": "token_delta",
            "input_tokens": 20,
            "output_tokens": 4,
            "total_tokens": 24
        });
        assert!(extract_event_usage(&delta, "token_delta").is_none());

        let compaction = json!({
            "type": "compaction_started",
            "input_tokens": 20,
            "output_tokens": 0,
            "total_tokens": 20
        });
        assert!(extract_event_usage(&compaction, "compaction_started").is_none());
    }

    #[test]
    fn result_usage_is_counted_as_cumulative_with_common_field_names() {
        let result = json!({
            "usage": {
                "prompt_tokens": 7,
                "completion_tokens": 8,
                "total_tokens": 15
            }
        });
        assert_eq!(extract_usage(&result).unwrap().total_tokens, 15);

        let camel_case = json!({
            "inputTokens": 2,
            "outputTokens": 3,
            "totalTokens": 5
        });
        let totals = extract_usage(&camel_case).unwrap();
        assert_eq!(totals.input_tokens, 2);
        assert_eq!(totals.output_tokens, 3);
        assert_eq!(totals.total_tokens, 5);
    }

    #[test]
    fn payload_wrapped_stream_event_is_unwrapped() {
        let wrapped = json!({
            "stream_id": "stream-1",
            "sequence": 4,
            "event": {
                "kind": "agent_event",
                "payload": {
                    "type": "turn_completed",
                    "message": "done",
                    "usage": {
                        "prompt_tokens": 11,
                        "completion_tokens": 12,
                        "total_tokens": 23
                    }
                }
            }
        });

        let parsed = parse_agent_event(&wrapped);
        assert_eq!(parsed.event_type, "turn_completed");
        assert_eq!(parsed.message.as_deref(), Some("done"));
        assert!(parsed.usage.is_none());
    }

    #[test]
    fn payload_wrapped_failure_event_is_unwrapped() {
        let wrapped = json!({
            "event": {
                "payload": {
                    "type": "turn_failed",
                    "error": "model failure"
                }
            }
        });

        let parsed = parse_agent_event(&wrapped);
        assert_eq!(parsed.event_type, "turn_failed");
        assert_eq!(parsed.message.as_deref(), Some("model failure"));
    }

    #[test]
    fn unscoped_session_events_are_ignored_only_while_stream_is_open() {
        let mut session = LiveSession::default();
        assert!(!ignore_unscoped_session_event(&session));

        session.stream_id = Some("stream-1".to_string());
        assert!(ignore_unscoped_session_event(&session));

        session.stream_closed_at = Some(Utc::now());
        assert!(!ignore_unscoped_session_event(&session));
    }

    #[test]
    fn callback_pending_event_is_terminal_input_required() {
        assert!(is_input_required_event("interaction_callback_pending"));
        assert!(is_input_required_event("policy_callback_pending"));
        assert!(is_input_required_event("turn_input_required"));
        assert!(is_input_required_event("user_input_required"));
        assert!(!is_input_required_event("notification"));
    }

    #[tokio::test]
    async fn after_run_runs_when_after_create_fails_after_directory_creation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspaces");
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "workspace": {"root": root.display().to_string()},
                "hooks": {
                    "after_create": "exit 7",
                    "after_run": "printf done > after_marker",
                    "timeout_ms": 10_000
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(temp.path().join("fugue.yaml"), &definition).unwrap();
        let mut issue = Issue {
            id: "id".to_string(),
            identifier: "ABC-1".to_string(),
            title: "Fix it".to_string(),
            description: None,
            priority: None,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: vec![],
            blocked_by: vec![],
            attachments: vec![],
            created_at: None,
            updated_at: None,
        };
        let (tx, _rx) = mpsc::channel(1);

        let err = run_agent_worker_inner(&mut issue, None, &config, &tx, CancellationToken::new())
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("workspace.create_workspace"));
        assert!(message.contains("ExecutionFailed"));
        assert!(message.contains("after_create"));
        assert!(root.join("ABC-1").join("after_marker").exists());
    }
}
