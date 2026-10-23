//! A real native tool-and-comms workflow with a deterministic model boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use meerkat_core::{Message, Provider, StopReason, ToolResult, TurnUsage, Usage};
use meerkat_llm_core::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest, LlmStream};
use serde::Deserialize;
use serde_json::{Value, json};

use super::config::{EnsembleConfig, RepoId};
use super::runtime::EnsembleRuntime;

const ARTIFACT: &str = "ensemble-workflow-proof.txt";
const CONTENT: &str = "Created by the coder, independently read by the reviewer.\n";
const FINAL: &str = "Verified workflow complete: the reviewer read the coder's artifact.";

#[derive(Default)]
struct ScriptState {
    peers: BTreeMap<String, String>,
    visited: BTreeSet<&'static str>,
    asked_planner: bool,
    attempted_forbidden_patch: bool,
    denial_observed: bool,
    sent_plan: bool,
    wrote_artifact: bool,
    asked_reviewer: bool,
    read_artifact: bool,
    review_observed: bool,
    sent_review: bool,
    completed: bool,
}

#[derive(Default)]
struct WorkflowScript {
    state: Mutex<ScriptState>,
}

#[derive(Deserialize)]
struct ReadEvidence {
    content: String,
}

fn tool_result<'a>(request: &'a LlmRequest, id: &str) -> Option<&'a ToolResult> {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::ToolResults { results, .. } => {
                results.iter().find(|result| result.tool_use_id == id)
            }
            _ => None,
        })
}

fn tool_call(id: &str, name: &str, args: Value) -> LlmEvent {
    LlmEvent::ToolCallComplete {
        id: id.into(),
        name: name.into(),
        args,
        meta: None,
    }
}

fn peer_message(state: &ScriptState, id: &str, role: &str, body: &str) -> LlmEvent {
    tool_call(
        id,
        "send_message",
        json!({
            "peer_id": state.peers.get(role).expect("resolved role peer"),
            "body": body,
            "handling_mode": "queue"
        }),
    )
}

#[async_trait]
impl LlmClient for WorkflowScript {
    fn project_replay_messages(&self, messages: &[Message]) -> Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
        let transcript = serde_json::to_string(&request.messages).expect("serialize transcript");
        let role = [
            ("representative", "You are the repository representative."),
            ("planner", "You are the repository planner."),
            ("coder", "You are the repository coder."),
            ("reviewer", "You are the independent repository reviewer."),
        ]
        .into_iter()
        .find_map(|(role, charter)| transcript.contains(charter).then_some(role))
        .expect("repository role charter");
        assert!(
            transcript
                .contains("An explicit request to merge or deploy locally grants authorization"),
            "{role} did not receive the current-task authorization contract"
        );
        assert!(
            transcript.contains("Review the result against the original objective"),
            "{role} did not receive the objective completion contract"
        );
        let catalog: BTreeSet<&str> = request
            .tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect();
        for required in ["repo_read_file", "repo_list_files", "send_message"] {
            assert!(catalog.contains(required), "{role} lacks {required}");
        }
        assert!(
            catalog.iter().any(|name| name.starts_with("workgraph_")),
            "{role} lacks native WorkGraph tools"
        );
        for mutation in ["shell", "apply_patch"] {
            assert_eq!(
                catalog.contains(mutation),
                role == "coder",
                "incorrect {mutation} authority for {role}"
            );
        }
        let mut state = self.state.lock().expect("script state");
        state.visited.insert(role);
        let next = match role {
            "representative"
                if transcript.contains("WORKFLOW_OBJECTIVE") && !state.asked_planner =>
            {
                state.asked_planner = true;
                Some(peer_message(
                    &state,
                    "request-plan",
                    "planner",
                    "WORKFLOW_PLAN_REQUEST: plan a proof artifact and hand implementation to coder.",
                ))
            }
            "planner"
                if transcript.contains("WORKFLOW_PLAN_REQUEST")
                    && !state.attempted_forbidden_patch =>
            {
                state.attempted_forbidden_patch = true;
                Some(tool_call(
                    "forbidden-planner-patch",
                    "apply_patch",
                    json!({"patch":"*** Begin Patch\n*** Add File: unauthorized.txt\n+must not exist\n*** End Patch"}),
                ))
            }
            "planner" if !state.sent_plan => {
                let denied = tool_result(request, "forbidden-planner-patch")
                    .expect("planner must receive a rejected mutation result");
                assert!(denied.is_error, "read-only planner was allowed to patch");
                state.denial_observed = true;
                state.sent_plan = true;
                Some(peer_message(
                    &state,
                    "handoff-plan",
                    "coder",
                    "WORKFLOW_IMPLEMENT: create ensemble-workflow-proof.txt, then ask reviewer to read it.",
                ))
            }
            "coder" if transcript.contains("WORKFLOW_IMPLEMENT") && !state.wrote_artifact => {
                state.wrote_artifact = true;
                Some(tool_call(
                    "coder-patch",
                    "apply_patch",
                    json!({"patch": format!("*** Begin Patch\n*** Add File: {ARTIFACT}\n+{}\n*** End Patch", CONTENT.trim_end())}),
                ))
            }
            "coder" if !state.asked_reviewer => {
                let patch = tool_result(request, "coder-patch").expect("coder patch result");
                assert!(
                    !patch.is_error,
                    "coder patch failed: {}",
                    patch.text_content()
                );
                state.asked_reviewer = true;
                Some(peer_message(
                    &state,
                    "request-review",
                    "reviewer",
                    "WORKFLOW_REVIEW: independently read ensemble-workflow-proof.txt and report evidence to representative.",
                ))
            }
            "reviewer" if transcript.contains("WORKFLOW_REVIEW") && !state.read_artifact => {
                state.read_artifact = true;
                Some(tool_call(
                    "reviewer-read",
                    "repo_read_file",
                    json!({"path": ARTIFACT}),
                ))
            }
            "reviewer" if !state.sent_review => {
                let read = tool_result(request, "reviewer-read").expect("reviewer read result");
                assert!(
                    !read.is_error,
                    "reviewer read failed: {}",
                    read.text_content()
                );
                let evidence: ReadEvidence =
                    serde_json::from_str(&read.text_content()).expect("typed file evidence");
                assert_eq!(evidence.content, CONTENT);
                state.review_observed = true;
                state.sent_review = true;
                Some(peer_message(
                    &state,
                    "verified-review",
                    "representative",
                    "WORKFLOW_VERIFIED: independently read the expected exact artifact contents.",
                ))
            }
            _ => None,
        };
        let mut events = Vec::new();
        let stop_reason = if let Some(call) = next {
            events.push(call);
            StopReason::ToolUse
        } else {
            let text = if role == "representative" && transcript.contains("WORKFLOW_VERIFIED") {
                assert!(state.review_observed);
                state.completed = true;
                FINAL
            } else {
                "Awaiting the next concrete peer message."
            };
            events.push(LlmEvent::TextDelta {
                delta: text.into(),
                meta: None,
            });
            StopReason::EndTurn
        };
        events.push(LlmEvent::UsageUpdate {
            usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
        });
        events.push(LlmEvent::Done {
            outcome: LlmDoneOutcome::success(stop_reason),
        });
        Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> Result<(), LlmError> {
        Ok(())
    }
}

fn isolated() -> bool {
    const TEST: &str =
        "ensemble::workflow_tests::native_team_plans_edits_and_independently_reviews";
    if std::env::var("FUGUE_ENSEMBLE_WORKFLOW_TEST").as_deref() == Ok(TEST) {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .env("FUGUE_ENSEMBLE_WORKFLOW_TEST", TEST)
        .env("RUST_MIN_STACK", "16777216")
        .args([TEST, "--exact", "--nocapture"])
        .output()
        .expect("start isolated native workflow");
    assert!(
        output.status.success(),
        "native workflow failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_team_plans_edits_and_independently_reviews() {
    if !isolated() {
        return;
    }
    let temp = tempfile::tempdir().expect("temp fixture");
    let checkout = temp.path().join("product");
    std::fs::create_dir(&checkout).expect("checkout");
    assert!(
        std::process::Command::new("git")
            .args(["init", "-q"])
            .arg(&checkout)
            .status()
            .expect("git init")
            .success()
    );
    std::fs::write(checkout.join("README.md"), "# Product\nWorkflow test.\n").expect("README");
    let mut config =
        EnsembleConfig::for_repositories(std::slice::from_ref(&checkout), "Workflow".into())
            .expect("config");
    config.state_root = temp.path().join("state");
    super::runtime::write_fixture_teams(&config);
    let script = Arc::new(WorkflowScript::default());
    let ensemble = EnsembleRuntime::build_with_client(config, script.clone())
        .await
        .expect("runtime");
    let repo = RepoId::try_from("product".to_string()).expect("id");
    let native = ensemble.repository(&repo).expect("native");
    let status = ensemble.status().await.expect("status");
    for member in &status.repositories[0].members {
        let alias = member.runtime_alias.as_ref().expect("materialized alias");
        let (peer, _, _) = native
            .local_member_peer_info(alias)
            .await
            .expect("peer identity");
        script
            .state
            .lock()
            .expect("script state")
            .peers
            .insert(member.member_id.as_str().into(), peer);
    }
    assert!(
        script
            .state
            .lock()
            .expect("script state")
            .visited
            .is_empty(),
        "startup must stay idle"
    );
    ensemble
        .ask(
            &repo,
            "WORKFLOW_OBJECTIVE: have your team produce and independently review a proof artifact.",
        )
        .await
        .expect("objective accepted");
    let identity = meerkat_meta::identity_first::AgentIdentity::parse("product:representative")
        .expect("identity");
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let inspection = native
                .identity_runtime()
                .expect("authority")
                .inspect(&identity)
                .await
                .expect("inspect");
            if inspection
                .output_preview
                .as_deref()
                .is_some_and(|text| text.contains(FINAL))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("native workflow completes with independently reviewed evidence");
    assert_eq!(
        std::fs::read_to_string(checkout.join(ARTIFACT)).expect("artifact"),
        CONTENT
    );
    assert!(
        !checkout.join("unauthorized.txt").exists(),
        "planner mutation escaped its tool boundary"
    );
    {
        let state = script.state.lock().expect("script state");
        assert_eq!(
            state.visited,
            BTreeSet::from(["representative", "planner", "coder", "reviewer"])
        );
        assert!(state.denial_observed && state.review_observed && state.completed);
    }
    ensemble.shutdown().await.expect("clean shutdown");
}
