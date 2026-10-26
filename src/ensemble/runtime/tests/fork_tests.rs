use super::*;

const PARENT: &str = "PARENT-FORK-CHECK-83";
const CHILD_TASK: &str = "TASK-FORK-QUARTZ-23";
const CHILD_REPLY: &str = "RESULT-FORK-OPAL-19";
const WORKTREE_TOKEN: &str = "FUGUE_FORK_WORKTREE_83";

#[derive(serde::Deserialize)]
struct ForkAdmission {
    job_id: String,
    agent_identity: String,
    status: String,
}

#[derive(Default)]
struct ForkScript {
    parent_returned: AtomicBool,
    child_verified: AtomicBool,
    release_child: AtomicBool,
    completion_delivered: AtomicBool,
    failure: Mutex<Option<String>>,
}

#[async_trait]
impl LlmClient for ForkScript {
    fn project_replay_messages(
        &self,
        messages: &[Message],
    ) -> std::result::Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
        let last_user = request
            .messages
            .iter()
            .rev()
            .find_map(|message| {
                let Message::User(user) = message else {
                    return None;
                };
                Some(serde_json::to_string(&user.content).unwrap())
            })
            .unwrap_or_default();
        let results: Vec<_> = request
            .messages
            .iter()
            .flat_map(|message| match message {
                Message::ToolResults { results, .. } => results.as_slice(),
                _ => &[],
            })
            .collect();
        let result = |id: &str| {
            results
                .iter()
                .rev()
                .find(|result| result.tool_use_id == id)
                .copied()
        };
        let mut call = None;
        let mut answer = "Ready.";
        let mut await_context_probe = false;
        if let Some(error) = results.iter().find(|result| result.is_error) {
            *self.failure.lock().unwrap() = Some(error.text_content());
            answer = "Tool failed.";
        } else if last_user.contains(CHILD_TASK) {
            assert!(
                !serde_json::to_string(&request.messages)
                    .unwrap()
                    .contains(PARENT),
                "message_count zero must not inherit the parent's conclusions"
            );
            if let Some(read) = result("child-worktree") {
                assert!(
                    read.text_content().contains(WORKTREE_TOKEN),
                    "forked reviewer must read its inherited issue workspace: {}",
                    read.text_content()
                );
                self.child_verified.store(true, Ordering::SeqCst);
                await_context_probe = true;
                answer = CHILD_REPLY;
            } else {
                call = Some((
                    "child-worktree",
                    "shell",
                    json!({"command":"cat fork-proof.txt"}),
                ));
            }
        } else if last_user.contains(PARENT) {
            if let Some(fork_result) = result("parent-fork") {
                let admission: ForkAdmission =
                    serde_json::from_str(&fork_result.text_content()).unwrap();
                assert_eq!(admission.agent_identity, "neutral-review-helper");
                assert_eq!(admission.status, "running");
                self.parent_returned.store(true, Ordering::SeqCst);
                for message in &request.messages {
                    let Message::SystemNotice(notice) = message else {
                        continue;
                    };
                    if notice.kind != meerkat_core::types::SystemNoticeKind::BackgroundJob {
                        continue;
                    }
                    for block in &notice.blocks {
                        if let meerkat_core::types::SystemNoticeBlock::BackgroundJob {
                            job_id,
                            status,
                            detail: Some(detail),
                            persisted: true,
                            ..
                        } = block
                            && job_id == &admission.job_id
                        {
                            assert_eq!(
                                *status,
                                meerkat_core::event::BackgroundJobTerminalStatus::Completed
                            );
                            assert!(detail.contains(CHILD_REPLY));
                            self.completion_delivered.store(true, Ordering::SeqCst);
                        }
                    }
                }
                answer = "Fork admitted; await its native completion.";
            } else if result("parent-round").is_some() {
                assert!(
                    request.tools.iter().any(|tool| tool.name == "fork_off"),
                    "the actual ticket reviewer must expose native fork_off"
                );
                call = Some((
                    "parent-fork",
                    "fork_off",
                    json!({
                        "member_id":"neutral-review-helper", "message_count":0,
                        "task":CHILD_TASK, "max_run_secs":30
                    }),
                ));
            } else {
                // Finish a real tool round before forking from the next one.
                call = Some(("parent-round", "peers", json!({})));
            }
        }
        let stop = if call.is_some() {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        };
        let event = if let Some((id, name, args)) = call {
            LlmEvent::ToolCallComplete {
                id: id.into(),
                name: name.into(),
                args,
                meta: None,
            }
        } else {
            LlmEvent::TextDelta {
                delta: answer.into(),
                meta: None,
            }
        };
        let events = vec![
            Ok(event),
            Ok(LlmEvent::UsageUpdate {
                usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
            }),
            Ok(LlmEvent::Done {
                outcome: LlmDoneOutcome::success(stop),
            }),
        ];
        Box::pin(async_stream::stream! {
            // Keep the real child alive after its tool round so the test can
            // inspect its committed native build context before retirement.
            if await_context_probe {
                while !self.release_child.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            for event in events {
                yield event;
            }
        })
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> std::result::Result<(), LlmError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_ticket_reviewer_forks_after_its_own_tool_round() {
    if !isolated("fork_tests::native_ticket_reviewer_forks_after_its_own_tool_round") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let mut config = fixture_config(temp.path());
    config.repositories.retain(|id, _| id.as_str() == "alpha");
    config.peers.clear();
    let repo = RepoId::try_from("alpha".to_owned()).unwrap();
    let worktree = temp.path().join("issue-worktree");
    std::fs::create_dir(&worktree).unwrap();
    std::fs::write(worktree.join("fork-proof.txt"), WORKTREE_TOKEN).unwrap();
    let workflow = temp.path().join("fugue.yaml");
    let definition = crate::config::parse_native_workflow(
        &workflow,
        r#"
version: 1
backend: native-execution
linear:
  kind: fugue-ims
  endpoint: issues.sqlite3
  project_slug: example/app
selection:
  default_template: review_probe
worker:
  prompt_template: "Review {{ issue.identifier }}"
agents:
  ticket_reviewer:
    tools: [shell, mob]
mob_templates:
  review_probe:
    members:
      - name: reviewer
        profile: ticket_reviewer
"#,
    )
    .unwrap();
    let service = crate::config::ServiceConfig::from_definition(workflow, &definition).unwrap();
    let mut queue_config =
        crate::meta_runtime::tests::test_meerkat_config(temp.path().join("queue"));
    queue_config.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
        repository: repo.clone(),
        state_path: config.state_root.join("alpha"),
    });
    let queue = crate::meta_runtime::MetaRuntimeRegistry::with_templates(
        queue_config,
        BTreeMap::new(),
        service.mob.templates,
    );
    let client = Arc::new(ForkScript::default());
    let ensemble = EnsembleRuntime::build_inner(config, Some(client.clone()), Some(queue))
        .await
        .unwrap();
    let runtime = ensemble.repository(&repo).unwrap();
    let handle = runtime.mob_handle();
    let identity = meerkat_mob::AgentIdentity::from("issue_DEMO-1__reviewer");
    let context = json!({
        "fugue": {"worktree_path": worktree},
        "qualification": {"domain": "review-domain", "grant": "review-scope"}
    });
    handle
        .ensure_member(
            meerkat_mob::SpawnMemberSpec::new("ticket_reviewer", identity.clone())
                .with_runtime_mode(MobRuntimeMode::TurnDriven)
                .with_context(context.clone()),
        )
        .await
        .unwrap();
    handle
        .member(&identity)
        .await
        .unwrap()
        .send(PARENT, meerkat_core::HandlingMode::Steer)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !client.child_verified.load(Ordering::SeqCst) {
            assert!(
                client.failure.lock().unwrap().is_none(),
                "the native fork must reach its child tool round"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("native fork must reach the context inspection boundary");
    let service = runtime.mob_runtime().session_service().unwrap();
    for member in [
        identity,
        meerkat_mob::AgentIdentity::from("neutral-review-helper"),
    ] {
        let snapshot = tokio::time::timeout(Duration::from_secs(10), handle.member_status(&member))
            .await
            .unwrap()
            .unwrap();
        let session_id = snapshot.current_session_id.unwrap();
        let session = tokio::time::timeout(
            Duration::from_secs(10),
            service.load_persisted_session_tolerating_tail(&session_id),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(
            session.try_build_state().unwrap().unwrap().app_context,
            Some(context.clone()),
            "{member} must retain the exact application context, not only a working shell"
        );
    }
    client.release_child.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let failure = client.failure.lock().unwrap().clone();
            assert!(failure.is_none(), "native fork failed: {failure:?}");
            if client.parent_returned.load(Ordering::SeqCst)
                && client.child_verified.load(Ordering::SeqCst)
                && client.completion_delivered.load(Ordering::SeqCst)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("native fork result must reach its parent");
    ensemble.shutdown().await.unwrap();
}
