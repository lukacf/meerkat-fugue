use super::*;

#[derive(Default)]
struct AdmissionScript {
    called: AtomicBool,
    result: Mutex<Option<String>>,
    finished: tokio::sync::Notify,
}

#[async_trait]
impl LlmClient for AdmissionScript {
    fn project_replay_messages(
        &self,
        messages: &[Message],
    ) -> std::result::Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
        let result = request.messages.iter().find_map(|message| match message {
            Message::ToolResults { results, .. } => results
                .iter()
                .find(|result| result.tool_use_id == "native-admission-context"),
            _ => None,
        });
        if let Some(result) = result {
            *self.result.lock().unwrap() = Some(result.text_content());
            self.finished.notify_one();
        }
        let mut events = Vec::new();
        let stop = if request
            .tools
            .iter()
            .any(|tool| tool.name == "fugue_admit_independent_review")
            && !self.called.swap(true, Ordering::SeqCst)
        {
            events.push(Ok(LlmEvent::ToolCallComplete {
                id: "native-admission-context".into(),
                name: "fugue_admit_independent_review".into(),
                args: json!({
                    "issue":{"kind":"identifier","identifier":"TEST-1"},
                    "repo":{"owner":"example","name":"app"},
                    "pull_request":7,"head_sha":"a".repeat(40),
                    "reviewers":{"reviewer":["alpha:reviewer"]},
                    "task":"Inspect the immutable candidate.",
                    "idempotency_key":"native-context-probe"
                }),
                meta: None,
            }));
            StopReason::ToolUse
        } else {
            events.push(Ok(LlmEvent::TextDelta {
                delta: "Ready.".into(),
                meta: None,
            }));
            StopReason::EndTurn
        };
        events.push(Ok(LlmEvent::UsageUpdate {
            usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
        }));
        events.push(Ok(LlmEvent::Done {
            outcome: LlmDoneOutcome::success(stop),
        }));
        Box::pin(futures::stream::iter(events))
    }

    fn provider(&self) -> Provider {
        Provider::OpenAI
    }

    async fn health_check(&self) -> std::result::Result<(), LlmError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_review_tool_reaches_host_without_governed_retained_work() {
    const NAME: &str =
        "review_context_tests::native_review_tool_reaches_host_without_governed_retained_work";
    if !isolated(NAME) {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let mut config = fixture_config(temp.path());
    config.repositories.retain(|id, _| id.as_str() == "alpha");
    config.peers.clear();
    let repo = RepoId::try_from("alpha".to_owned()).unwrap();
    let mut queue_config =
        crate::meta_runtime::tests::test_meerkat_config(temp.path().join("queue"));
    queue_config.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
        repository: repo.clone(),
        state_path: config.state_root.join("alpha"),
    });
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let queue = crate::meta_runtime::MetaRuntimeRegistry::new(queue_config)
        .with_independent_review_config(crate::config::IndependentReviewConfig {
            enabled: true,
            ..Default::default()
        })
        .with_review_admission(sender);
    let client = Arc::new(AdmissionScript::default());
    let ensemble = EnsembleRuntime::build_inner(config, Some(client.clone()), Some(queue))
        .await
        .unwrap();
    let runtime = ensemble.repository(&repo).unwrap();
    runtime
        .mob_handle()
        .ensure_member(
            meerkat_mob::SpawnMemberSpec::new(
                "review_orchestrator",
                "issue_TEST-1__review_5Forchestrator",
            )
            .with_runtime_mode(MobRuntimeMode::AutonomousHost)
            .with_context(json!({"fugue":{"worktree_path":temp.path().join("alpha")}}))
            .with_initial_message("Inspect the candidate through native admission.".to_owned()),
        )
        .await
        .unwrap();

    // Exercise the actual model -> native dispatch -> queue hook boundary.
    // Dropping the command deliberately stops before tracker/GitHub access;
    // receiving it proves that the trusted native caller reached the host.
    let received = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            message = receiver.recv() => matches!(
                message,
                Some(crate::orchestrator::InternalMessage::NativeReviewAdmission(_))
            ),
            _ = client.finished.notified() => false,
        }
    })
    .await;
    ensemble.shutdown().await.unwrap();
    assert!(
        matches!(received, Ok(true)),
        "native admission did not reach the host: {:?}",
        client.result.lock().unwrap()
    );
}

#[tokio::test]
async fn native_review_tool_refuses_missing_session_origin() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let session = crate::review_admission::ReviewSession::new(
        meerkat_core::MobMemberBinding {
            mob_id: "repository-one".into(),
            role: "review_orchestrator".into(),
            member: "recorded-coordinator".into(),
        },
        sender,
    );
    let request = serde_json::from_value(json!({
        "issue":{"kind":"identifier","identifier":"TEST-1"},
        "repo":{"owner":"example","name":"app"},
        "pull_request":7,"head_sha":"a".repeat(40),
        "reviewers":{"reviewer":["alpha:reviewer"]},
        "task":"Inspect the immutable candidate.",
        "idempotency_key":"missing-origin-probe"
    }))
    .unwrap();
    let result = session
        .admit(request, &meerkat_core::ToolDispatchContext::default())
        .await;
    assert!(matches!(
        result,
        Err(crate::review_admission::AdmissionFailure::MissingCaller)
    ));
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}
