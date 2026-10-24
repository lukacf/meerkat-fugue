use super::*;

const TASK: &str = "FUGUE-CONTINUE-AFTER-PROCESS-LOSS-41";
const COMPLETION: &str = "FUGUE-CONTINUE-COMPLETED-41";

enum RestartPhase {
    ExitAfterToolRound(PathBuf),
    Resume,
}

struct RestartScript {
    phase: RestartPhase,
    resumed: AtomicBool,
}

#[async_trait]
impl LlmClient for RestartScript {
    fn project_replay_messages(
        &self,
        messages: &[Message],
    ) -> std::result::Result<Vec<Message>, LlmError> {
        Ok(messages.to_vec())
    }

    fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
        let original_inputs = request
            .messages
            .iter()
            .filter(|message| {
                matches!(message, Message::User(user)
                if serde_json::to_string(&user.content).unwrap().contains(TASK))
            })
            .count();
        let mut events = Vec::new();
        let mut stop = StopReason::EndTurn;
        let mut answer = "Ready.";
        if original_inputs > 0 {
            assert_eq!(
                original_inputs, 1,
                "recovery must not repeat the original input"
            );
            let results: Vec<_> = request
                .messages
                .iter()
                .flat_map(|message| match message {
                    Message::ToolResults { results, .. } => results.as_slice(),
                    _ => &[],
                })
                .filter(|result| result.tool_use_id == "before-process-loss")
                .collect();
            match &self.phase {
                RestartPhase::ExitAfterToolRound(marker) if !results.is_empty() => {
                    assert_eq!(results.len(), 1);
                    assert!(!results[0].is_error, "real peers tool must succeed");
                    std::fs::write(
                        marker,
                        "second model request reached after real tool result",
                    )
                    .unwrap();
                    // Lose the process during the turn, without destructor or
                    // shutdown. Incremental session persistence is enabled by
                    // the real repository runtime.
                    std::process::exit(0);
                }
                RestartPhase::ExitAfterToolRound(_) => {
                    stop = StopReason::ToolUse;
                    events.push(Ok(LlmEvent::ToolCallComplete {
                        id: "before-process-loss".into(),
                        name: "peers".into(),
                        args: json!({}),
                        meta: None,
                    }));
                }
                RestartPhase::Resume => {
                    assert_eq!(
                        results.len(),
                        1,
                        "the completed tool round must survive process loss"
                    );
                    assert!(!results[0].is_error);
                    self.resumed.store(true, Ordering::SeqCst);
                    answer = COMPLETION;
                }
            }
        }
        if events.is_empty() {
            events.push(Ok(LlmEvent::TextDelta {
                delta: answer.into(),
                meta: None,
            }));
        }
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
async fn interrupted_repository_turn_continues_after_process_loss_without_new_input() {
    let name = "cold_restart_tests::interrupted_repository_turn_continues_after_process_loss_without_new_input";
    let repo = RepoId::try_from("alpha".to_owned()).unwrap();
    let identity = member_identity(&repo, REPRESENTATIVE).unwrap();
    if let Ok(path) = std::env::var("FUGUE_CONTINUE_CRASH_CONFIG") {
        let config: EnsembleConfig =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let runtime = EnsembleRuntime::build_with_client(
            config,
            Arc::new(RestartScript {
                phase: RestartPhase::ExitAfterToolRound(PathBuf::from(format!("{path}.reached"))),
                resumed: AtomicBool::new(false),
            }),
        )
        .await
        .unwrap();
        let authority = runtime
            .repository(&repo)
            .unwrap()
            .identity_runtime()
            .unwrap();
        let session = authority
            .status(&identity)
            .await
            .unwrap()
            .session_id
            .unwrap();
        std::fs::write(format!("{path}.session"), session.to_string()).unwrap();
        runtime.ask(&repo, TASK).await.unwrap();
        tokio::time::sleep(Duration::from_secs(20)).await;
        panic!("child never reached the second model request");
    }
    if !isolated(name) {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let mut config = fixture_config(temp.path());
    config.repositories.retain(|id, _| id == &repo);
    config.peers.clear();
    let path = temp.path().join("continue.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("ensemble::runtime::tests::{name}"),
            "--nocapture",
        ])
        .env("FUGUE_CONTINUE_CRASH_CONFIG", &path)
        .env("RUST_MIN_STACK", "16777216")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("crash child exceeded its bounded setup time");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let child = child.wait_with_output().unwrap();
    assert!(
        child.status.success(),
        "crash child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        path.with_extension("json.reached").is_file(),
        "the child must reach the intended failure point"
    );
    let before_session = std::fs::read_to_string(path.with_extension("json.session")).unwrap();
    let client = Arc::new(RestartScript {
        phase: RestartPhase::Resume,
        resumed: AtomicBool::new(false),
    });
    let restored = tokio::time::timeout(
        Duration::from_secs(45),
        EnsembleRuntime::build_with_client(config, client.clone()),
    )
    .await
    .expect("cold runtime construction must terminate")
    .unwrap();
    // There is deliberately no new send/ask after reopening the repository.
    let continued = tokio::time::timeout(Duration::from_secs(15), async {
        while !client.resumed.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let runtime = restored.repository(&repo).unwrap();
    let authority = runtime.identity_runtime().unwrap();
    let after_session = tokio::time::timeout(Duration::from_secs(10), authority.status(&identity))
        .await
        .expect("restored identity status must remain readable")
        .unwrap()
        .session_id
        .unwrap();
    assert_eq!(
        before_session,
        after_session.to_string(),
        "recovery must keep the same session"
    );
    if continued.is_ok() {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let page = runtime
                    .mob_runtime()
                    .read_session_history(&after_session.to_string(), 0, None)
                    .await
                    .unwrap();
                if serde_json::to_string(&page.messages)
                    .unwrap()
                    .contains(COMPLETION)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("continued answer must be committed to the same session history");
    }
    tokio::time::timeout(Duration::from_secs(45), restored.shutdown())
        .await
        .expect("recovered runtime shutdown must terminate")
        .unwrap();
    continued.expect("interrupted repository turn must continue without another operator message");
}
