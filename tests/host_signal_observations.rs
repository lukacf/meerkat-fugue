use chrono::{DateTime, Utc};
use meerkat_fugue::config::{ServiceConfig, WorkflowDefinition};
use meerkat_fugue::domain::Issue;
use meerkat_fugue::fugue_ims::{CreateFugueIssue, FugueImsStore};
use meerkat_fugue::host_signal_journal::HostSignalBinding;
use meerkat_fugue::ims::{ImsIssueKey, ImsIssueRef, ImsStageName, IssueManagementSystem};
use meerkat_fugue::kapellmeister::{KapellmeisterShadowSignalKind, ShadowSignalId};
use meerkat_fugue::linear_adapter::{
    IssueAdapter, IssueLaneRuntimeObservation, reconcile_issue_lane_observation_state,
};
use meerkat_fugue::tracker::IssueTracker;
use serde_json::json;

fn fixture() -> (tempfile::TempDir, ServiceConfig, Issue) {
    let temp = tempfile::tempdir().unwrap();
    let definition = WorkflowDefinition {
        config: json!({"tracker": {
            "kind": "fugue-ims", "endpoint": temp.path().join("issues.sqlite3"),
            "project_slug": "example/app", "intake_states": ["Queued"],
            "active_states": ["Queued", "Active"], "terminal_states": ["Done"]
        }})
        .as_object()
        .unwrap()
        .clone(),
        prompt_template: "Follow the issue.".into(),
    };
    let config =
        ServiceConfig::from_definition(temp.path().join("fugue.yaml"), &definition).unwrap();
    let issue = Issue {
        id: "issue-1".into(),
        identifier: "APP-1".into(),
        title: "Durable observation".into(),
        description: None,
        priority: None,
        state: "Queued".into(),
        branch_name: None,
        url: None,
        labels: Vec::new(),
        blocked_by: Vec::new(),
        attachments: Vec::new(),
        created_at: None,
        updated_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
    };
    (temp, config, issue)
}

fn observe(
    config: &ServiceConfig,
    issue: &Issue,
) -> meerkat_fugue::error::Result<Vec<meerkat_fugue::kapellmeister::KapellmeisterShadowSignal>> {
    reconcile_issue_lane_observation_state(
        config,
        HostSignalBinding::from_config(config)?.open()?.revision()?,
        vec![IssueLaneRuntimeObservation {
            issue: issue.clone(),
            running_mob: None,
        }],
    )
}

#[test]
fn failed_observation_commit_does_not_lose_or_partially_enqueue_the_change() {
    let (_temp, config, mut issue) = fixture();
    observe(&config, &issue).unwrap();
    let binding = HostSignalBinding::from_config(&config).unwrap();
    let before = binding.open().unwrap().pending(100).unwrap();
    assert_eq!(before.len(), 1);
    let connection = rusqlite::Connection::open(&binding.path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_observation BEFORE UPDATE ON metadata
        BEGIN SELECT RAISE(ABORT, 'test observation failure'); END;",
        )
        .unwrap();
    issue.state = "Active".into();
    assert!(observe(&config, &issue).is_err());
    assert_eq!(binding.open().unwrap().pending(100).unwrap(), before);
    connection
        .execute_batch("DROP TRIGGER reject_observation")
        .unwrap();
    drop(connection);

    let observed = observe(&config, &issue).unwrap();
    assert!(observed.iter().any(|signal| matches!(
        signal.kind,
        KapellmeisterShadowSignalKind::LinearLaneChanged { .. }
    )));
    let pending = binding.open().unwrap().pending(100).unwrap();
    assert!(pending.len() > before.len());
    for signal in observed {
        assert!(
            pending
                .iter()
                .any(|delivery| delivery.envelope.content.contains(&signal.id.0))
        );
    }
}

#[test]
fn pending_observation_keeps_its_original_payload_when_policy_changes() {
    let (_temp, mut config, issue) = fixture();
    let signals = observe(&config, &issue).unwrap();
    let binding = HostSignalBinding::from_config(&config).unwrap();
    let before = binding.open().unwrap().pending(100).unwrap();
    assert_eq!(before.len(), 1);
    assert!(before[0].envelope.content.contains(&signals[0].id.0));
    config.linear.lanes.success = "Accepted later".into();
    observe(&config, &issue).unwrap();
    let after = binding.open().unwrap().pending(100).unwrap();
    assert!(
        after.contains(&before[0]),
        "a saved notification is immutable until native acknowledgement"
    );
    assert!(!before[0].envelope.content.contains("Accepted later"));
}

#[test]
fn late_tracker_response_cannot_undo_a_newer_internal_lane_observation() {
    let (_temp, config, mut issue) = fixture();
    observe(&config, &issue).unwrap();
    let binding = HostSignalBinding::from_config(&config).unwrap();
    let fetched_at = binding.open().unwrap().revision().unwrap();
    issue.state = "Active".into();
    // The request is in flight while a host-owned projection records a newer
    // state. The old response must not manufacture external drift.
    meerkat_fugue::linear_adapter::record_internal_lane_for_workflow(
        &config,
        &issue.identifier,
        "Done",
    )
    .unwrap();
    let pending = binding.open().unwrap().pending(100).unwrap();
    let result = reconcile_issue_lane_observation_state(
        &config,
        fetched_at,
        vec![IssueLaneRuntimeObservation {
            issue: issue.clone(),
            running_mob: None,
        }],
    );
    assert!(matches!(
        result,
        Err(meerkat_fugue::error::FugueError::HostObservationChanged)
    ));
    assert_eq!(binding.open().unwrap().pending(100).unwrap(), pending);
    issue.state = "Done".into();
    assert!(observe(&config, &issue).unwrap().is_empty());
}

#[test]
fn routing_scope_survives_auth_rotation_but_refuses_project_runtime_or_query_retargeting() {
    let (_temp, mut config, _issue) = fixture();
    config.tracker.kind = "jira".into();
    config.tracker.endpoint = "https://JIRA.EXAMPLE.COM/site".into();
    config.tracker.project_slug = Some("APP".into());
    config.tracker.api_key = None;
    config.tracker.jira = Some(meerkat_fugue::jira::JiraConfig::default());
    let binding = HostSignalBinding::from_config(&config).unwrap();
    binding.open().unwrap();
    config.tracker.api_key = Some("fixture-secret-must-not-be-persisted".into());
    config.tracker.endpoint = "https://jira.example.com/site/".into();
    assert_eq!(binding, HostSignalBinding::from_config(&config).unwrap());
    let saved = std::fs::read(&binding.path).unwrap();
    assert!(!String::from_utf8_lossy(&saved).contains("fixture-secret"));
    for change in ["project", "runtime", "query"] {
        let mut changed = config.clone();
        match change {
            "project" => changed.tracker.project_slug = Some("OTHER".into()),
            "runtime" => {
                changed.meerkat.state_root = Some(config.workflow_dir.join("different-runtime"))
            }
            "query" => {
                changed.tracker.jira.as_mut().unwrap().jql = Some("labels = selected".into())
            }
            _ => unreachable!(),
        }
        assert!(
            HostSignalBinding::from_config(&changed)
                .unwrap()
                .open()
                .is_err()
        );
        assert_eq!(std::fs::read(&binding.path).unwrap(), saved);
    }
    config.tracker.endpoint = "https://user:private-password@jira.example.com".into();
    let error = HostSignalBinding::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("private-password"));
}

#[test]
fn repeated_lane_changes_have_distinct_durable_occurrence_ids() {
    let temp = tempfile::tempdir().unwrap();
    let definition = WorkflowDefinition {
        config: json!({
            "tracker": {
                "kind": "fugue-ims",
                "endpoint": temp.path().join("issues.sqlite3"),
                "project_slug": "example/app",
                "intake_states": ["Queued"],
                "active_states": ["Queued", "Active"],
                "terminal_states": ["Done"]
            }
        })
        .as_object()
        .unwrap()
        .clone(),
        prompt_template: "Follow the issue.".into(),
    };
    let workflow = temp.path().join("fugue.yaml");
    let at: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
    let mut forward_ids: Vec<ShadowSignalId> = Vec::new();
    for state in ["Queued", "Active", "Queued", "Active"] {
        // Reconstruct the adapter config and read the saved observations on
        // every poll. Even a tracker with coarse timestamps must distinguish
        // two occurrences of the same transition across a service restart.
        let config = ServiceConfig::from_definition(workflow.clone(), &definition).unwrap();
        let issue = Issue {
            id: "issue-1".into(),
            identifier: "APP-1".into(),
            title: "Repeated observation".into(),
            description: None,
            priority: None,
            state: state.into(),
            branch_name: None,
            url: None,
            labels: Vec::new(),
            blocked_by: Vec::new(),
            attachments: Vec::new(),
            created_at: Some(at),
            updated_at: Some(at),
        };
        let signals = reconcile_issue_lane_observation_state(
            &config,
            HostSignalBinding::from_config(&config)
                .unwrap()
                .open()
                .unwrap()
                .revision()
                .unwrap(),
            vec![IssueLaneRuntimeObservation {
                issue,
                running_mob: None,
            }],
        )
        .unwrap();
        if state == "Active" {
            let signal = signals
                .into_iter()
                .find(|signal| {
                    matches!(
                        signal.kind,
                        KapellmeisterShadowSignalKind::LinearLaneChanged { .. }
                    )
                })
                .expect("each real lane transition must be observed");
            forward_ids.push(signal.id);
        }
    }
    assert_eq!(forward_ids.len(), 2);
    assert_ne!(
        forward_ids[0], forward_ids[1],
        "a content-derived ID lets the coordinator deduper discard the later transition"
    );
}

#[tokio::test]
async fn repeated_comment_edits_have_distinct_durable_occurrence_ids() {
    let temp = tempfile::tempdir().unwrap();
    let definition = WorkflowDefinition {
        config: json!({
            "tracker": {
                "kind": "fugue-ims",
                "endpoint": temp.path().join("issues.sqlite3"),
                "project_slug": "example/app",
                "intake_states": ["Queued"],
                "active_states": ["Queued", "Active"],
                "terminal_states": ["Done"]
            }
        })
        .as_object()
        .unwrap()
        .clone(),
        prompt_template: "Follow the issue.".into(),
    };
    let config =
        ServiceConfig::from_definition(temp.path().join("fugue.yaml"), &definition).unwrap();
    let store = FugueImsStore::from_config(&config.tracker).unwrap();
    let created = store
        .create_issue(CreateFugueIssue {
            key: ImsIssueKey("APP-1".into()),
            repo: "example/app".into(),
            title: "Repeated comment edits".into(),
            description: None,
            stage: ImsStageName("Queued".into()),
            labels: Vec::new(),
        })
        .unwrap();
    let issue_ref = ImsIssueRef {
        key: ImsIssueKey("APP-1".into()),
    };
    let comment = store.add_comment(&issue_ref, "Original").await.unwrap();
    let mut issues = store
        .fetch_issue_states_by_ids(&[created.id.0])
        .await
        .unwrap();
    assert_eq!(issues.len(), 1);
    // Jira and other trackers may leave the issue timestamp unchanged when a
    // comment is edited. The occurrence must not depend on that timestamp.
    issues[0].updated_at = Some("2026-01-01T00:00:00Z".parse().unwrap());
    let mut changed_ids = Vec::new();
    for body in ["Original", "Revised", "Original", "Revised"] {
        store.update_comment(&comment.id, body).unwrap();
        // Recreate the adapter between polls to exercise persisted observation
        // state, including a quiet baseline for an already existing comment.
        let adapter = IssueAdapter::from_config(&config.tracker).unwrap();
        let signals = adapter
            .reconcile_issue_comment_observation_state(&config, &issues)
            .await
            .unwrap();
        if body == "Revised" {
            assert_eq!(signals.len(), 1);
            changed_ids.push(signals[0].id.clone());
        } else if changed_ids.is_empty() {
            assert!(signals.is_empty(), "the first comment snapshot is quiet");
        }
    }
    assert_eq!(changed_ids.len(), 2);
    assert_ne!(
        changed_ids[0], changed_ids[1],
        "restoring the same comment text is a new operator observation"
    );
}

#[tokio::test]
async fn tracker_tool_process_records_internal_moves_in_the_host_journal() {
    const CHILD: &str = "FUGUE_TEST_HOST_JOURNAL_TOOL_CHILD";
    if let Some(root) = std::env::var_os(CHILD) {
        let definition = WorkflowDefinition {
            config: json!({"tracker": {
                "kind": "fugue-ims", "endpoint": std::path::PathBuf::from(root).join("issues.sqlite3"),
                "project_slug": "example/app", "active_states": ["Queued", "Active"],
                "terminal_states": ["Done"]
            }}).as_object().unwrap().clone(),
            prompt_template: String::new(),
        };
        let config = ServiceConfig::from_definition("fugue.yaml".into(), &definition).unwrap();
        let (failed, _) = meerkat_fugue::linear_tools::execute_linear_fugue_tool(
            &config.tracker,
            "linear_move_issue",
            &json!({"issue": {"kind": "identifier", "identifier": "APP-1"},
                "state": {"kind": "name", "name": "Done"}, "idempotency_key": "test-internal-move"}),
        ).await;
        assert!(!failed, "the internal tracker move should succeed");
        return;
    }
    let (temp, config, _) = fixture();
    let store = FugueImsStore::from_config(&config.tracker).unwrap();
    store
        .create_issue(CreateFugueIssue {
            key: ImsIssueKey("APP-1".into()),
            repo: "example/app".into(),
            title: "Move through the real tool boundary".into(),
            description: None,
            stage: ImsStageName("Queued".into()),
            labels: Vec::new(),
        })
        .unwrap();
    let issue = store.fetch_candidate_issues().await.unwrap().remove(0);
    observe(&config, &issue).unwrap();
    let binding = HostSignalBinding::from_config(&config).unwrap();
    let before = binding.open().unwrap().pending(100).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tracker_tool_process_records_internal_moves_in_the_host_journal",
            "--nocapture",
        ])
        .env(CHILD, temp.path())
        .env(
            "FUGUE_HOST_SIGNAL_BINDING",
            serde_json::to_string(&binding).unwrap(),
        )
        .env_remove("FUGUE_LINEAR_LANE_STATE_PATH")
        .status()
        .unwrap();
    assert!(status.success());
    let mut moved = issue;
    moved.state = "Done".into();
    assert!(
        observe(&config, &moved).unwrap().is_empty(),
        "the tool's own move must not be misreported as an external operator action"
    );
    assert_eq!(binding.open().unwrap().pending(100).unwrap(), before);
}

#[tokio::test]
async fn bound_tracker_tools_refuse_cross_repository_routing_before_mutation() {
    let (_one, config_a, _) = fixture();
    let (_two, config_b, _) = fixture();
    for config in [&config_a, &config_b] {
        let store = FugueImsStore::from_config(&config.tracker).unwrap();
        store
            .create_issue(CreateFugueIssue {
                key: ImsIssueKey("APP-1".into()),
                repo: "example/app".into(),
                title: "The same key in different repositories".into(),
                description: None,
                stage: ImsStageName("Queued".into()),
                labels: Vec::new(),
            })
            .unwrap();
        observe(
            config,
            &store.fetch_candidate_issues().await.unwrap().remove(0),
        )
        .unwrap();
    }
    let binding_a = HostSignalBinding::from_config(&config_a).unwrap();
    let binding_b = HostSignalBinding::from_config(&config_b).unwrap();
    let revision_b = binding_b.open().unwrap().revision().unwrap();
    let pending_a = binding_a.open().unwrap().pending(100).unwrap();
    let input = json!({"issue": {"kind": "identifier", "identifier": "APP-1"},
        "state": {"kind": "name", "name": "Done"}, "idempotency_key": "scoped-move"});
    let (failed, _) = meerkat_fugue::linear_tools::execute_linear_fugue_tool_bound(
        &config_a.tracker,
        "linear_move_issue",
        &input,
        &[],
        Some(&binding_b),
    )
    .await;
    assert!(failed);
    let store_a = FugueImsStore::from_config(&config_a.tracker).unwrap();
    assert_eq!(
        store_a.fetch_candidate_issues().await.unwrap()[0].state,
        "Queued"
    );
    assert_eq!(binding_a.open().unwrap().pending(100).unwrap(), pending_a);
    assert_eq!(binding_b.open().unwrap().revision().unwrap(), revision_b);

    let (failed, _) = meerkat_fugue::linear_tools::execute_linear_fugue_tool_bound(
        &config_a.tracker,
        "linear_move_issue",
        &input,
        &[],
        Some(&binding_a),
    )
    .await;
    assert!(!failed);
    let moved = store_a
        .inspect_issue_tool(meerkat_fugue::tools::InspectIssueInput {
            issue: meerkat_fugue::tools::LinearIssueRef::Identifier {
                identifier: meerkat_fugue::tools::LinearIssueIdentifier("APP-1".into()),
            },
        })
        .await
        .unwrap();
    assert_eq!(moved.issue.state.name, "Done");
    assert_eq!(binding_a.open().unwrap().pending(100).unwrap(), pending_a);
    assert_eq!(binding_b.open().unwrap().revision().unwrap(), revision_b);
}

#[tokio::test]
async fn tracker_mutation_is_not_reported_successful_when_observation_commit_fails() {
    let (_temp, config, _) = fixture();
    let store = FugueImsStore::from_config(&config.tracker).unwrap();
    store
        .create_issue(CreateFugueIssue {
            key: ImsIssueKey("APP-1".into()),
            repo: "example/app".into(),
            title: "Recover a failed observation write".into(),
            description: None,
            stage: ImsStageName("Queued".into()),
            labels: Vec::new(),
        })
        .unwrap();
    let mut issue = store.fetch_candidate_issues().await.unwrap().remove(0);
    observe(&config, &issue).unwrap();
    let binding = HostSignalBinding::from_config(&config).unwrap();
    let before = binding.open().unwrap().pending(100).unwrap();
    let connection = rusqlite::Connection::open(&binding.path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_tool_observation BEFORE UPDATE ON metadata
        BEGIN SELECT RAISE(ABORT, 'test observation failure'); END;",
        )
        .unwrap();
    let input = json!({"issue": {"kind": "identifier", "identifier": "APP-1"},
        "state": {"kind": "name", "name": "Done"}, "idempotency_key": "retry-observation"});
    let (failed, content) = meerkat_fugue::linear_tools::execute_linear_fugue_tool_bound(
        &config.tracker,
        "linear_move_issue",
        &input,
        &[],
        Some(&binding),
    )
    .await;
    assert!(failed);
    let result: meerkat_fugue::tools::FugueToolResult = serde_json::from_str(&content).unwrap();
    match result {
        meerkat_fugue::tools::FugueToolResult::Err { error, .. } => {
            assert!(error.message.contains("Tracker mutation succeeded"));
            assert_eq!(
                error.retry,
                meerkat_fugue::tools::RetryAdvice::RefreshStateThenRetry
            );
            assert_eq!(error.idempotency_key.unwrap().0, "retry-observation");
        }
        _ => panic!("a failed journal commit must be an explicit tool error"),
    }
    assert_eq!(binding.open().unwrap().pending(100).unwrap(), before);
    connection
        .execute_batch("DROP TRIGGER reject_tool_observation")
        .unwrap();
    drop(connection);
    let (failed, _) = meerkat_fugue::linear_tools::execute_linear_fugue_tool_bound(
        &config.tracker,
        "linear_move_issue",
        &input,
        &[],
        Some(&binding),
    )
    .await;
    assert!(!failed);
    issue.state = "Done".into();
    assert!(observe(&config, &issue).unwrap().is_empty());
    assert_eq!(binding.open().unwrap().pending(100).unwrap(), before);
}
