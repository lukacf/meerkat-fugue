use super::*;

#[test]
fn jql_values_cannot_escape_project_or_status_scope() {
    assert_eq!(
        jql_literal("ABC\" OR project = OTHER"),
        "\"ABC\\\" OR project = OTHER\""
    );
    assert_eq!(jql_literal("In Progress"), "\"In Progress\"");
}

#[test]
fn adf_keeps_workpad_markdown_and_nested_description_text() {
    let markdown = "## Fugue Workpad\n\n- [x] Tested\n- [ ] Review";
    let encoded = JiraBody::Document(AdfDocument::from_markdown(markdown));
    assert_eq!(encoded.markdown(), markdown);
    let body: JiraBody = serde_json::from_str(r#"{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":"See "},{"type":"text","text":"PR","marks":[{"type":"link","attrs":{"href":"https://git.example.com/acme/app/pull/7"}}]}]},{"type":"bulletList","content":[{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"Test this"}]}]}]}]}"#).expect("ADF");
    assert!(
        body.markdown()
            .contains("[PR](https://git.example.com/acme/app/pull/7)")
    );
    assert!(body.markdown().contains("Test this"));
}

#[test]
fn status_categories_map_to_runtime_classification() {
    let status: JiraStatus = serde_json::from_str(
        r#"{"id":"3","name":"In Progress","statusCategory":{"key":"indeterminate"}}"#,
    )
    .expect("status");
    assert_eq!(status.snapshot(&[]).state_type, LinearStateType::Started);
    assert_eq!(
        status.snapshot(&["In Progress".to_string()]).state_type,
        LinearStateType::Completed
    );
}

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode},
    routing::any,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

struct MockResponse {
    method: &'static str,
    path: &'static str,
    status: StatusCode,
    body: Value,
}

fn reply(method: &'static str, path: &'static str, body: Value) -> MockResponse {
    MockResponse {
        method,
        path,
        status: StatusCode::OK,
        body,
    }
}

struct MockJira {
    endpoint: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for MockJira {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mock_jira(responses: Vec<MockResponse>) -> MockJira {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let remaining = Arc::clone(&responses);
    let observed = Arc::clone(&requests);
    let app = Router::new().fallback(any(move |request: Request<Body>| {
        let remaining = Arc::clone(&remaining);
        let observed = Arc::clone(&observed);
        async move {
            let (parts, body) = request.into_parts();
            let response = remaining
                .lock()
                .expect("responses")
                .pop_front()
                .expect("unexpected request");
            assert_eq!(parts.method.as_str(), response.method);
            assert_eq!(parts.uri.path(), response.path);
            assert!(parts.headers.contains_key("authorization"));
            let body = to_bytes(body, 1024 * 1024).await.expect("request body");
            let body = if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).expect("JSON")
            };
            observed
                .lock()
                .expect("requests")
                .push((parts.uri.to_string(), body));
            Response::builder()
                .status(response.status)
                .header("content-type", "application/json")
                .body(Body::from(response.body.to_string()))
                .expect("response")
        }
    }));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    MockJira {
        endpoint,
        requests,
        responses,
        task,
    }
}

fn tracker(endpoint: &str) -> TrackerConfig {
    TrackerConfig {
        kind: "jira".to_string(),
        endpoint: endpoint.to_string(),
        api_key: Some("test-token".to_string()),
        jira: Some(JiraConfig {
            email: Some("test@example.com".to_string()),
            ..Default::default()
        }),
        project_slug: Some("TEST".to_string()),
        intake_states: vec!["To Do".to_string()],
        in_flight_states: vec!["In Progress".to_string()],
        active_states: vec!["To Do".to_string(), "In Progress".to_string()],
        terminal_states: vec!["Done".to_string()],
        attachment_root: None,
        max_attachment_files: 2,
        max_attachment_bytes: 1024,
        max_attachment_total_bytes: 2048,
    }
}

fn issue(status: &str) -> Value {
    json!({"id":"10001","key":"TEST-1","fields":{"summary":"Integration fixture","description":null,"project":{"id":"10","key":"TEST"},"updated":"2026-01-01T00:00:00.000+0000","status":{"id":if status == "Done" { "3" } else { "2" },"name":status,"statusCategory":{"key":if status == "Done" { "done" } else { "indeterminate" }}}}})
}

#[test]
fn opaque_jira_priority_ids_are_not_numeric_ranks() {
    let mut config = tracker("https://jira.example.com");
    let client = JiraClient::from_config(&config).unwrap();
    let mut value = issue("In Progress");
    value["fields"]["priority"] = json!({"id":"10002", "name":"Normal"});
    assert_eq!(
        client
            .normalize(serde_json::from_value(value.clone()).unwrap())
            .priority,
        None
    );
    config
        .jira
        .as_mut()
        .unwrap()
        .priority_ranks
        .insert("Normal".into(), std::num::NonZeroU32::new(3).unwrap());
    let client = JiraClient::from_config(&config).unwrap();
    assert_eq!(
        client
            .normalize(serde_json::from_value(value.clone()).unwrap())
            .priority,
        Some(3)
    );
    value["fields"]["priority"]["id"] = json!("opaque-priority-id");
    assert_eq!(
        client
            .normalize(serde_json::from_value(value).unwrap())
            .priority,
        Some(3)
    );
    assert!(serde_json::from_value::<JiraConfig>(json!({"priority_ranks":{"Normal":0}})).is_err());
}
fn reference() -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier("TEST-1".to_string()),
    }
}

#[tokio::test]
async fn cloud_search_paginates_with_project_and_jql_scope() {
    let mock = mock_jira(vec![
        reply(
            "POST",
            "/rest/api/3/search/jql",
            json!({"issues":[issue("To Do")],"isLast":false,"nextPageToken":"next"}),
        ),
        reply(
            "POST",
            "/rest/api/3/search/jql",
            json!({"issues":[issue("In Progress")],"isLast":true}),
        ),
    ])
    .await;
    let mut config = tracker(&mock.endpoint);
    config.jira.as_mut().expect("jira").jql = Some("labels = automation".to_string());
    let client = JiraClient::from_config(&config).expect("client");
    assert_eq!(
        client.fetch_candidate_issues().await.expect("issues").len(),
        2
    );
    let requests = mock.requests.lock().expect("requests");
    assert_eq!(
        requests[0].1["jql"],
        "project = \"TEST\" AND (labels = automation) AND (status in (\"To Do\",\"In Progress\")) ORDER BY created ASC, key ASC"
    );
    assert!(requests[0].1.get("startAt").is_none());
    assert_eq!(requests[1].1["nextPageToken"], "next");
    assert!(mock.responses.lock().expect("responses").is_empty());
}

#[tokio::test]
async fn server_search_uses_offset_pagination_and_context_path() {
    let mock = mock_jira(vec![
        reply(
            "POST",
            "/jira/rest/api/2/search",
            json!({"issues":[issue("To Do")],"total":2}),
        ),
        reply(
            "POST",
            "/jira/rest/api/2/search",
            json!({"issues":[issue("In Progress")],"total":2}),
        ),
    ])
    .await;
    let mut config = tracker(&format!("{}/jira", mock.endpoint));
    let jira = config.jira.as_mut().expect("jira");
    jira.api_version = JiraApiVersion::V2;
    jira.auth = JiraAuth::Bearer;
    assert_eq!(
        JiraClient::from_config(&config)
            .expect("client")
            .fetch_candidate_issues()
            .await
            .expect("issues")
            .len(),
        2
    );
    let requests = mock.requests.lock().expect("requests");
    assert_eq!(requests[0].1["startAt"], 0);
    assert_eq!(requests[1].1["startAt"], 1);
}

#[tokio::test]
async fn workpad_update_finds_existing_comment_on_later_pages() {
    let mock = mock_jira(vec![
        reply("GET", "/rest/api/3/issue/TEST-1", issue("In Progress")),
        reply(
            "GET",
            "/rest/api/3/issue/TEST-1/comment",
            json!({"comments":[{"id":"1","body":"Other comment"}],"total":2}),
        ),
        reply(
            "GET",
            "/rest/api/3/issue/TEST-1/comment",
            json!({"comments":[{"id":"2","body":"## Fugue Workpad\n\nOld plan"}],"total":2}),
        ),
        reply(
            "PUT",
            "/rest/api/3/issue/TEST-1/comment/2",
            json!({"id":"2"}),
        ),
    ])
    .await;
    let client = JiraClient::from_config(&tracker(&mock.endpoint)).expect("client");
    let output = client
        .upsert_workpad(UpsertWorkpadInput {
            issue: reference(),
            heading: "Fugue Workpad".to_string(),
            body_markdown: "- [x] Verified".to_string(),
            create_if_missing: true,
            idempotency_key: IdempotencyKey("workpad".to_string()),
        })
        .await
        .expect("update");
    assert!(!output.created && output.updated);
    assert_eq!(output.comment_id.0, "2");
    let requests = mock.requests.lock().expect("requests");
    assert!(requests[2].0.contains("startAt=1"));
    let body: JiraBody = serde_json::from_value(requests[3].1["body"].clone()).expect("ADF");
    assert_eq!(body.markdown(), "## Fugue Workpad\n\n- [x] Verified");
}

#[tokio::test]
async fn late_workpad_update_preserves_recorded_deployment() {
    let evidence =
        "### Deployment\n\nIdempotency-Key: `merge-1`\n\n```json\n{\"status\":\"merged\"}\n```";
    let existing = format!("## Fugue Workpad\n\nOld plan\n\n{evidence}\n\n### Other\nOld note");
    let mock = mock_jira(vec![
        reply("GET", "/rest/api/3/issue/TEST-1", issue("Done")),
        reply(
            "GET",
            "/rest/api/3/issue/TEST-1/comment",
            json!({"comments":[{"id":"1","body":existing}],"total":1}),
        ),
        reply(
            "PUT",
            "/rest/api/3/issue/TEST-1/comment/1",
            json!({"id":"1"}),
        ),
    ])
    .await;
    JiraClient::from_config(&tracker(&mock.endpoint))
        .expect("client")
        .upsert_workpad(UpsertWorkpadInput {
            issue: reference(),
            heading: "Fugue Workpad".into(),
            body_markdown: "### Validation\nReviewed and tested.".into(),
            create_if_missing: true,
            idempotency_key: IdempotencyKey("late-update".into()),
        })
        .await
        .expect("update");
    let requests = mock.requests.lock().expect("requests");
    let body: JiraBody = serde_json::from_value(requests[2].1["body"].clone()).expect("ADF");
    assert_eq!(
        body.markdown(),
        format!("## Fugue Workpad\n\n### Validation\nReviewed and tested.\n\n{evidence}")
    );
}

#[tokio::test]
async fn workpad_retries_are_noops_and_duplicate_workpads_fail_closed() {
    for duplicate in [false, true] {
        let mut comments = vec![json!({"id":"1","body":"## Fugue Workpad\n\nSame plan"})];
        if duplicate {
            comments.push(json!({"id":"2","body":"## TEST-1 Workpad\n\nOther plan"}));
        }
        let mock = mock_jira(vec![
            reply("GET", "/rest/api/3/issue/TEST-1", issue("In Progress")),
            reply(
                "GET",
                "/rest/api/3/issue/TEST-1/comment",
                json!({"total":comments.len(),"comments":comments}),
            ),
        ])
        .await;
        let result = JiraClient::from_config(&tracker(&mock.endpoint))
            .expect("client")
            .upsert_workpad(UpsertWorkpadInput {
                issue: reference(),
                heading: "Fugue Workpad".to_string(),
                body_markdown: "Same plan".to_string(),
                create_if_missing: true,
                idempotency_key: IdempotencyKey("retry".to_string()),
            })
            .await;
        if duplicate {
            assert!(result.is_err());
        } else {
            let output = result.expect("noop");
            assert!(!output.created && !output.updated);
        }
        assert_eq!(mock.requests.lock().expect("requests").len(), 2);
    }
}

#[tokio::test]
async fn transitions_use_destination_status_and_verify_observed_result() {
    let mock = mock_jira(vec![
        reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")),
        reply("GET", "/rest/api/3/issue/TEST-1/transitions", json!({"transitions":[{"id":"31","name":"Start work","to":{"id":"2","name":"In Progress","statusCategory":{"key":"indeterminate"}}}]})),
        reply("POST", "/rest/api/3/issue/TEST-1/transitions", json!({})),
        reply("GET", "/rest/api/3/issue/TEST-1", issue("In Progress")),
    ]).await;
    let output = JiraClient::from_config(&tracker(&mock.endpoint))
        .expect("client")
        .move_issue(MoveIssueInput {
            issue: reference(),
            state: LinearStateRef::Name {
                name: "In Progress".to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("move".to_string()),
        })
        .await
        .expect("transition");
    assert!(output.changed);
    assert_eq!(output.issue.state.name, "In Progress");
    assert_eq!(
        mock.requests.lock().expect("requests")[2].1,
        json!({"transition":{"id":"31"}})
    );
}

#[tokio::test]
async fn terminal_reopen_requires_explicit_operator_authority() {
    for operator in [false, true] {
        let mut responses = vec![
            reply("GET", "/rest/api/3/issue/TEST-1", issue("Done")),
            reply(
                "GET",
                "/rest/api/3/issue/TEST-1/transitions",
                json!({"transitions":[{"id":"4","name":"Request merge","to":{"id":"2","name":"Merge","statusCategory":{"key":"indeterminate"}}}]}),
            ),
        ];
        if operator {
            responses.extend([
                reply("POST", "/rest/api/3/issue/TEST-1/transitions", json!({})),
                reply("GET", "/rest/api/3/issue/TEST-1", issue("Merge")),
            ]);
        }
        let mock = mock_jira(responses).await;
        let adapter = crate::linear_adapter::IssueAdapter::from_config(&tracker(&mock.endpoint))
            .expect("adapter");
        let input = MoveIssueInput {
            issue: reference(),
            state: LinearStateRef::Name {
                name: "Merge".into(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("operator-merge".into()),
        };
        if operator {
            let moved = adapter
                .move_issue_as_operator(input)
                .await
                .expect("operator move");
            assert_eq!(moved.issue.state.name, "Merge");
            assert!(moved.changed);
        } else {
            let error = adapter
                .move_issue(input)
                .await
                .expect_err("agent reopen refused");
            assert!(error.to_string().contains("explicit operator restart"));
        }
        assert!(mock.responses.lock().expect("responses").is_empty());
    }
}

#[tokio::test]
async fn stale_or_out_of_project_issues_never_mutate() {
    for outside in [false, true] {
        let mut data = issue("To Do");
        if outside {
            data["fields"]["project"]["key"] = json!("OTHER");
        }
        let mock = mock_jira(vec![reply("GET", "/rest/api/3/issue/TEST-1", data)]).await;
        let result = JiraClient::from_config(&tracker(&mock.endpoint))
            .expect("client")
            .move_issue(MoveIssueInput {
                issue: reference(),
                state: LinearStateRef::Name {
                    name: "In Progress".to_string(),
                },
                expected_updated_at: Some(Utc::now()),
                idempotency_key: IdempotencyKey("stale".to_string()),
            })
            .await;
        assert!(result.is_err());
        assert_eq!(mock.requests.lock().expect("requests").len(), 1);
    }
}

#[tokio::test]
async fn authentication_errors_do_not_echo_response_bodies_or_credentials() {
    let mock = mock_jira(vec![MockResponse {
        method: "GET",
        path: "/rest/api/3/issue/TEST-1",
        status: StatusCode::UNAUTHORIZED,
        body: json!({"error":"private response test-token"}),
    }])
    .await;
    let (failed, content) = execute_tool(
        &tracker(&mock.endpoint),
        FugueToolOperation::LinearInspectIssue,
        &json!({"issue":reference()}),
    )
    .await;
    assert!(failed);
    assert!(content.contains("authentication_failed"));
    assert!(!content.contains("private response") && !content.contains("test-token"));
}

#[test]
fn jira_config_requires_site_project_and_correct_authentication() {
    let mut config = tracker("https://jira.example.com");
    config.api_key = None;
    assert!(JiraClient::from_config(&config).is_err());
    config.api_key = Some("token".to_string());
    config.project_slug = None;
    assert!(JiraClient::from_config(&config).is_err());
    config.project_slug = Some("TEST".to_string());
    config.endpoint = "https://user:pass@jira.example.com".to_string();
    assert!(JiraClient::from_config(&config).is_err());
}

#[test]
fn missing_filter_environment_never_broadens_jira_scope() {
    let mut config = tracker("https://jira.example.com");
    config.jira.as_mut().expect("jira").jql =
        Some("$FUGUE_TEST_MISSING_JIRA_FILTER_9439".to_string());
    assert!(JiraClient::from_config(&config).is_err());
}

#[tokio::test]
async fn only_inward_blocking_links_become_dependencies() {
    let mut data = issue("To Do");
    let linked = json!({"id":"10002","key":"TEST-2","fields":{"status":{"id":"1","name":"To Do","statusCategory":{"key":"new"}}}});
    data["fields"]["issuelinks"] = json!([
        {"type":{"name":"Blocks"},"inwardIssue":linked},
        {"type":{"name":"Relates"},"inwardIssue":linked},
        {"type":{"name":"Blocks"},"outwardIssue":linked}
    ]);
    let mock = mock_jira(vec![reply(
        "POST",
        "/rest/api/3/search/jql",
        json!({"issues":[data],"isLast":true}),
    )])
    .await;
    let issues = JiraClient::from_config(&tracker(&mock.endpoint))
        .expect("client")
        .fetch_candidate_issues()
        .await
        .expect("issues");
    assert_eq!(issues[0].blocked_by.len(), 1);
    assert_eq!(
        issues[0].blocked_by[0].identifier.as_deref(),
        Some("TEST-2")
    );
}

#[tokio::test]
async fn missing_search_cursor_is_a_failure_instead_of_partial_success() {
    let mock = mock_jira(vec![reply(
        "POST",
        "/rest/api/3/search/jql",
        json!({"issues":[issue("To Do")],"isLast":false}),
    )])
    .await;
    assert!(
        JiraClient::from_config(&tracker(&mock.endpoint))
            .expect("client")
            .fetch_candidate_issues()
            .await
            .is_err()
    );
}

#[test]
fn private_create_fields_cannot_override_typed_scope() {
    let file = tempfile::NamedTempFile::new().expect("private fields");
    std::fs::write(file.path(), r#"{"project":{"key":"OTHER"}}"#).expect("write");
    let mut config = tracker("https://jira.example.com");
    config.jira.as_mut().expect("jira").create_fields_file =
        Some(file.path().display().to_string());
    assert!(JiraClient::from_config(&config).is_err());
}

#[tokio::test]
async fn required_transition_fields_are_passed_through_from_private_config() {
    let mock = mock_jira(vec![
        reply("GET", "/rest/api/3/issue/TEST-1", issue("In Progress")),
        reply("GET", "/rest/api/3/issue/TEST-1/transitions", json!({"transitions":[{"id":"5","to":{"id":"3","name":"Done","statusCategory":{"key":"done"}}}]})),
        reply("POST", "/rest/api/3/issue/TEST-1/transitions", json!({})),
        reply("GET", "/rest/api/3/issue/TEST-1", issue("Done")),
    ]).await;
    let fields = tempfile::NamedTempFile::new().expect("fields");
    std::fs::write(fields.path(), r#"{"Done":{"resolution":{"name":"Done"}}}"#).expect("write");
    let mut config = tracker(&mock.endpoint);
    config.jira.as_mut().expect("jira").transition_fields_file =
        Some(fields.path().display().to_string());
    JiraClient::from_config(&config)
        .expect("client")
        .move_issue(MoveIssueInput {
            issue: reference(),
            state: LinearStateRef::Name {
                name: "Done".to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("resolve".to_string()),
        })
        .await
        .expect("resolve");
    assert_eq!(
        mock.requests.lock().expect("requests")[2].1,
        json!({"transition":{"id":"5"},"fields":{"resolution":{"name":"Done"}}})
    );
}

#[tokio::test]
async fn creation_passes_required_custom_fields_without_changing_project() {
    let mock = mock_jira(vec![
        reply("POST", "/rest/api/3/issue", json!({"id":"10001"})),
        reply("GET", "/rest/api/3/issue/10001", issue("To Do")),
        reply("GET", "/rest/api/3/issue/10001", issue("To Do")),
    ])
    .await;
    let fields = tempfile::NamedTempFile::new().expect("fields");
    std::fs::write(
        fields.path(),
        r#"{"customfield_12345":{"id":"7"},"labels":["fugue"]}"#,
    )
    .expect("write");
    let mut config = tracker(&mock.endpoint);
    config.jira.as_mut().expect("jira").create_fields_file =
        Some(fields.path().display().to_string());
    let result = JiraClient::from_config(&config)
        .expect("client")
        .create_issue("Example", Some("Private field fixture"), "To Do")
        .await
        .expect("create");
    assert_eq!(result.identifier, "TEST-1");
    let requests = mock.requests.lock().expect("requests");
    assert_eq!(requests[0].1["fields"]["project"], json!({"key":"TEST"}));
    assert_eq!(
        requests[0].1["fields"]["customfield_12345"],
        json!({"id":"7"})
    );
    assert_eq!(requests[0].1["fields"]["labels"], json!(["fugue"]));
}
