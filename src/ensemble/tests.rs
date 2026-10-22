//! End-to-end local HTTP tests over real persistent native runtimes.

use std::collections::BTreeMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::config::EnsembleConfig;
use super::runtime::EnsembleRuntime;

#[derive(Default)]
struct CountingClient {
    calls: AtomicUsize,
    inner: meerkat_llm_core::TestClient,
}

#[async_trait]
impl meerkat::LlmClient for CountingClient {
    fn project_replay_messages(
        &self,
        messages: &[meerkat_core::Message],
    ) -> Result<Vec<meerkat_core::Message>, meerkat::LlmError> {
        self.inner.project_replay_messages(messages)
    }

    fn stream<'a>(
        &'a self,
        request: &'a meerkat::LlmRequest,
    ) -> Pin<
        Box<dyn futures::Stream<Item = Result<meerkat::LlmEvent, meerkat::LlmError>> + Send + 'a>,
    > {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.stream(request)
    }

    fn provider(&self) -> meerkat_core::Provider {
        meerkat_core::Provider::Other
    }

    async fn health_check(&self) -> Result<(), meerkat::LlmError> {
        Ok(())
    }
}

#[derive(Deserialize)]
struct Overview {
    name: String,
    runtime: RuntimeStatus,
}

#[derive(Deserialize)]
struct RuntimeStatus {
    repositories: Vec<RepositoryStatus>,
    peers: Vec<PeerStatus>,
}

#[derive(Deserialize)]
struct RepositoryStatus {
    repo: String,
    members: Vec<MemberStatus>,
}

#[derive(Deserialize)]
struct MemberStatus {
    member_id: String,
    identity: String,
    session_id: Option<String>,
    state: String,
}

#[derive(Deserialize)]
struct PeerStatus {
    from: String,
    to: String,
}

#[derive(Serialize)]
struct Ask<'a> {
    message: &'a str,
}

#[derive(Deserialize)]
struct Accepted {
    repo: String,
    identity: String,
    session_id: String,
    interaction_id: String,
}

fn git_repository(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    let status = std::process::Command::new("git")
        .args(["init", "--quiet"])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success());
    std::fs::write(
        path.join("README.md"),
        "# Fixture\nA local test repository.\n",
    )
    .unwrap();
}

#[test]
fn dashboard_search_browser_coverage_is_checked_in() {
    let spec = std::fs::read_to_string("tests/ensemble-dashboard-search.spec.mjs")
        .expect("read Ensemble dashboard search Playwright spec");
    let package = std::fs::read_to_string("package.json")
        .expect("read package.json for Ensemble search script");

    for required in [
        "test:ensemble-search",
        "playwright test tests/ensemble-dashboard-search.spec.mjs --project=chromium",
    ] {
        assert!(
            package.contains(required),
            "package.json missing Ensemble search entrypoint {required}"
        );
    }

    for required in [
        "typing a repository match shows the whole team",
        "specialist-only search narrows to matching members",
        "query and expanded specialist survive refresh",
        "restores q on initial load and updates q without history spam",
        "empty filter removes q and browser back and forward restore bookmarkable query",
        "clear and no-results states remain keyboard accessible",
        "Search repository teams",
        "FRONTEND_MEMBER_LABEL",
        "accessible search & <tag>",
        "url.searchParams.get('q')",
        "expect(await page.evaluate(() => history.length)).toBe(historyLength)",
        "await page.goBack()",
        "await page.goForward()",
        "await page.keyboard.press('Enter')",
        "await page.evaluate(() => window.refresh())",
    ] {
        assert!(
            spec.contains(required),
            "Ensemble search browser spec missing behavior coverage marker {required}"
        );
    }
}

#[test]
fn cockpit_routes_real_native_repositories_and_rejects_invalid_requests() {
    const TEST_NAME: &str =
        "ensemble::tests::cockpit_routes_real_native_repositories_and_rejects_invalid_requests";
    const MARKER: &str = "FUGUE_ENSEMBLE_HTTP_TEST";
    // Native in-process routing is global. A dedicated process prevents this
    // proof's repository names from colliding with another runtime test.
    if std::env::var(MARKER).ok().as_deref() != Some(TEST_NAME) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env(MARKER, TEST_NAME)
            .env("FUGUE_INTEGRATED_TEST_USER", "test-user")
            .env("FUGUE_INTEGRATED_TEST_PASSWORD", "test-pass")
            .arg(TEST_NAME)
            .args(["--exact", "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated Ensemble HTTP test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    // Native bootstrap constructs large futures; give this isolated integration
    // lane sufficient stack without changing normal runtime worker settings.
    std::thread::Builder::new()
        .name("ensemble-http-test".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(http_proof());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn http_proof() {
    let fixture = tempfile::tempdir().unwrap();
    let api = fixture.path().join("api");
    let web = fixture.path().join("web");
    git_repository(&api);
    git_repository(&web);
    let mut config = EnsembleConfig::for_repositories(&[api, web], "HTTP proof".into()).unwrap();
    config.state_root = fixture.path().join("state");
    super::runtime::write_fixture_teams(&config);
    let llm = Arc::new(CountingClient::default());
    let runtime = Arc::new(
        EnsembleRuntime::build_with_client(config, llm.clone())
            .await
            .unwrap(),
    );
    let console =
        crate::console::ConsoleHub::open(&fixture.path().join("console.sqlite3")).unwrap();
    for repo in runtime.config().repositories.keys() {
        console.register(repo.to_string(), runtime.repository(repo).unwrap().clone());
    }
    let app = super::http::router(Arc::clone(&runtime))
        .await
        .unwrap()
        .fallback_service(
            console.router(meerkat_meta::RuntimeDecisionState::local_console(
                meerkat_meta::ConsolePolicy {
                    require_app_auth: false,
                    ..Default::default()
                },
                None,
            )),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let response = client.get(&base).send().await.unwrap();
    assert!(response.status().is_success());
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(response.text().await.unwrap().contains("Repositories"));
    let overview: Overview = client
        .get(format!("{base}/api/ensemble"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview.name, "HTTP proof");
    assert_eq!(overview.runtime.repositories.len(), 2);
    assert_eq!(overview.runtime.peers.len(), 1);
    assert_eq!(overview.runtime.peers[0].from, "api");
    assert_eq!(overview.runtime.peers[0].to, "web");
    let mut sessions = BTreeMap::new();
    for repo in &overview.runtime.repositories {
        assert_eq!(repo.members.len(), 4);
        for member in &repo.members {
            assert_eq!(member.state, "active");
            assert!(!member.member_id.is_empty());
            assert!(member.identity.starts_with(&format!("{}:", repo.repo)));
            assert!(
                sessions
                    .insert(member.session_id.clone().unwrap(), member.identity.clone())
                    .is_none()
            );
        }
    }
    assert_eq!(sessions.len(), 8);
    assert_eq!(
        llm.calls.load(Ordering::SeqCst),
        0,
        "forming teams must not start unsolicited work"
    );

    for repo in ["api", "web"] {
        let html = client
            .get(format!("{base}/repos/{repo}/console"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!html.contains("content=\"/api/repos/"));
        assert!(html.contains("/console/assets/console-app.js"));
        assert!(
            client
                .get(format!("{base}/api/repos/{repo}/meta/console/identities"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    for (asset, content_type) in [
        ("console-app.js", "application/javascript"),
        ("console-app.css", "text/css"),
    ] {
        let response = client
            .get(format!("{base}/console/assets/{asset}"))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with(content_type)
        );
        assert!(!response.bytes().await.unwrap().is_empty());
    }
    for endpoint in ["/api/ensemble", "/api/repos/api/meta/console/identities"] {
        assert_eq!(
            client
                .get(format!("{base}{endpoint}"))
                .header("Origin", "https://evil.example")
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
        assert_eq!(
            client
                .get(format!("{base}{endpoint}"))
                .header("Host", "rebound.example")
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
    for (repo, message, expected) in [
        ("unknown", "hello", reqwest::StatusCode::NOT_FOUND),
        ("api", "   ", reqwest::StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(
            client
                .post(format!("{base}/api/repos/{repo}/messages"))
                .json(&Ask { message })
                .send()
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    let oversized = "x".repeat(64 * 1024 + 1);
    assert_eq!(
        client
            .post(format!("{base}/api/repos/api/messages"))
            .json(&Ask {
                message: &oversized
            })
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        llm.calls.load(Ordering::SeqCst),
        0,
        "rejected requests must not reach a model"
    );
    let response = client
        .post(format!("{base}/api/repos/api/messages"))
        .header("Origin", &base)
        .json(&Ask {
            message: "Answer briefly with the word ready.",
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Accepted = response.json().await.unwrap();
    assert_eq!(accepted.repo, "api");
    assert_eq!(accepted.identity, "api:representative");
    assert!(!accepted.interaction_id.is_empty());
    assert_eq!(sessions.get(&accepted.session_id), Some(&accepted.identity));
    tokio::time::timeout(Duration::from_secs(10), async {
        while llm.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
    let _ = server.await;
    integrated_http_proof(fixture.path(), Arc::clone(&runtime)).await;
    runtime.shutdown().await.unwrap();
    drop(runtime);
    workspace_enrollment_proof().await;
}

async fn integrated_http_proof(root: &Path, runtime: Arc<EnsembleRuntime>) {
    let workflow = root.join("fugue.yaml");
    std::fs::write(
        &workflow,
        format!(
            r#"version: 1
linear:
  kind: fugue-ims
  endpoint: sqlite://{}/issues.sqlite3
  project_slug: integrated-ui-test
selection:
  default_template: implementation_mob
agents:
  issue_lead:
    tools: [mob, workgraph]
mob_templates:
  implementation_mob:
    members: [issue_lead]
worker:
  prompt_template: "Work on {{{{ issue.identifier }}}}"
  native_launch_context_template: "Work in {{{{ worktree }}}}"
workspace:
  root: ./queue-workspaces
meerkat:
  realm: integrated-ui-test
  state_root: ./queue-state
  context_root: ./queue-context
  user_config_root: ./queue-user-config
kapellmeister:
  shadow_mode:
    enabled: false
server:
  port: 0
  meta_console:
    enabled: true
    basic_auth:
      username_env: FUGUE_INTEGRATED_TEST_USER
      password_env: FUGUE_INTEGRATED_TEST_PASSWORD
"#,
            root.display()
        ),
    )
    .unwrap();
    let manager = crate::config::WorkflowConfigManager::load(workflow).unwrap();
    manager.validate_dispatch().unwrap();
    let (refresh_tx, refresh_rx) = tokio::sync::mpsc::channel(8);
    let orchestrator = crate::orchestrator::Orchestrator::new(manager, refresh_rx);
    let registry = orchestrator.meta_runtime_registry();
    let app = crate::http::router(
        orchestrator.shared_state(),
        orchestrator.config().effective().clone(),
        refresh_tx,
        orchestrator.internal_sender(),
        registry.clone(),
        Some(super::http::integrated_router(runtime).await.unwrap()),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        "Basic dGVzdC11c2VyOnRlc3QtcGFzcw==".parse().unwrap(),
    );
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    for path in ["/overview", "/issues", "/ensemble", "/repos/api/console"] {
        let response = client.get(format!("{base}{path}")).send().await.unwrap();
        assert!(
            response.status().is_success(),
            "{path}: {}",
            response.status()
        );
        let html = response.text().await.unwrap();
        assert!(
            html.contains("/overview"),
            "shared navigation missing from {path}"
        );
        assert!(
            html.contains("/issues"),
            "IMS navigation missing from {path}"
        );
        assert!(
            html.contains("/ensemble"),
            "team navigation missing from {path}"
        );
        assert!(
            !html.contains("http://127.0.0.1:8788"),
            "{path} sent the user to another server"
        );
        if path == "/repos/api/console" {
            assert!(!html.contains("content=\"/api/repos/"));
        }
    }
    let overview: Overview = client
        .get(format!("{base}/api/ensemble"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview.runtime.repositories.len(), 2);
    assert!(
        client
            .get(format!("{base}/api/repos/api/meta/console/identities"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert!(
        client
            .get(format!("{base}/api/v1/issues/board"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    for path in [
        "/ensemble",
        "/api/ensemble",
        "/repos/api/console",
        "/api/repos/api/meta/console/identities",
    ] {
        assert_eq!(
            reqwest::get(format!("{base}{path}"))
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{path} bypassed main-app authentication"
        );
        assert_eq!(
            client
                .get(format!("{base}{path}"))
                .header("Origin", "https://evil.example")
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path} bypassed local origin protection"
        );
    }
    let endpoint = format!("{base}/api/repos/api/messages");
    let objective = Ask {
        message: "Confirm the integrated UI reached this representative.",
    };
    assert_eq!(
        reqwest::Client::new()
            .post(&endpoint)
            .json(&objective)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&endpoint)
            .header("Origin", "https://evil.example")
            .json(&objective)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let response = client
        .post(&endpoint)
        .header("Origin", &base)
        .json(&objective)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Accepted = response.json().await.unwrap();
    assert_eq!(accepted.identity, "api:representative");
    server.abort();
    let _ = server.await;
    registry.shutdown().await.unwrap();
}

// Exercise enrollment through HTTP, real native routes, and durable restart.
async fn workspace_enrollment_proof() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("enrolled-one");
    let second = root.path().join("enrolled-two");
    git_repository(&first);
    git_repository(&second);
    let path = root.path().join("workspace.yaml");
    let mut config = EnsembleConfig::for_repositories(
        &[first.clone(), second.clone()],
        "Enrollment proof".into(),
    )
    .unwrap();
    config.state_root = root.path().join("native-state");
    config.peers.clear();
    super::runtime::write_fixture_teams(&config);
    config.repositories.clear();
    std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let llm = Arc::new(CountingClient::default());
    let workspace = super::workspace::Workspace::open_with_client(path.clone(), llm.clone());
    let console = crate::console::ConsoleHub::open(&root.path().join("console.sqlite3")).unwrap();
    workspace.use_console(console.clone()).unwrap();
    workspace.start().await.unwrap();
    let app = super::http::workspace_router(Arc::clone(&workspace), true)
        .unwrap()
        .fallback_service(
            console.router(meerkat_meta::RuntimeDecisionState::local_console(
                meerkat_meta::ConsolePolicy {
                    require_app_auth: false,
                    ..Default::default()
                },
                None,
            )),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let initial: serde_json::Value = client
        .get(format!("{base}/api/ensemble"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(initial["setup"]["phase"], "empty");
    let request = serde_json::json!({"id":"enrolled-one", "path":first});
    let forbidden = client
        .post(format!("{base}/api/repos"))
        .header("Origin", "https://untrusted.example")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);
    let invalid = client
        .post(format!("{base}/api/repos"))
        .json(&serde_json::json!({"id":"invalid", "path":"relative/path"}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::CONFLICT);
    assert!(EnsembleConfig::load(&path).unwrap().repositories.is_empty());
    let added = client
        .post(format!("{base}/api/repos"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(added.status(), reqwest::StatusCode::ACCEPTED);
    let ready = wait_for_workspace(&client, &base).await;
    assert_eq!(
        ready["runtime"]["repositories"].as_array().unwrap().len(),
        1
    );
    assert!(
        client
            .get(format!("{base}/repos/enrolled-one/console"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .contains("/console/assets/console-app.js")
    );
    let accepted = client
        .post(format!("{base}/api/repos/enrolled-one/messages"))
        .json(&Ask {
            message: "Enrollment conversation persists.",
        })
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
    let accepted: Accepted = accepted.json().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let response = client
                .post(format!("{base}/api/repos"))
                .json(&serde_json::json!({"id":"enrolled-two", "path":second}))
                .send()
                .await
                .unwrap();
            if response.status() == reqwest::StatusCode::ACCEPTED {
                break;
            }
            let body = response.text().await.unwrap();
            assert!(body.contains("active or unobserved work"), "{body}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let updated = wait_for_workspace(&client, &base).await;
    assert_eq!(
        updated["runtime"]["repositories"].as_array().unwrap().len(),
        2
    );
    assert_eq!(updated["runtime"]["peers"], serde_json::json!([]));
    assert_eq!(EnsembleConfig::load(&path).unwrap().repositories.len(), 2);
    assert!(
        updated["runtime"]["repositories"][0]["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|member| member["session_id"] == accepted.session_id)
    );
    workspace.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
    drop(workspace);
    let restored = super::workspace::Workspace::open_with_client(path, llm);
    restored.start().await.unwrap();
    let app = super::http::workspace_router(Arc::clone(&restored), true).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let status = wait_for_workspace(&client, &base).await;
    assert_eq!(
        status["runtime"]["repositories"].as_array().unwrap().len(),
        2
    );
    assert!(
        status["runtime"]["repositories"][0]["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|member| member["session_id"] == accepted.session_id)
    );
    restored.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
}

async fn wait_for_workspace(client: &reqwest::Client, base: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status: serde_json::Value = client
                .get(format!("{base}/api/ensemble"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_ne!(status["setup"]["phase"], "failed", "{status}");
            if status["setup"]["phase"] == "ready" {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap()
}
