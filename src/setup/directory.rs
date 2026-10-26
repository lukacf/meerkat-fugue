//! Each repository has its own workflow, credentials and native mob.
//! The directory hosts their lifecycles and one shared native Meta console.
use super::{
    connections::{ConnectionStore, Connections, GitHubConnection, GitHubCredential},
    repository::RepositoryAddress,
};
use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Request, State, rejection::JsonRejection},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{any, get, post},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    repository: RepositoryAddress,
    port: u16,
}

pub(super) fn validate_selection(
    root: &std::path::Path,
    setup: &super::workflow::WorkflowSetup,
    github_host: Option<&str>,
) -> Result<()> {
    let bytes = match std::fs::read(root.join("entry.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let entry: Entry = serde_json::from_slice(&bytes)?;
    let selected = setup
        .repository_url
        .as_deref()
        .map(RepositoryAddress::parse)
        .transpose()?;
    if !entry.repository.owner.eq_ignore_ascii_case(&setup.owner)
        || !entry
            .repository
            .repository
            .eq_ignore_ascii_case(&setup.repository)
        || selected
            .as_ref()
            .is_some_and(|repo| !entry.repository.host.eq_ignore_ascii_case(&repo.host))
        || github_host.is_some_and(|host| !entry.repository.host.eq_ignore_ascii_case(host))
    {
        bail!(
            "This workspace belongs to its registered repository. Add a new repository from Repositories to use a different address."
        );
    }
    Ok(())
}

struct Running {
    task: tokio::task::JoinHandle<Result<()>>,
    stop: tokio_util::sync::CancellationToken,
}

pub struct Directory {
    root: PathBuf,
    hub: String,
    console: Arc<crate::console::ConsoleHub>,
    _lock: crate::file_lease::FileLease,
    children: Mutex<BTreeMap<String, Running>>,
    browser_client: reqwest::Client,
    lifecycle: Arc<super::runtime_host::HostLifecycle>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Add {
    repository_url: String,
}

#[derive(Serialize)]
struct Workspace {
    id: String,
    repository: RepositoryAddress,
    url: String,
    configured: bool,
    running: bool,
}

#[derive(Serialize)]
struct Listing {
    hub: Option<String>,
    workspaces: Vec<Workspace>,
}

impl Directory {
    pub fn open(root: PathBuf, port: u16) -> Result<Arc<Self>> {
        // Reuse the private-storage boundary and its exclusive OS lock.
        let store = ConnectionStore::open(&root)?;
        drop(store);
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("directory.lock"))?;
        let lock = crate::file_lease::FileLease::acquire(lock)
            .context("Another Fugue directory owns these workspaces")?;
        let console = crate::console::ConsoleHub::open(&root.join("console.sqlite3"))?;
        Ok(Arc::new(Self {
            root,
            console,
            hub: format!("http://127.0.0.1:{port}"),
            _lock: lock,
            children: Mutex::new(BTreeMap::new()),
            browser_client: super::proxy::client()?,
            lifecycle: Arc::default(),
        }))
    }

    fn entries(&self) -> Result<BTreeMap<String, Entry>> {
        let mut entries = BTreeMap::new();
        for item in std::fs::read_dir(&self.root)? {
            let item = item?;
            if !item.file_type()?.is_dir() {
                continue;
            }
            let id = item.file_name().to_string_lossy().to_string();
            if !id.starts_with("repository-") {
                continue;
            }
            let path = item.path().join("entry.json");
            if path.exists() {
                entries.insert(id, serde_json::from_slice(&std::fs::read(path)?)?);
            }
        }
        Ok(entries)
    }

    pub fn console(&self) -> Arc<crate::console::ConsoleHub> {
        self.console.clone()
    }

    async fn spawn(&self, id: &str, entry: &Entry) -> Result<Running> {
        self.lifecycle.ensure_can_start()?;
        let root = self.root.join(id);
        let stop = tokio_util::sync::CancellationToken::new();
        let run_stop = stop.clone();
        let hub = self.hub.clone();
        let console = self.console.clone();
        let port = entry.port;
        let lifecycle = self.lifecycle.clone();
        let task = tokio::spawn(async move {
            super::runtime_host::serve(&root, port, hub, console, run_stop, lifecycle).await
        });
        Ok(Running { task, stop })
    }

    pub async fn restore(&self) -> Result<()> {
        let mut children = self.children.lock().await;
        for (id, entry) in self.entries()? {
            children.insert(id.clone(), self.spawn(&id, &entry).await?);
        }
        Ok(())
    }

    async fn list(&self) -> Result<Listing> {
        let mut children = self.children.lock().await;
        let mut workspaces = Vec::new();
        for (id, entry) in self.entries()? {
            let running = match children.get_mut(&id) {
                Some(running) => !running.task.is_finished(),
                None => false,
            };
            let root = self.root.join(&id);
            let configured = super::managed::ActiveWorkspace::load(&root)?.is_some()
                && !root.join("startup-error.txt").try_exists()?;
            workspaces.push(Workspace {
                url: format!(
                    "/repositories/{}/{}",
                    id,
                    if configured { "issues" } else { "setup" }
                ),
                id,
                repository: entry.repository,
                configured,
                running,
            });
        }
        Ok(Listing {
            hub: None,
            workspaces,
        })
    }

    async fn resume(&self, id: &str) -> Result<()> {
        self.lifecycle.ensure_can_start()?;
        let mut children = self.children.lock().await;
        let entry = self
            .entries()?
            .remove(id)
            .context("Repository workspace was not found")?;
        if let Some(running) = children.get_mut(id)
            && !running.task.is_finished()
        {
            return Ok(());
        }
        // The generated tool endpoints use this port as well as the UI.
        // Retain it on resume rather than silently moving only the HTTP server.
        let listener = std::net::TcpListener::bind(("127.0.0.1", entry.port)).context(
            "The workspace port is in use. Stop the conflicting process before resuming.",
        )?;
        drop(listener);
        children.insert(id.into(), self.spawn(id, &entry).await?);
        Ok(())
    }

    async fn add(&self, request: Add) -> Result<Workspace> {
        self.lifecycle.ensure_can_start()?;
        let repository = RepositoryAddress::parse(&request.repository_url)?;
        let mut children = self.children.lock().await;
        if self
            .entries()?
            .values()
            .any(|entry| entry.repository.same_repository(&repository))
        {
            bail!("This repository already has a workspace. Continue its setup below.");
        }
        let id = format!("repository-{}", meerkat_core::types::SessionId::new());
        let root = self.root.join(&id);
        let store = ConnectionStore::open(&root)?;
        store.save(&Connections {
            github: Some(GitHubConnection {
                host: repository.host.clone(),
                credential: GitHubCredential::GhCli,
            }),
            ..Default::default()
        })?;
        drop(store);
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let entry = Entry {
            repository: repository.clone(),
            port,
        };
        let draft = super::workflow::WorkflowSetup {
            lane_configuration: super::workflow::LaneConfiguration::Generated,
            commit_identity: Default::default(),
            allow_no_ci_checks: false,
            independent_reviewer_count: 1,
            jira_transition_fields: BTreeMap::new(),
            repository_url: Some(repository.https_url()),
            owner: repository.owner.clone(),
            repository: repository.repository.clone(),
            checkout: None,
            tracker_project: None,
            tracker_team: None,
            jira_issue_type: Some("Task".into()),
            jira_jql: None,
            jira_issue_labels: Vec::new(),
            lanes: crate::config::TrackerLaneRoles {
                deploy_action: "Merge".into(),
                ..Default::default()
            },
        };
        super::save_json(&root.join("workflow-draft.json"), &draft)?;
        // Publish the entry last, after its private connections and initial
        // draft are durable. An interrupted add cannot expose partial JSON to
        // the directory's startup scan. A port collision refuses child startup.
        super::save_json(&root.join("entry.json"), &entry)?;
        #[cfg(unix)]
        std::fs::File::open(&self.root)?.sync_all()?;
        drop(listener);
        let running = self.spawn(&id, &entry).await?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()?;
        let url = format!("http://127.0.0.1:{port}");
        let mut ready = false;
        for _ in 0..100 {
            if running.task.is_finished() {
                break;
            }
            if let Ok(response) = client.get(format!("{url}/api/setup/identity")).send().await
                && let Ok(identity) = response.json::<String>().await
                && identity == id
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        children.insert(id.clone(), running);
        if !ready {
            bail!(
                "The repository setup could not start. Its private draft is saved; restart Fugue to retry."
            );
        }
        let browser_url = format!("/repositories/{id}/setup");
        Ok(Workspace {
            id,
            repository,
            url: browser_url,
            configured: false,
            running: true,
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut children = self.children.lock().await;
        for running in children.values() {
            running.stop.cancel();
        }
        let mut errors = Vec::new();
        for (id, running) in std::mem::take(&mut *children) {
            match running.task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("{id}: {error}")),
                Err(error) => errors.push(format!("{id}: {error}")),
            }
        }
        if !errors.is_empty() {
            bail!("workspace shutdown failed: {errors:?}");
        }
        Ok(())
    }
}

fn failure(error: anyhow::Error) -> Response {
    #[derive(Serialize)]
    struct Error {
        error: String,
    }
    (
        StatusCode::BAD_REQUEST,
        Json(Error {
            error: error.to_string(),
        }),
    )
        .into_response()
}

pub fn router(directory: Arc<Directory>) -> Router {
    Router::new()
        .route(
            "/overview",
            get(|| async { Html(crate::http::render_home_html(false)) }),
        )
        .route(
            "/queues",
            get(|State(directory): State<Arc<Directory>>| async move {
                match directory.list().await {
                    Ok(list) => queue_directory(list),
                    Err(error) => failure(error),
                }
            }),
        )
        .with_state(directory.clone())
        .merge(api_router(directory))
        .layer(middleware::from_fn(
            crate::ensemble::http::local_origin_guard,
        ))
}

/// Compose with an existing queue that already owns its Home and Board pages.
pub fn api_router(directory: Arc<Directory>) -> Router {
    let proxy = Router::new()
        .route(
            "/repositories/{id}/{*path}",
            any(
                |State(directory): State<Arc<Directory>>,
                 Path((id, _)): Path<(String, String)>,
                 request: Request| async move {
                    let result = async {
                        let entry = directory
                            .entries()?
                            .remove(&id)
                            .context("Repository workspace was not found")?;
                        super::proxy::forward(
                            &directory.browser_client,
                            entry.port,
                            &directory.hub,
                            &format!("/repositories/{id}"),
                            request,
                        )
                        .await
                    }
                    .await;
                    match result {
                        Ok(response) => response,
                        Err(error) => failure(error),
                    }
                },
            ),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(directory.clone());
    Router::new()
        .route(
            "/api/workspaces/{id}/resume",
            post(
                |State(directory): State<Arc<Directory>>, Path(id): Path<String>| async move {
                    match directory.resume(&id).await {
                        Ok(()) => StatusCode::ACCEPTED.into_response(),
                        Err(error) => failure(error),
                    }
                },
            ),
        )
        .route(
            "/api/workspaces",
            get(|State(directory): State<Arc<Directory>>| async move {
                match directory.list().await {
                    Ok(list) => Json(list).into_response(),
                    Err(error) => failure(error),
                }
            })
            .post(
                |State(directory): State<Arc<Directory>>,
                 request: Result<Json<Add>, JsonRejection>| async move {
                    let Ok(Json(request)) = request else {
                        return failure(anyhow::anyhow!("Enter a repository URL"));
                    };
                    match directory.add(request).await {
                        Ok(workspace) => (StatusCode::CREATED, Json(workspace)).into_response(),
                        Err(error) => failure(error),
                    }
                },
            ),
        )
        .with_state(directory)
        .layer(axum::extract::DefaultBodyLimit::max(4096))
        .merge(proxy)
        .layer(middleware::from_fn(
            crate::ensemble::http::local_origin_guard,
        ))
}

fn queue_directory(_list: Listing) -> Response {
    let html = include_str!("../ui/queues.html");
    Html(
        crate::http::inject_app_chrome(html, crate::http::AppPage::Work, false)
            .unwrap_or_else(|| html.to_owned()),
    )
    .into_response()
}

pub fn hub_router(hub: String) -> Result<Router> {
    let url = reqwest::Url::parse(&hub)?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || url.path() != "/"
    {
        bail!("Invalid Fugue workspace directory address");
    }
    // Page routes belong to the workspace app or setup router. Registering a
    // second /queues handler here makes Axum panic when composing either app.
    Ok(Router::new().route(
        "/api/workspaces",
        get(move || async {
            Json(Listing {
                hub: Some("/".into()),
                workspaces: Vec::new(),
            })
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn browser_proxy_uses_registered_port_preserves_body_and_rejects_cross_origin() {
        use axum::http::HeaderMap;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let child = Router::new().route(
            "/echo",
            post(move |headers: HeaderMap, body: String| async move {
                assert_eq!(headers["authorization"], "Bearer private-edge-credential");
                assert!(!headers.contains_key("cookie"));
                assert_eq!(headers["origin"], "http://127.0.0.1:8788");
                body
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, child).await.unwrap() });
        let temp = tempfile::tempdir().unwrap();
        let directory = Directory::open(temp.path().to_owned(), 8788).unwrap();
        let root = temp.path().join("repository-test");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(
            root.join("entry.json"),
            serde_json::to_vec(&Entry {
                repository: RepositoryAddress::parse("https://github.com/example/application")
                    .unwrap(),
                port,
            })
            .unwrap(),
        )
        .unwrap();
        let app = router(directory);
        for (origin, expected) in [
            ("http://127.0.0.1:8788", StatusCode::OK),
            ("https://other.example", StatusCode::FORBIDDEN),
        ] {
            let body = "test-body".repeat(2000);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/repositories/repository-test/echo?example=1")
                        .header("Host", "127.0.0.1:8788")
                        .header("Origin", origin)
                        .header("Authorization", "Bearer private-edge-credential")
                        .header("Cookie", "session=private")
                        .body(Body::from(body.clone()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::OK {
                assert_eq!(to_bytes(response.into_body(), 100_000).await.unwrap(), body);
            }
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/repositories/repository-unknown/echo")
                    .header("Host", "127.0.0.1:8788")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        server.abort();
    }

    #[test]
    fn one_ready_repository_does_not_change_the_global_board_scope() {
        let workspace = Workspace {
            id: "repository-one".into(),
            repository: RepositoryAddress::parse("https://github.com/example/service").unwrap(),
            url: "http://127.0.0.1:9001/issues".into(),
            configured: true,
            running: true,
        };
        let response = queue_directory(Listing {
            hub: None,
            workspaces: vec![workspace],
        });
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key("location"));
    }

    #[tokio::test]
    async fn hub_queue_directory_keeps_work_and_conversation_navigation() {
        let temp = tempfile::tempdir().unwrap();
        let directory = Directory::open(temp.path().to_owned(), 8788).unwrap();
        let response = router(directory)
            .oneshot(
                Request::get("/queues")
                    .header("Host", "127.0.0.1:8788")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        let html = std::str::from_utf8(&body).unwrap();
        assert!(html.contains(r#"href="/queues" class="active""#));
        assert!(html.contains(r#"href="/console""#));
        assert!(html.contains("/api/workspaces"));
    }

    #[tokio::test]
    async fn directory_restores_distinct_repository_records_without_sharing_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let directory = Directory::open(temp.path().to_owned(), 8788).unwrap();
        for (id, repo, port) in [
            ("repository-one", "service", 9001),
            ("repository-two", "web", 9002),
        ] {
            let root = temp.path().join(id);
            let store = ConnectionStore::open(&root).unwrap();
            store
                .save(&Connections {
                    tracker: Some(super::super::connections::TrackerConnection::Local),
                    ..Default::default()
                })
                .unwrap();
            let entry = Entry {
                repository: RepositoryAddress::parse(&format!(
                    "https://git.example.com/team/{repo}"
                ))
                .unwrap(),
                port,
            };
            std::fs::write(root.join("entry.json"), serde_json::to_vec(&entry).unwrap()).unwrap();
        }
        assert!(Directory::open(temp.path().to_owned(), 8788).is_err());
        let listing = directory.list().await.unwrap();
        assert_eq!(listing.workspaces.len(), 2);
        assert_ne!(listing.workspaces[0].url, listing.workspaces[1].url);
        assert!(
            listing
                .workspaces
                .iter()
                .all(|workspace| !workspace.configured && !workspace.running)
        );
        assert!(directory.resume("../outside").await.is_err());
    }

    #[tokio::test]
    async fn case_variants_cannot_register_the_same_repository_twice() {
        let temp = tempfile::tempdir().unwrap();
        let directory = Directory::open(temp.path().to_owned(), 8788).unwrap();
        let root = temp.path().join("repository-existing");
        std::fs::create_dir(&root).unwrap();
        let entry = Entry {
            repository: RepositoryAddress::parse("https://github.com/Example/App").unwrap(),
            port: 9001,
        };
        super::super::save_json(&root.join("entry.json"), &entry).unwrap();
        let result = directory
            .add(Add {
                repository_url: "https://github.com/example/app".into(),
            })
            .await;
        directory.shutdown().await.unwrap();
        assert!(
            result.is_err(),
            "case variants must not create another native mob"
        );
        assert_eq!(directory.entries().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn remote_creation_rejects_cross_origin_and_embedded_credentials_before_spawning() {
        let temp = tempfile::tempdir().unwrap();
        let directory = Directory::open(temp.path().to_owned(), 8788).unwrap();
        let app = router(Arc::clone(&directory));
        for (origin, expected) in [
            ("https://untrusted.example", StatusCode::FORBIDDEN),
            ("http://127.0.0.1:8788", StatusCode::BAD_REQUEST),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/api/workspaces")
                        .header("Host", "127.0.0.1:8788")
                        .header("Origin", origin)
                        .header("Content-Type", "application/json")
                        .body(Body::from(
                            r#"{"repository_url":"https://secret-sentinel@github.com/team/repo"}"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let body = to_bytes(response.into_body(), 8192).await.unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("secret-sentinel"));
        }
        assert!(directory.entries().unwrap().is_empty());
        assert!(directory.children.lock().await.is_empty());
    }
}
