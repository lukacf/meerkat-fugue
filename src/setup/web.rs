use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::StatusCode,
    middleware,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::connections::{
    ConnectionStore, Connections, GitHubConnection, ModelConnection, TrackerConnection,
};

pub struct Setup {
    connections: Mutex<ConnectionState>,
    root: PathBuf,
    port: u16,
    preparation: Mutex<Preparation>,
    operation: Mutex<Option<tokio::task::JoinHandle<()>>>,
    restart: tokio_util::sync::CancellationToken,
    login: super::auth::Login,
}

enum Preparation {
    Draft,
    Preparing,
    Ready(Box<super::managed::ActiveWorkspace>),
    Failed(String),
}

#[derive(Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum PreparationStatus {
    Draft,
    Preparing,
    Ready,
    Failed { error: String },
}

struct ConnectionState {
    store: ConnectionStore,
    value: Connections,
}

impl Setup {
    pub fn open(root: &Path, port: u16) -> Result<Arc<Self>> {
        let store = ConnectionStore::open(root)?;
        let value = store.load()?;
        let preparation = match std::fs::read_to_string(root.join("startup-error.txt")) {
            Ok(message) => Preparation::Failed(message),
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Preparation::Draft,
            Err(cause) => return Err(cause.into()),
        };
        Ok(Arc::new(Self {
            root: root.to_path_buf(),
            port,
            preparation: Mutex::new(preparation),
            operation: Mutex::new(None),
            restart: tokio_util::sync::CancellationToken::new(),
            login: super::auth::Login::default(),
            connections: Mutex::new(ConnectionState { store, value }),
        }))
    }
}

async fn preparation_status(State(setup): State<Arc<Setup>>) -> Json<PreparationStatus> {
    Json(match &*setup.preparation.lock().await {
        Preparation::Draft => PreparationStatus::Draft,
        Preparation::Preparing => PreparationStatus::Preparing,
        Preparation::Ready(_) => PreparationStatus::Ready,
        Preparation::Failed(error) => PreparationStatus::Failed {
            error: error.clone(),
        },
    })
}

async fn workflow_draft(State(setup): State<Arc<Setup>>) -> Response {
    let draft = match std::fs::read(setup.root.join("workflow-draft.json")) {
        Ok(bytes) => serde_json::from_slice::<super::workflow::WorkflowSetup>(&bytes)
            .map(Some)
            .map_err(anyhow::Error::from),
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => {
            super::managed::ActiveWorkspace::load(&setup.root)
                .map(|active| active.map(|active| active.setup))
        }
        Err(cause) => Err(cause.into()),
    };
    match draft {
        Ok(draft) => Json(draft).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not read saved workspace settings",
        ),
    }
}

async fn prepare(
    State(setup): State<Arc<Setup>>,
    request: Result<Json<super::workflow::WorkflowSetup>, JsonRejection>,
) -> Response {
    let Ok(Json(request)) = request else {
        return error(
            StatusCode::BAD_REQUEST,
            "Invalid repository or workflow settings",
        );
    };
    if let Err(cause) = request.validate() {
        return error(StatusCode::BAD_REQUEST, cause);
    }
    let mut preparation = setup.preparation.lock().await;
    if matches!(*preparation, Preparation::Preparing) {
        return error(StatusCode::CONFLICT, "Preparation is already running");
    }
    let connections = setup.connections.lock().await.value.clone();
    if let Err(cause) = super::directory::validate_selection(
        &setup.root,
        &request,
        connections
            .github
            .as_ref()
            .map(|github| github.host.as_str()),
    ) {
        return error(StatusCode::BAD_REQUEST, cause);
    }
    let saved = super::save_json(&setup.root.join("workflow-draft.json"), &request);
    if saved.is_err() {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not save workspace settings",
        );
    }
    *preparation = Preparation::Preparing;
    drop(preparation);
    let mut operation = setup.operation.lock().await;
    let work = Arc::clone(&setup);
    *operation = Some(tokio::spawn(async move {
        let result = super::managed::prepare(&work.root, &request, &connections, work.port).await;
        *work.preparation.lock().await = match result {
            Ok(manifest) => Preparation::Ready(Box::new(manifest)),
            Err(cause) => Preparation::Failed(cause.to_string()),
        };
    }));
    (StatusCode::ACCEPTED, Json(PreparationStatus::Preparing)).into_response()
}

async fn start(State(setup): State<Arc<Setup>>) -> Response {
    let preparation = setup.preparation.lock().await;
    let Preparation::Ready(manifest) = &*preparation else {
        return error(
            StatusCode::CONFLICT,
            "Prepare the repository and workflow first",
        );
    };
    if let Err(cause) = manifest.activate(&setup.root) {
        return error(StatusCode::INTERNAL_SERVER_ERROR, cause);
    }
    if let Err(cause) = std::fs::remove_file(setup.root.join("startup-error.txt"))
        && cause.kind() != std::io::ErrorKind::NotFound
    {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not reset workspace startup status",
        );
    }
    setup.restart.cancel();
    (StatusCode::ACCEPTED, Json(PreparationStatus::Ready)).into_response()
}

#[derive(Deserialize)]
#[serde(
    tag = "connection",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum UpdateConnection {
    Model(ModelConnection),
    Github(GitHubConnection),
    Tracker(TrackerConnection),
}

#[derive(Serialize)]
struct Error {
    error: String,
}

fn error(status: StatusCode, message: impl ToString) -> Response {
    (
        status,
        Json(Error {
            error: message.to_string(),
        }),
    )
        .into_response()
}

async fn status(State(setup): State<Arc<Setup>>) -> Response {
    Json(setup.connections.lock().await.value.status()).into_response()
}

async fn update(
    State(setup): State<Arc<Setup>>,
    request: Result<Json<UpdateConnection>, JsonRejection>,
) -> Response {
    let Ok(Json(update)) = request else {
        // Deserializer diagnostics can quote the submitted value. A credential
        // pasted into the wrong field must not be echoed into browser errors.
        return error(StatusCode::BAD_REQUEST, "Invalid connection details");
    };
    let mut preparation = setup.preparation.lock().await;
    if matches!(*preparation, Preparation::Preparing) {
        return error(
            StatusCode::CONFLICT,
            "Wait for the current connection checks to finish before changing credentials",
        );
    }
    let mut state = setup.connections.lock().await;
    let mut next = state.value.clone();
    let update_model = matches!(&update, UpdateConnection::Model(_));
    match update {
        UpdateConnection::Model(value) => next.model = Some(value),
        UpdateConnection::Github(value) => next.github = Some(value),
        UpdateConnection::Tracker(value) => next.tracker = Some(value),
    }
    next.retain_missing_credentials(&state.value);
    if update_model
        && let Some(model) = &mut next.model
        && let super::connections::ModelCredential::ApiKey { key } = &model.credential
    {
        match super::auth::provision_key(model.provider, key, &setup.root).await {
            Ok(auth_binding) => {
                model.credential = super::connections::ModelCredential::Binding { auth_binding }
            }
            Err(cause) => return error(StatusCode::BAD_GATEWAY, cause),
        }
    }
    match state.store.save(&next) {
        Ok(()) => {
            state.value = next;
            *preparation = Preparation::Draft;
            Json(state.value.status()).into_response()
        }
        Err(cause) => error(StatusCode::BAD_REQUEST, cause),
    }
}

async fn check_github(State(setup): State<Arc<Setup>>) -> Response {
    let connection = setup.connections.lock().await.value.github.clone();
    let Some(connection) = connection else {
        return error(StatusCode::CONFLICT, "Save a GitHub connection first");
    };
    match connection.check().await {
        Ok(account) => Json(account).into_response(),
        Err(cause) => error(StatusCode::BAD_GATEWAY, cause),
    }
}

async fn accounts() -> Response {
    match super::auth::accounts().await {
        Ok(accounts) => Json(accounts).into_response(),
        Err(cause) => error(StatusCode::SERVICE_UNAVAILABLE, cause),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JiraProjectRequest {
    project: String,
}

async fn jira_states(
    State(setup): State<Arc<Setup>>,
    request: Result<Json<JiraProjectRequest>, JsonRejection>,
) -> Response {
    let Ok(Json(request)) = request else {
        return error(StatusCode::BAD_REQUEST, "Enter a Jira project key");
    };
    let connections = setup.connections.lock().await.value.clone();
    let result = async {
        connections
            .jira_client(&request.project)?
            .list_project_states()
            .await
            .map_err(anyhow::Error::from)
    }
    .await;
    match result {
        Ok(states) => Json(states).into_response(),
        Err(cause) => error(StatusCode::BAD_REQUEST, cause),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginRequest {
    provider: super::connections::ModelProvider,
}

async fn login_start(
    State(setup): State<Arc<Setup>>,
    request: Result<Json<LoginRequest>, JsonRejection>,
) -> Response {
    let Ok(Json(request)) = request else {
        return error(StatusCode::BAD_REQUEST, "Choose a supported rkat provider");
    };
    if matches!(*setup.preparation.lock().await, Preparation::Preparing) {
        return error(
            StatusCode::CONFLICT,
            "Wait for workspace preparation before changing AI access",
        );
    }
    match setup.login.start(request.provider, &setup.root).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(setup.login.status().await)).into_response(),
        Err(cause) => error(StatusCode::BAD_REQUEST, cause),
    }
}

async fn login_cancel(State(setup): State<Arc<Setup>>) -> Response {
    match setup.login.cancel().await {
        Ok(()) => Json(setup.login.status().await).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not cancel rkat sign-in",
        ),
    }
}

pub fn router(setup: Arc<Setup>) -> Router {
    let page = include_str!("setup.html")
        .replace("__APP_CSS__", include_str!("../ui/app.css"))
        .replace("__DEFAULT_MODEL__", crate::config::default_agent_model())
        .replace(
            "__OPENAI_MODEL__",
            meerkat_models::default_model(meerkat_core::Provider::OpenAI)
                .unwrap_or(crate::config::default_agent_model()),
        )
        .replace(
            "__ANTHROPIC_MODEL__",
            meerkat_models::default_model(meerkat_core::Provider::Anthropic)
                .unwrap_or(crate::config::default_agent_model()),
        )
        .replace(
            "__GEMINI_MODEL__",
            meerkat_models::default_model(meerkat_core::Provider::Gemini)
                .unwrap_or(crate::config::default_agent_model()),
        );
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/setup") }))
        .route("/overview", get(|| async { Redirect::temporary("/setup") }))
        .route("/issues", get(|| async { Redirect::temporary("/setup") }))
        .route("/queues", get(|| async { Redirect::temporary("/setup") }))
        .route("/ensemble", get(|| async { Redirect::temporary("/setup") }))
        .route(
            "/setup",
            get(move || {
                let page = page.clone();
                async move {
                    Html(
                        crate::http::inject_app_chrome(&page, crate::http::AppPage::Setup, false)
                            .unwrap_or(page),
                    )
                }
            }),
        )
        .route("/api/setup/connections", get(status).post(update))
        .route(
            "/api/setup/identity",
            get(|State(setup): State<Arc<Setup>>| async move {
                Json(
                    setup
                        .root
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                )
            }),
        )
        .route("/api/setup/auth/accounts", get(accounts))
        .route(
            "/api/setup/auth/login",
            get(|State(setup): State<Arc<Setup>>| async move { Json(setup.login.status().await) })
                .post(login_start),
        )
        .route("/api/setup/auth/cancel", post(login_cancel))
        .route("/api/setup/check/github", post(check_github))
        .route("/api/setup/check/jira-states", post(jira_states))
        .route("/api/setup/workflow", get(workflow_draft))
        .route(
            "/api/setup/preparation",
            get(preparation_status).post(prepare),
        )
        .route("/api/setup/start", post(start))
        .with_state(setup)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn(
            crate::ensemble::http::local_origin_guard,
        ))
}

pub async fn serve(root: &Path, address: SocketAddr) -> Result<()> {
    if !address.ip().is_loopback() {
        bail!("First-run setup requires a loopback listener");
    }
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let setup = Setup::open(root, address.port())?;
    let restart = setup.restart.clone();
    println!("Set up Fugue at http://{address}/setup");
    axum::serve(listener, router(Arc::clone(&setup)))
        .with_graceful_shutdown({let restart=restart.clone();async move {
            tokio::select! { _=restart.cancelled()=>{},signal=super::managed::shutdown_signal()=>{
                if let Err(cause)=signal {tracing::error!(error=%cause,"setup shutdown signal failed");}
            }}
        }
        })
        .await
        .context("serve first-run setup")?;
    if let Some(operation) = setup.operation.lock().await.take() {
        operation.abort();
        if let Err(cause) = operation.await
            && !cause.is_cancelled()
        {
            return Err(cause.into());
        }
    }
    setup.login.cancel().await?;
    if restart.is_cancelled() {
        return Err(super::managed::RestartRequested.into());
    }
    Ok(())
}

pub(crate) async fn serve_managed(
    root: &Path,
    port: u16,
    hub: &str,
    stop: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let setup = Setup::open(root, port)?;
    let restart = setup.restart.clone();
    let app = router(setup.clone()).merge(super::directory::hub_router(hub.to_owned())?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! { _ = stop.cancelled() => {}, _ = restart.cancelled() => {} }
        })
        .await?;
    if let Some(operation) = setup.operation.lock().await.take() {
        operation.abort();
        if let Err(error) = operation.await
            && !error.is_cancelled()
        {
            return Err(error).context("stop workspace preparation");
        }
    }
    setup.login.cancel().await?;
    Ok(())
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
    async fn registered_repository_cannot_be_retargeted_by_preparation() {
        let directory = tempfile::tempdir().unwrap();
        let setup = Setup::open(directory.path(), 8788).unwrap();
        std::fs::write(directory.path().join("entry.json"),
            r#"{"repository":{"host":"github.com","owner":"example","repository":"service"},"port":8788}"#).unwrap();
        let draft = directory.path().join("workflow-draft.json");
        std::fs::write(&draft, "original draft").unwrap();
        let request = serde_json::json!({
            "repository_url": "https://github.com/example/web",
            "owner": "example", "repository": "web", "lanes": crate::config::TrackerLaneRoles::default()
        });
        let response = router(setup.clone())
            .oneshot(
                Request::post("/api/setup/preparation")
                    .header("Host", "127.0.0.1:8788")
                    .header("Content-Type", "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(std::fs::read_to_string(draft).unwrap(), "original draft");
        assert!(setup.operation.lock().await.is_none());
    }

    #[tokio::test]
    async fn managed_setup_composes_directory_routes_without_duplicates() {
        let directory = tempfile::tempdir().unwrap();
        let setup = Setup::open(directory.path(), 8789).unwrap();
        let app = router(setup)
            .merge(super::super::directory::hub_router("http://127.0.0.1:8788".into()).unwrap());
        for (path, expected) in [
            ("/setup", StatusCode::OK),
            ("/queues", StatusCode::TEMPORARY_REDIRECT),
            ("/overview", StatusCode::TEMPORARY_REDIRECT),
            ("/api/workspaces", StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(path)
                        .header("Host", "127.0.0.1:8789")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{path}");
        }
    }

    #[tokio::test]
    async fn connection_api_never_returns_keys_and_rejects_cross_origin_writes() {
        let directory = tempfile::tempdir().unwrap();
        let app = router(Setup::open(directory.path(), 8788).unwrap());
        let request = |origin: &str| {
            Request::builder().method("POST").uri("/api/setup/connections")
            .header("Host", "127.0.0.1:8788").header("Origin", origin).header("Content-Type", "application/json")
            .body(Body::from(r#"{"connection":"github","value":{"host":"github.com","credential":{"method":"token","token":"sensitive-test-sentinel"}}}"#)).unwrap()
        };
        let rejected = app
            .clone()
            .oneshot(request("https://untrusted.example"))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
        let response = app
            .clone()
            .oneshot(request("http://127.0.0.1:8788"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body = to_bytes(response.into_body(), 10240).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("sensitive-test-sentinel"));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/setup/connections")
                    .header("Host", "127.0.0.1:8788")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 10240).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("sensitive-test-sentinel"));
    }

    #[tokio::test]
    async fn invalid_secret_input_is_not_reflected_in_errors() {
        let directory = tempfile::tempdir().unwrap();
        let app = router(Setup::open(directory.path(), 8788).unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/setup/connections")
                    .header("Host", "localhost:8788")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"connection":"accidental-secret-sentinel"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 10240).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("accidental-secret-sentinel"));
    }
}
