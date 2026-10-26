//! Local repository enrollment and the lifecycle of its native teams.
//!
//! Formation runs outside HTTP requests. This layer owns configuration and
//! startup only; native runtimes continue to own conversations and agent work.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tower::ServiceExt;

use super::{
    config::{EnsembleConfig, RepoId, RepositoryConfig},
    runtime::{EnsembleRuntime, EnsembleStatus},
};

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum SetupState {
    Empty,
    Preparing,
    Ready,
    Failed {
        error: String,
        restart_required: bool,
    },
    Stopped,
}

impl SetupState {
    fn ensure_cleanup_complete(&self) -> Result<()> {
        if let Self::Failed {
            error,
            restart_required: true,
        } = self
        {
            return Err(crate::error::NativeCleanupIncomplete(anyhow::anyhow!("{error}")).into());
        }
        Ok(())
    }
}

struct Configuration {
    config: EnsembleConfig,
    saved: Option<Vec<u8>>,
    setup: SetupState,
}

struct Active {
    runtime: Arc<EnsembleRuntime>,
    router: Router,
}

pub struct Workspace {
    path: PathBuf,
    _config_lock: crate::file_lease::FileLease,
    queue_enabled: bool,
    configuration: Mutex<Configuration>,
    active: RwLock<Option<Active>>,
    operation: Mutex<Option<tokio::task::JoinHandle<()>>>,
    queue: Option<crate::meta_runtime::MetaRuntimeRegistry>,
    console: std::sync::OnceLock<Arc<crate::console::ConsoleHub>>,
    #[cfg(test)]
    client: Option<Arc<dyn meerkat::LlmClient>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddRepository {
    pub id: RepoId,
    pub path: PathBuf,
}

#[derive(Serialize)]
pub struct WorkspaceStatus {
    name: String,
    repositories: BTreeMap<RepoId, RepositoryConfig>,
    setup: SetupState,
    runtime: EnsembleStatus,
}

impl Workspace {
    /// An absent file is an empty workspace, persisted on the first enrollment.
    pub fn open(
        path: PathBuf,
        queue_enabled: bool,
        model: Option<String>,
        auth_binding: Option<meerkat_core::AuthBindingRef>,
        queue: Option<crate::meta_runtime::MetaRuntimeRegistry>,
    ) -> Result<Arc<Self>> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .context("repository configuration needs a parent directory")?;
        std::fs::create_dir_all(parent)?;
        let path = if path.exists() {
            path.canonicalize()?
        } else {
            parent
                .canonicalize()?
                .join(path.file_name().context("configuration needs a filename")?)
        };
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let config_lock = options.open(path.with_extension("yaml.lock"))?;
        let config_lock = crate::file_lease::FileLease::acquire(config_lock)
            .context("another Fugue instance owns this repository configuration")?;
        let saved = match std::fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("read repository configuration"),
        };
        let config = if saved.is_some() {
            EnsembleConfig::load(&path)?
        } else {
            let mut config = EnsembleConfig::for_repositories(&[], "My repositories".into())?;
            config.state_root = path.with_extension("state");
            if let Some(model) = model {
                config.model = model;
            }
            config.auth_binding = auth_binding;
            config
        };
        if queue_enabled && queue.is_none() && !config.repositories.is_empty() {
            bail!(
                "This configuration separates ticket agents from repository specialists. Prepare the repository settings again to bind them to one mob. Existing state has been preserved."
            );
        }
        Ok(Arc::new(Self {
            path,
            _config_lock: config_lock,
            queue_enabled,
            configuration: Mutex::new(Configuration {
                config,
                saved,
                setup: SetupState::Empty,
            }),
            active: RwLock::new(None),
            operation: Mutex::new(None),
            queue,
            console: std::sync::OnceLock::new(),
            #[cfg(test)]
            client: None,
        }))
    }

    #[cfg(test)]
    pub(crate) fn open_with_client(
        path: PathBuf,
        client: Arc<dyn meerkat::LlmClient>,
    ) -> Arc<Self> {
        let mut workspace = Self::open(path, false, None, None, None).unwrap();
        let inner = Arc::get_mut(&mut workspace).unwrap();
        inner.queue_enabled = true;
        inner.client = Some(client);
        workspace
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let mut operation = self.operation.lock().await;
        if operation.as_ref().is_some_and(|task| !task.is_finished()) {
            bail!("Repository teams are still being prepared. Please wait for setup to finish.");
        }
        let mut state = self.configuration.lock().await;
        state.setup.ensure_cleanup_complete()?;
        if state.setup == SetupState::Stopped {
            bail!("Workspace is shutting down");
        }
        if state.setup == SetupState::Ready {
            bail!("Repository teams are already ready");
        }
        if state.config.repositories.is_empty() {
            return Ok(());
        }
        state.setup = SetupState::Preparing;
        let config = state.config.clone();
        let previous = self.active.write().await.take();
        let workspace = Arc::clone(self);
        *operation = Some(tokio::spawn(async move {
            workspace.prepare(config, previous).await;
        }));
        Ok(())
    }

    pub fn use_console(&self, console: Arc<crate::console::ConsoleHub>) -> Result<()> {
        self.console
            .set(console)
            .map_err(|_| anyhow::anyhow!("console is already attached"))
    }

    fn console_key(&self, runtime: &EnsembleRuntime, repo: &RepoId) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "{repo}-{:x}",
            Sha256::digest(
                runtime
                    .config()
                    .state_root
                    .join(repo.as_str())
                    .as_os_str()
                    .as_encoded_bytes()
            )
        )
    }

    pub async fn await_ready(&self) -> Result<()> {
        loop {
            match &self.configuration.lock().await.setup {
                SetupState::Ready => return Ok(()),
                SetupState::Failed {
                    error,
                    restart_required: true,
                } => {
                    return Err(crate::error::NativeCleanupIncomplete(anyhow::anyhow!(
                        "repository formation failed: {error}"
                    ))
                    .into());
                }
                SetupState::Failed {
                    error,
                    restart_required: false,
                } => bail!("repository formation failed: {error}"),
                SetupState::Preparing => {}
                _ => bail!("repository formation did not produce a ready mob"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    async fn prepare(&self, config: EnsembleConfig, previous: Option<Active>) {
        let result = self.replace_runtime(config, previous).await;
        self.complete_preparation(result).await;
    }

    pub(crate) async fn complete_preparation(&self, result: Result<()>) {
        self.configuration.lock().await.setup = match result {
            Ok(()) => SetupState::Ready,
            Err(error) => {
                tracing::error!(error = %error, "repository setup failed");
                SetupState::Failed {
                    error: format!("{error:#}"),
                    restart_required: error.is::<crate::error::NativeCleanupIncomplete>(),
                }
            }
        };
    }

    async fn replace_runtime(
        &self,
        config: EnsembleConfig,
        previous: Option<Active>,
    ) -> Result<()> {
        // Drain request leases before removing routes, then close native
        // producers before releasing their durable-state ownership lock.
        if let Some(previous) = previous {
            if let Err(error) = previous.runtime.shutdown().await {
                *self.active.write().await = Some(previous);
                return Err(error);
            }
            if let Some(console) = self.console.get() {
                for repo in previous.runtime.config().repositories.keys() {
                    console.unregister(&self.console_key(&previous.runtime, repo));
                }
            }
            drop(previous);
        }
        #[cfg(test)]
        let built =
            EnsembleRuntime::build_inner(config, self.client.clone(), self.queue.clone()).await?;
        #[cfg(not(test))]
        let built = match &self.queue {
            Some(queue) => EnsembleRuntime::build_for_queue(config, queue.clone()).await?,
            None => EnsembleRuntime::build(config).await?,
        };
        let runtime = Arc::new(built);
        let routes = if self.queue_enabled {
            super::http::integrated_router(Arc::clone(&runtime)).await
        } else {
            super::http::router(Arc::clone(&runtime)).await
        };
        match routes {
            Ok(router) => {
                if let Some(console) = self.console.get() {
                    for repo in runtime.config().repositories.keys() {
                        let native = runtime
                            .repository(repo)
                            .context("repository runtime missing")?;
                        console.register(self.console_key(&runtime, repo), native.clone());
                    }
                }
                *self.active.write().await = Some(Active { runtime, router });
                Ok(())
            }
            Err(error) => {
                runtime
                    .shutdown()
                    .await
                    .context("cleanup repository router failure")?;
                Err(error)
            }
        }
    }

    pub async fn add(self: &Arc<Self>, request: AddRepository) -> Result<()> {
        if self.queue.is_some() {
            bail!(
                "Add repositories through the workspace directory. Each repository needs its own tracker and queue."
            );
        }
        let mut operation = self.operation.lock().await;
        if operation.as_ref().is_some_and(|task| !task.is_finished()) {
            bail!("Repository teams are still being prepared. Please wait for setup to finish.");
        }
        let mut state = self.configuration.lock().await;
        state.setup.ensure_cleanup_complete()?;
        if state.setup == SetupState::Stopped {
            bail!("Workspace is shutting down");
        }
        let mut config = state.config.clone();
        validate_addition(&mut config, request)?;
        let mut active = self.active.write().await;
        if let Some(active) = active.as_ref() {
            active.runtime.ensure_idle_for_configuration().await?;
        }
        let saved = save_config(&self.path, state.saved.as_deref(), &config)?;
        let previous = active.take();
        state.config = config.clone();
        state.saved = Some(saved);
        state.setup = SetupState::Preparing;
        let workspace = Arc::clone(self);
        *operation = Some(tokio::spawn(async move {
            workspace.prepare(config, previous).await;
        }));
        Ok(())
    }

    pub async fn status(&self) -> Result<WorkspaceStatus> {
        let state = self.configuration.lock().await;
        let (name, repositories, setup) = (
            state.config.name.clone(),
            state.config.repositories.clone(),
            state.setup.clone(),
        );
        drop(state);
        let active = self.active.read().await;
        let runtime = match active.as_ref() {
            Some(active) => active.runtime.status().await?,
            None => EnsembleStatus {
                repositories: Vec::new(),
                peers: Vec::new(),
            },
        };
        Ok(WorkspaceStatus {
            name,
            repositories,
            setup,
            runtime,
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut operation = self.operation.lock().await;
        if let Some(task) = operation.take() {
            task.await.context("join repository setup")?;
        }
        {
            let mut configuration = self.configuration.lock().await;
            configuration.setup.ensure_cleanup_complete()?;
            configuration.setup = SetupState::Stopped;
        }
        let mut owned = self.active.write().await;
        if let Some(active) = owned.as_ref() {
            active.runtime.shutdown().await?;
            if let Some(console) = self.console.get() {
                for repo in active.runtime.config().repositories.keys() {
                    console.unregister(&self.console_key(&active.runtime, repo));
                }
            }
        }
        owned.take();
        Ok(())
    }
}

fn validate_addition(config: &mut EnsembleConfig, request: AddRepository) -> Result<()> {
    if config.repositories.contains_key(&request.id) {
        bail!("A repository with this name already exists");
    }
    if !request.path.is_absolute() {
        bail!("Enter an absolute path to an existing Git checkout on this machine");
    }
    config.repositories.insert(
        request.id,
        RepositoryConfig {
            path: request.path,
            description: None,
        },
    );
    config.resolve_and_validate(&std::env::current_dir()?)
}

fn save_config(path: &Path, expected: Option<&[u8]>, config: &EnsembleConfig) -> Result<Vec<u8>> {
    use std::io::Write;
    let current = match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("read repository configuration before saving"),
    };
    if current.as_deref() != expected {
        bail!(
            "Repository configuration changed on disk. Restart Fugue to load it before adding a repository."
        );
    }
    let parent = path
        .parent()
        .context("repository configuration needs a parent directory")?;
    std::fs::create_dir_all(parent)?;
    let bytes = serde_yaml::to_string(config)?.into_bytes();
    let mut pending = tempfile::Builder::new()
        .prefix(".fugue-ensemble-")
        .tempfile_in(parent)?;
    pending.write_all(&bytes)?;
    pending.as_file().sync_all()?;
    pending.persist(path)?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(bytes)
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}
pub(super) fn error_response(status: StatusCode, error: impl ToString) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
        .into_response()
}

pub async fn status(State(workspace): State<Arc<Workspace>>) -> Response {
    match workspace.status().await {
        Ok(status) => Json(status).into_response(),
        Err(error) => error_response(StatusCode::SERVICE_UNAVAILABLE, error),
    }
}

pub async fn add(
    State(workspace): State<Arc<Workspace>>,
    Json(request): Json<AddRepository>,
) -> Response {
    match workspace.add(request).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(SetupState::Preparing)).into_response(),
        Err(error) => error_response(StatusCode::CONFLICT, format!("{error:#}")),
    }
}

pub async fn retry(State(workspace): State<Arc<Workspace>>) -> Response {
    if !matches!(
        workspace.configuration.lock().await.setup,
        SetupState::Failed { .. }
    ) {
        return error_response(StatusCode::CONFLICT, "Only failed setup can be retried");
    }
    match workspace.start().await {
        Ok(()) => (StatusCode::ACCEPTED, Json(SetupState::Preparing)).into_response(),
        Err(error) => error_response(StatusCode::CONFLICT, error),
    }
}

/// Delegate all conversation semantics and streaming to the native router.
pub async fn dispatch(State(workspace): State<Arc<Workspace>>, request: Request) -> Response {
    if workspace.configuration.lock().await.setup != SetupState::Ready {
        if request.method() == axum::http::Method::GET && request.uri().path().ends_with("/console")
        {
            return Redirect::to("/ensemble").into_response();
        }
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Repository setup is not ready. Open Repository setup for details.",
        );
    }
    let active = workspace.active.read().await;
    match active.as_ref() {
        Some(active) => match active
            .router
            .clone()
            .oneshot(forward_request(request))
            .await
        {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Repository teams are being prepared. Return to Repositories for setup status.",
        ),
    }
}

fn forward_request(request: Request) -> Request {
    // A nested native router must parse its own path parameters. Carrying
    // axum's outer route captures adds a second `repo` and breaks extraction.
    let (parts, body) = request.into_parts();
    let mut forwarded = Request::new(body);
    *forwarded.method_mut() = parts.method;
    *forwarded.uri_mut() = parts.uri;
    *forwarded.version_mut() = parts.version;
    *forwarded.headers_mut() = parts.headers;
    forwarded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrollment_validates_before_persistence_and_preserves_peer_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            std::process::Command::new("git")
                .args(["init", "-q"])
                .arg(dir.path())
                .status()
                .unwrap()
                .success()
        );
        let mut config = EnsembleConfig::for_repositories(&[], "Test".into()).unwrap();
        let request = || AddRepository {
            id: RepoId::try_from("example".to_string()).unwrap(),
            path: dir.path().to_path_buf(),
        };
        validate_addition(&mut config, request()).unwrap();
        assert!(config.peers.is_empty());
        assert!(validate_addition(&mut config.clone(), request()).is_err());
        assert!(
            validate_addition(
                &mut config.clone(),
                AddRepository {
                    id: RepoId::try_from("alias".to_string()).unwrap(),
                    path: dir.path().to_path_buf()
                }
            )
            .is_err()
        );
        assert!(
            validate_addition(
                &mut config.clone(),
                AddRepository {
                    id: RepoId::try_from("relative".to_string()).unwrap(),
                    path: "relative".into()
                }
            )
            .is_err()
        );
        let path = dir.path().join("private/ensemble.yaml");
        let saved = save_config(&path, None, &config).unwrap();
        assert_eq!(EnsembleConfig::load(&path).unwrap().repositories.len(), 1);
        std::fs::write(&path, "external edit").unwrap();
        assert!(save_config(&path, Some(&saved), &config).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "external edit");
    }

    #[tokio::test]
    async fn empty_workspace_is_available_without_starting_model_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workspace.yaml");
        let workspace = Workspace::open(path.clone(), true, None, None, None).unwrap();
        assert!(Workspace::open(path.clone(), true, None, None, None).is_err());
        workspace.start().await.unwrap();
        let status = workspace.status().await.unwrap();
        assert_eq!(status.setup, SetupState::Empty);
        assert!(status.repositories.is_empty());
        assert!(!path.exists());
        workspace.shutdown().await.unwrap();
        assert!(workspace.start().await.is_err());
    }

    #[tokio::test]
    async fn configuration_owner_releases_lock_with_a_fork_style_descriptor_copy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workspace.yaml");
        let owner = Workspace::open(path.clone(), true, None, None, None).unwrap();
        let inherited = owner._config_lock.duplicate_descriptor_for_test().unwrap();
        drop(owner);
        let next = Workspace::open(path.clone(), true, None, None, None).unwrap();
        drop(inherited);
        assert!(Workspace::open(path.clone(), true, None, None, None).is_err());
        drop(next);
        assert!(Workspace::open(path, true, None, None, None).is_ok());
    }

    #[test]
    fn crashed_configuration_writer_does_not_block_next_save() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ensemble.yaml");
        let mut config = EnsembleConfig::for_repositories(&[], "Original".into()).unwrap();
        let original = save_config(&path, None, &config).unwrap();
        std::fs::write(path.with_extension("yaml.pending"), "interrupted write").unwrap();
        config.name = "Retry".into();
        save_config(&path, Some(&original), &config).unwrap();
        assert_eq!(EnsembleConfig::load(&path).unwrap().name, "Retry");
    }
}
