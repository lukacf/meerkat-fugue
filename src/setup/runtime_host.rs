//! Repository services share the native console host while retaining their own
//! tracker configuration, credentials, HTTP tools, state, and shutdown scope.
use crate::error::NativeCleanupIncomplete as HostRestartRequired;
use anyhow::{Context, Result};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct HostLifecycle {
    restart_required: std::sync::atomic::AtomicBool,
}

impl HostLifecycle {
    pub(crate) fn ensure_can_start(&self) -> Result<()> {
        if self
            .restart_required
            .load(std::sync::atomic::Ordering::Acquire)
        {
            anyhow::bail!(
                "Native cleanup is incomplete. Restart the Fugue host before resuming repositories."
            );
        }
        Ok(())
    }
}

fn finish_run(result: Result<()>, settled: Result<()>, cleanup: Result<()>) -> Result<()> {
    cleanup.map_err(HostRestartRequired)?;
    settled.map_err(HostRestartRequired)?;
    result
}

fn record_failure(root: &Path, host: &HostLifecycle, error: anyhow::Error) -> Result<()> {
    tracing::error!(workspace = %root.display(), error = %error, "repository workspace failed");
    let fatal = error.is::<HostRestartRequired>();
    if fatal {
        host.restart_required
            .store(true, std::sync::atomic::Ordering::Release);
    }
    // Only operator-safe diagnostics reach the setup page.
    std::fs::write(
        root.join("startup-error.txt"),
        if fatal {
            "Native cleanup is incomplete. Restart the Fugue host before resuming this repository."
        } else {
            "The repository runtime could not start. Check the host log, then prepare the saved settings and start again."
        },
    )?;
    if fatal {
        return Err(error);
    }
    Ok(())
}

enum RunDisposition {
    Stopped,
    Configure,
}

fn finish_attempt(
    root: &Path,
    host: &HostLifecycle,
    stop: &CancellationToken,
    result: Result<()>,
) -> Result<RunDisposition> {
    if let Err(error) = result {
        record_failure(root, host, error)?;
    }
    Ok(if stop.is_cancelled() {
        RunDisposition::Stopped
    } else {
        RunDisposition::Configure
    })
}

pub(crate) async fn serve(
    root: &Path,
    port: u16,
    hub_url: String,
    console: Arc<crate::console::ConsoleHub>,
    stop: CancellationToken,
    host: Arc<HostLifecycle>,
) -> Result<()> {
    let mut configure = false;
    loop {
        host.ensure_can_start()?;
        if stop.is_cancelled() {
            return Ok(());
        }
        let active = if configure {
            None
        } else {
            super::managed::ActiveWorkspace::load(root)?
        };
        let Some(active) = active else {
            super::web::serve_managed(root, port, &hub_url, stop.clone()).await?;
            configure = false;
            continue;
        };
        let result = run(root, port, &hub_url, &active, console.clone(), stop.clone()).await;
        match finish_attempt(root, &host, &stop, result)? {
            RunDisposition::Stopped => return Ok(()),
            RunDisposition::Configure => configure = true,
        }
    }
}

async fn run(
    root: &Path,
    port: u16,
    hub_url: &str,
    active: &super::managed::ActiveWorkspace,
    console: Arc<crate::console::ConsoleHub>,
    stop: CancellationToken,
) -> Result<()> {
    let connections = {
        let store = super::connections::ConnectionStore::open(root)?;
        Arc::new(store.load()?)
    };
    let manager =
        crate::config::WorkflowConfigManager::load_scoped(active.workflow(root), connections)?;
    manager.validate_dispatch()?;
    if manager.effective().meerkat.repository_runtime.is_none() {
        anyhow::bail!(
            "Legacy split-runtime configuration must be prepared again before starting a shared repository mob. Existing state has been preserved."
        );
    }
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let (refresh_tx, refresh_rx) = tokio::sync::mpsc::channel(8);
    let mut orchestrator = crate::orchestrator::Orchestrator::new(manager, refresh_rx);
    let registry = orchestrator.meta_runtime_registry();
    registry
        .use_console(console.clone())
        .map_err(anyhow::Error::msg)?;
    let config = orchestrator.config().effective().clone();
    let workspace = crate::ensemble::workspace::Workspace::open(
        active.ensemble(root),
        true,
        config.meerkat.model.clone(),
        config.meerkat.auth_binding.clone(),
        Some(registry),
    )?;
    workspace.use_console(console)?;
    let reconfigure = CancellationToken::new();
    let redirect = format!("{hub_url}/console");
    let routes = crate::ensemble::http::workspace_router(workspace.clone(), true)?
        .merge(super::settings::router(root, reconfigure.clone())?)
        .merge(super::directory::hub_router(hub_url.to_owned())?)
        .route(
            "/console",
            axum::routing::get(move || {
                let redirect = redirect.clone();
                async move { axum::response::Redirect::temporary(&redirect) }
            }),
        );
    let app = crate::http::router(
        orchestrator.shared_state(),
        config,
        refresh_tx,
        orchestrator.internal_sender(),
        orchestrator.meta_runtime_registry(),
        Some(routes),
    )
    .await?;
    workspace.start().await?;
    let server_stop = CancellationToken::new();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(server_stop.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let work_stop = CancellationToken::new();
    let owner = workspace.clone();
    let run_stop = work_stop.clone();
    let mut work = tokio::spawn(async move {
        tokio::select! {
            result = owner.await_ready() => result?,
            _ = run_stop.cancelled() => return Ok(()),
        }
        orchestrator.run_until_shutdown(run_stop).await
    });
    let mut joined = false;
    let result = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        _ = reconfigure.cancelled() => Ok(()),
        result = &mut server => result.context("serve repository HTTP"),
        result = &mut work => { joined = true; result.context("repository task failed").and_then(|result| result) },
    };
    work_stop.cancel();
    server_stop.cancel();
    // Native producers settle before their runtime/state ownership is released.
    let settled = if !joined {
        work.await
            .context("settle repository task")
            .and_then(|result| result)
    } else {
        Ok(())
    };
    let cleanup = workspace.shutdown().await;
    finish_run(result, settled, cleanup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_preparation_cleanup_survives_workspace_readiness_and_host_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let workspace = crate::ensemble::workspace::Workspace::open(
            root.path().join("ensemble.yaml"),
            true,
            None,
            None,
            None,
        )
        .unwrap();
        // Inject a failed native build cleanup at the preparation completion
        // boundary. No Active runtime exists, which previously erased the fault.
        workspace
            .complete_preparation(Err(anyhow::Error::new(HostRestartRequired(
                anyhow::anyhow!("injected partial-build native cleanup refusal"),
            ))
            .context("repository initialization failed")))
            .await;
        let readiness = workspace.await_ready().await;
        assert!(readiness.as_ref().unwrap_err().is::<HostRestartRequired>());
        assert!(
            workspace
                .start()
                .await
                .unwrap_err()
                .is::<HostRestartRequired>()
        );
        let cleanup = workspace.shutdown().await;
        assert!(cleanup.as_ref().unwrap_err().is::<HostRestartRequired>());
        let host = HostLifecycle::default();
        let stop = CancellationToken::new();
        stop.cancel();
        let result = finish_run(readiness, Ok(()), cleanup);
        assert!(finish_attempt(root.path(), &host, &stop, result).is_err());
        assert!(host.ensure_can_start().is_err());
        assert!(
            std::fs::read_to_string(root.path().join("startup-error.txt"))
                .unwrap()
                .contains("Restart the Fugue host")
        );
    }

    #[test]
    fn cleanup_failure_survives_cancellation_and_disables_same_host_reopen() {
        for stopped in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let host = HostLifecycle::default();
            let stop = CancellationToken::new();
            if stopped {
                stop.cancel();
            }
            let error = finish_run(
                Ok(()),
                Ok(()),
                Err(anyhow::anyhow!("retained native owner")),
            )
            .unwrap_err();
            assert!(finish_attempt(root.path(), &host, &stop, Err(error)).is_err());
            assert!(host.ensure_can_start().is_err());
            assert_eq!(stop.is_cancelled(), stopped);
            assert!(
                !std::fs::read_to_string(root.path().join("startup-error.txt"))
                    .unwrap()
                    .contains("retained native owner")
            );
        }
    }

    #[test]
    fn cleanup_failure_takes_precedence_over_recoverable_run_error() {
        let result = finish_run(
            Err(anyhow::anyhow!("startup")),
            Ok(()),
            Err(anyhow::anyhow!("cleanup")),
        );
        assert!(result.unwrap_err().is::<HostRestartRequired>());
        let root = tempfile::tempdir().unwrap();
        let host = HostLifecycle::default();
        let result = finish_run(Err(anyhow::anyhow!("startup")), Ok(()), Ok(()));
        record_failure(root.path(), &host, result.unwrap_err()).unwrap();
        host.ensure_can_start().unwrap();
    }
}
