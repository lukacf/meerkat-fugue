//! Repository services share the native console host while retaining their own
//! tracker configuration, credentials, HTTP tools, state, and shutdown scope.
use anyhow::{Context, Result};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
    root: &Path,
    port: u16,
    hub_url: String,
    console: Arc<crate::console::ConsoleHub>,
    stop: CancellationToken,
) -> Result<()> {
    let mut configure = false;
    loop {
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
        match run(root, port, &hub_url, &active, console.clone(), stop.clone()).await {
            Ok(()) if stop.is_cancelled() => return Ok(()),
            Ok(()) => configure = true,
            Err(error) => {
                tracing::error!(workspace = %root.display(), error = %error, "repository workspace failed");
                // Keep detailed diagnostics in the host log. The setup page must
                // never reflect upstream response bodies or secret config values.
                std::fs::write(
                    root.join("startup-error.txt"),
                    "The repository runtime could not start. Check the host log, then prepare the saved settings and start again.",
                )?;
                configure = true;
            }
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
    let result = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        _ = reconfigure.cancelled() => Ok(()),
        result = &mut server => result.context("serve repository HTTP"),
        result = &mut work => result.context("repository task failed").and_then(|result| result),
    };
    work_stop.cancel();
    server_stop.cancel();
    // Native producers settle before their runtime/state ownership is released.
    let settled = if !work.is_finished() {
        work.await
            .context("settle repository task")
            .and_then(|result| result)
    } else {
        Ok(())
    };
    let cleanup = workspace.shutdown().await;
    result.and(settled).and(cleanup)
}
