//! Persistent repository teams, connected through native representative peers.
pub mod cli;
pub mod config;
pub mod discovery;
pub mod formation_review;
mod handoff;
pub mod http;
pub mod notifications;
pub mod repository_tools;
pub mod runtime;
mod task_contract;
pub mod team;
pub mod team_discovery;
pub mod workspace;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod workflow_tests;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, bail};

/// Serve the local operator surface and native consoles for all repository mobs.
pub async fn serve(config: PathBuf, bind: SocketAddr) -> anyhow::Result<()> {
    serve_with_directory(config, bind, None).await
}

pub(crate) async fn serve_with_directory(
    config: PathBuf,
    bind: SocketAddr,
    directory_root: Option<PathBuf>,
) -> anyhow::Result<()> {
    if !bind.ip().is_loopback() {
        bail!("Ensemble is a local operator surface; --host must be a loopback address");
    }
    // Reserve the listener before creating persistent agents, so a port conflict
    // cannot leave an otherwise healthy ensemble running without its operator UI.
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let runtime = workspace::Workspace::open(config, false, None, None, None)?;
    let mut app = match http::workspace_router(Arc::clone(&runtime), false) {
        Ok(app) => app
            .route(
                "/api/v1/build",
                axum::routing::get(crate::build_identity::endpoint),
            )
            .route(
                "/",
                axum::routing::get(|| async { axum::response::Redirect::temporary("/ensemble") }),
            ),
        Err(error) => {
            runtime
                .shutdown()
                .await
                .context("cleanup after router failure")?;
            return Err(error);
        }
    };
    let directory = if let Ok(hub) = std::env::var("FUGUE_HUB_URL") {
        app = app.merge(crate::setup::directory::hub_router(hub)?);
        None
    } else {
        let root = match directory_root {
            Some(root) => root,
            None => crate::setup::default_state_dir()?
                .join("directories")
                .join(format!("ui-{}", address.port())),
        };
        let directory = crate::setup::directory::Directory::open(root, address.port())?;
        runtime.use_console(directory.console())?;
        let native = directory
            .console()
            .router(meerkat_meta::RuntimeDecisionState::local_console(
                meerkat_meta::ConsolePolicy {
                    require_app_auth: false,
                    ..Default::default()
                },
                None,
            ));
        app = app.fallback_service(native.layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                crate::http::inject_console_browser_compat_for_workspace(
                    next.run(request).await,
                    false,
                )
                .await
            },
        )));
        directory.restore().await?;
        app = app.merge(crate::setup::directory::router(Arc::clone(&directory)));
        Some(directory)
    };
    runtime.start().await?;
    println!(
        "Fugue is available at http://{address}/ensemble; repository teams prepare in the background."
    );
    let stop = tokio_util::sync::CancellationToken::new();
    let server = axum::serve(
        listener,
        app.layer(axum::middleware::from_fn(http::local_origin_guard)),
    )
    .with_graceful_shutdown(stop.clone().cancelled_owned())
    .into_future();
    tokio::pin!(server);
    let (result, signal_error) = tokio::select! {
        result = &mut server => (Some(result), None),
        signal = shutdown_signal() => {
            stop.cancel();
            (None, signal.err())
        }
    };
    // Shut native producers down before waiting for HTTP streams to finish.
    // An open console SSE connection must not hold durable-state shutdown hostage.
    let shutdown = runtime.shutdown().await;
    if let Some(directory) = directory {
        directory.shutdown().await?;
    }
    if let Some(result) = result {
        result.context("Ensemble HTTP server failed")?;
    } else {
        match tokio::time::timeout(std::time::Duration::from_secs(5), &mut server).await {
            Ok(result) => result.context("Ensemble HTTP server failed")?,
            Err(_) => tracing::debug!("closing remaining Ensemble HTTP streams after shutdown"),
        }
    }
    if let Some(error) = signal_error {
        return Err(error).context("Ensemble shutdown signal failed");
    }
    shutdown.context("Ensemble runtime shutdown failed")
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
