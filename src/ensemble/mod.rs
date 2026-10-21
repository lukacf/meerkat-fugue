//! Persistent repository teams, connected through native representative peers.
pub mod cli;
pub mod config;
pub mod discovery;
pub mod formation_review;
mod handoff;
pub mod http;
pub mod repository_tools;
pub mod runtime;
mod task_contract;
pub mod team;
pub mod team_discovery;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod workflow_tests;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, bail};
use config::EnsembleConfig;
use runtime::EnsembleRuntime;

/// Serve the local operator surface and native consoles for all repository mobs.
pub async fn serve(config: EnsembleConfig, bind: SocketAddr) -> anyhow::Result<()> {
    if !bind.ip().is_loopback() {
        bail!("Ensemble is a local operator surface; --host must be a loopback address");
    }
    // Reserve the listener before creating persistent agents, so a port conflict
    // cannot leave an otherwise healthy ensemble running without its operator UI.
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let runtime = Arc::new(EnsembleRuntime::build(config).await?);
    let app = match http::router(Arc::clone(&runtime)).await {
        Ok(app) => app,
        Err(error) => {
            runtime
                .shutdown()
                .await
                .context("cleanup after router failure")?;
            return Err(error);
        }
    };
    println!("Ensemble is ready at http://{address}");
    let stop = tokio_util::sync::CancellationToken::new();
    let server = axum::serve(listener, app)
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
