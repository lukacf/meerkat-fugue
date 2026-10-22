//! Managed workspace configuration is edited while its runtime is stopped.
use super::connections::{ConnectionStore, Connections};
use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    response::Html,
    routing::{get, post},
};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

struct Settings {
    _store: ConnectionStore,
    connections: Connections,
    restart: CancellationToken,
}

pub fn router(root: &Path, restart: CancellationToken) -> Result<Router> {
    let store = ConnectionStore::open(root)?;
    let connections = store.load()?;
    let state = Arc::new(Settings {
        _store: store,
        connections,
        restart,
    });
    let page = include_str!("settings.html").replace("__APP_CSS__", include_str!("../ui/app.css"));
    Ok(
        Router::new()
            .route(
                "/settings",
                get(move || {
                    let page = page.clone();
                    async move { Html(crate::http::browser_document(&page)) }
                }),
            )
            .route(
                "/api/setup/connections",
                get(|State(state): State<Arc<Settings>>| async move {
                    Json(state.connections.status())
                }),
            )
            .route(
                "/api/setup/reconfigure",
                post(|State(state): State<Arc<Settings>>| async move {
                    state.restart.cancel();
                    StatusCode::ACCEPTED
                }),
            )
            .with_state(state)
            .layer(middleware::from_fn(
                crate::ensemble::http::local_origin_guard,
            )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn settings_restart_uses_guarded_native_shutdown_signal() {
        let root = tempfile::tempdir().unwrap();
        let restart = CancellationToken::new();
        let app = router(root.path(), restart.clone()).unwrap();
        let request = |origin: &str| {
            Request::post("/api/setup/reconfigure")
                .header("Host", "localhost:8788")
                .header("Origin", origin)
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(request("https://untrusted.example"))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert!(!restart.is_cancelled());
        assert_eq!(
            app.oneshot(request("http://localhost:8788"))
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
        assert!(restart.is_cancelled());
    }
}
