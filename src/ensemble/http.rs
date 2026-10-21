//! A thin operator surface over native runtime state and console projections.
use std::sync::Arc;

use anyhow::{Context, bail};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use meerkat_meta::{ConsolePolicy, RuntimeDecisionState};
use serde::{Deserialize, Serialize};

use super::{
    config::{EnsembleConfig, RepoId},
    runtime::{EnsembleAccepted, EnsembleRuntime, EnsembleStatus},
};

const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const CONSOLE_META_TAG: &str = "<meta name=\"meta-base-url\" content=\"\" />";
const ICON: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="16" fill="#b9efc4"/><path d="M8 11c6-5 10 5 16 0M8 16c6-5 10 5 16 0M8 21c6-5 10 5 16 0" fill="none" stroke="#193721" stroke-width="2"/></svg>"##;

pub async fn router(runtime: Arc<EnsembleRuntime>) -> anyhow::Result<Router> {
    Ok(routes(runtime, false)
        .await?
        .route("/", get(|| async { Redirect::temporary("/ensemble") }))
        .route("/api/v1/build", get(crate::build_identity::endpoint)))
}

/// Mount the same repository teams and native consoles in the regular Fugue app.
pub async fn integrated_router(runtime: Arc<EnsembleRuntime>) -> anyhow::Result<Router> {
    routes(runtime, true).await
}

async fn routes(runtime: Arc<EnsembleRuntime>, queue_enabled: bool) -> anyhow::Result<Router> {
    let page = crate::http::inject_app_chrome(
        include_str!("dashboard.html"),
        crate::http::AppPage::Ensemble,
        queue_enabled,
    )
    .context("Ensemble page has no app shell insertion point")?;
    let mut app = Router::new()
        .route(
            "/ensemble",
            get(move || {
                let page = page.clone();
                async move { Html(page) }
            }),
        )
        .route(
            "/favicon.ico",
            get(|| async { ([(header::CONTENT_TYPE, "image/svg+xml")], ICON) }),
        )
        .route("/api/ensemble", get(status))
        .route("/api/repos/{repo}/messages", post(ask))
        .with_state(Arc::clone(&runtime));

    // Use the packaged native console rather than a second conversation model.
    // Its supported base-url tag scopes every API request to the selected mob.
    let response = meerkat_meta::http_console::console_frontend_index_handler()
        .await
        .into_response();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    let template = String::from_utf8(bytes.to_vec()).context("native console HTML is not UTF-8")?;
    if !template.contains(CONSOLE_META_TAG) {
        bail!("native console no longer exposes its expected base-url configuration");
    }
    app = app
        .route(
            "/console/assets/console-app.js",
            get(meerkat_meta::http_console::console_frontend_app_js_handler),
        )
        .route(
            "/console/assets/console-app.css",
            get(meerkat_meta::http_console::console_frontend_app_css_handler),
        );
    for id in runtime.config().repositories.keys() {
        let native = runtime
            .repository(id)
            .context("configured repository has no native runtime")?;
        let prefix = format!("/api/repos/{id}/meta");
        let html = template
            .replace(
                CONSOLE_META_TAG,
                &format!("<meta name=\"meta-base-url\" content=\"{prefix}\" />"),
            )
            .replace(
                "<title>Meta Console</title>",
                &format!("<title>{id} · Ensemble console</title>"),
            );
        let html =
            crate::http::inject_app_chrome(&html, crate::http::AppPage::Console, queue_enabled)
                .context("native console has no app shell insertion point")?;
        app = app
            .nest(
                &prefix,
                native.build_console_json_router(RuntimeDecisionState::local_console(
                    ConsolePolicy {
                        require_app_auth: false,
                        ..ConsolePolicy::default()
                    },
                    None,
                )),
            )
            .route(
                &format!("/repos/{id}/console"),
                get(move || {
                    let html = html.clone();
                    async move { Html(html) }
                }),
            );
        native.reconcile_interrupted_console_sends().await;
    }
    Ok(app
        .layer(DefaultBodyLimit::max(MAX_MESSAGE_BYTES + 1024))
        .layer(middleware::from_fn(local_origin_guard)))
}

#[derive(Serialize)]
struct Overview<'a> {
    name: &'a str,
    repositories: &'a std::collections::BTreeMap<RepoId, super::config::RepositoryConfig>,
    runtime: EnsembleStatus,
}

async fn status(State(runtime): State<Arc<EnsembleRuntime>>) -> Response {
    match runtime.status().await {
        Ok(status) => {
            let EnsembleConfig {
                name, repositories, ..
            } = runtime.config();
            Json(Overview {
                name,
                repositories,
                runtime: status,
            })
            .into_response()
        }
        Err(error) => api_error(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AskRequest {
    message: String,
}

async fn ask(
    State(runtime): State<Arc<EnsembleRuntime>>,
    Path(repo): Path<String>,
    Json(request): Json<AskRequest>,
) -> Response {
    let id = match RepoId::try_from(repo) {
        Ok(id) => id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    if runtime.repository(&id).is_none() {
        return api_error(StatusCode::NOT_FOUND, "unknown repository".to_owned());
    }
    if request.message.trim().is_empty() || request.message.len() > MAX_MESSAGE_BYTES {
        return api_error(
            StatusCode::BAD_REQUEST,
            format!("message must contain between 1 and {MAX_MESSAGE_BYTES} bytes"),
        );
    }
    match runtime.ask(&id, &request.message).await {
        Ok(accepted) => (StatusCode::ACCEPTED, Json::<EnsembleAccepted>(accepted)).into_response(),
        Err(error) => api_error(StatusCode::CONFLICT, error.to_string()),
    }
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

fn api_error(status: StatusCode, error: String) -> Response {
    (status, Json(ApiError { error })).into_response()
}

/// Loopback listeners still receive browser requests from public websites.
/// Reject cross-origin requests and DNS-rebinding hosts, including on the
/// embedded native console. CLI requests without Origin remain supported.
async fn local_origin_guard(request: Request, next: Next) -> Response {
    if !is_local_request(request.headers()) {
        return api_error(
            StatusCode::FORBIDDEN,
            "local same-origin access required".into(),
        );
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static("frame-ancestors 'self'"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

fn is_local_request(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Ok(url) = reqwest::Url::parse(&format!("http://{host}")) else {
        return false;
    };
    let local = url.host_str().is_some_and(|hostname| {
        hostname == "localhost"
            || hostname
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !local || !url.username().is_empty() || url.password().is_some() || url.path() != "/" {
        return false;
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|value| value == "cross-site")
    {
        return false;
    }
    match headers.get(header::ORIGIN) {
        None => true,
        Some(origin) => origin
            .to_str()
            .ok()
            .and_then(|value| reqwest::Url::parse(value).ok())
            .is_some_and(|origin| origin.origin() == url.origin()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_surface_rejects_remote_browser_and_rebinding_requests() {
        for host in ["127.0.0.1:8788", "localhost:8788", "[::1]:8788"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, host.parse().unwrap());
            assert!(is_local_request(&headers));
            headers.insert(header::ORIGIN, format!("http://{host}").parse().unwrap());
            assert!(is_local_request(&headers));
            headers.insert(header::ORIGIN, "https://example.com".parse().unwrap());
            assert!(!is_local_request(&headers));
        }
        for host in ["example.com", "127.0.0.1.example.com", "user@localhost"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, host.parse().unwrap());
            assert!(!is_local_request(&headers));
        }
        assert!(!is_local_request(&HeaderMap::new()));
    }
}
