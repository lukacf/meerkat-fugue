use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::extract::{FromRequest, Multipart, Path as AxumPath, State};
use axum::http::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Json, Router};
use base64::Engine;
use chrono::Utc;
use indexmap::IndexMap;
#[cfg(test)]
use meerkat::{SqliteWorkGraphStore, WorkGraphService, WorkItemFilter, WorkNamespace};
use meerkat_meta::ConsolePolicy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use sha2::Digest;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::config::ServiceConfig;
use crate::deploy_gate::{
    DEPLOY_OUTCOME_SCHEMA_VERSION, DeployBinaryHash, DeployFailureCategory, DeployGateInput,
    DeployOtaEvidence, DeployOutcome, DeployOutcomeStatus, DeployPullRequestEvidence,
    DeployStartRequest, plan_deploy_gate,
};
use crate::domain::Issue;
use crate::final_pr_gate::FinalPrGateStartRequest;
#[cfg(test)]
use crate::independent_review::{IndependentReviewAggregationInput, aggregate_independent_review};
use crate::independent_review::{
    IndependentReviewCompletionRequest, IndependentReviewStartRequest,
};
use crate::linear_adapter::IssueAdapter;
use crate::meta_runtime::MetaRuntimeRegistry;
#[cfg(test)]
use crate::meta_runtime::meta_runtime_state_path;
use crate::orchestrator::{
    InternalMessage, RetryRow, RunningRow, SharedState, issue_snapshot, snapshot,
};
use crate::ota_update::{OtaUpdateReport, OtaUpdateStatus, run_ota_update};
use crate::tools::{
    AddCommentInput, FugueToolOutput, GitSha, IdempotencyKey, InspectIssueInput,
    LinearCommentAttachmentInput, LinearIssueCommentSnapshot, LinearIssueIdentifier,
    LinearIssueRef, LinearStateRef, MergePrOutput, MoveIssueInput, UpsertWorkpadInput,
};

const META_RPC_TCP_ADDR_ENV: &str = "FUGUE_META_RPC_TCP_ADDR";
const META_RPC_BEARER_TOKEN_ENV: &str = "FUGUE_META_RPC_BEARER_TOKEN";

#[derive(Clone)]
struct AppState {
    state: SharedState,
    config: ServiceConfig,
    refresh_tx: mpsc::Sender<()>,
    internal_tx: mpsc::Sender<InternalMessage>,
    meta_runtime_registry: MetaRuntimeRegistry,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployTransactionResult {
    pub accepted: bool,
    pub issue_identifier: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<DeployFailureCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<crate::deploy_gate::DeployGatePlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<DeployOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ota: Option<OtaUpdateReport>,
}

pub async fn serve(
    addr: SocketAddr,
    state: SharedState,
    config: ServiceConfig,
    refresh_tx: mpsc::Sender<()>,
    internal_tx: mpsc::Sender<InternalMessage>,
    meta_runtime_registry: MetaRuntimeRegistry,
    ensemble: Option<Router>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_on_listener(
        listener,
        state,
        config,
        refresh_tx,
        internal_tx,
        meta_runtime_registry,
        ensemble,
    )
    .await
}

pub async fn serve_on_listener(
    listener: TcpListener,
    state: SharedState,
    config: ServiceConfig,
    refresh_tx: mpsc::Sender<()>,
    internal_tx: mpsc::Sender<InternalMessage>,
    meta_runtime_registry: MetaRuntimeRegistry,
    ensemble: Option<Router>,
) -> anyhow::Result<()> {
    let local_addr = listener.local_addr()?;
    let app = router(
        state,
        config,
        refresh_tx,
        internal_tx,
        meta_runtime_registry,
        ensemble,
    )
    .await?;
    let app = if local_addr.ip().is_loopback() {
        app.layer(middleware::from_fn(
            crate::ensemble::http::local_origin_guard,
        ))
    } else {
        app
    };
    tracing::info!(address = %local_addr, "http_status listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub(crate) async fn router(
    state: SharedState,
    config: ServiceConfig,
    refresh_tx: mpsc::Sender<()>,
    internal_tx: mpsc::Sender<InternalMessage>,
    meta_runtime_registry: MetaRuntimeRegistry,
    ensemble: Option<Router>,
) -> anyhow::Result<Router> {
    let managed_repository = config.meerkat.repository_runtime.is_some();
    let app_state = AppState {
        state,
        config: config.clone(),
        refresh_tx,
        internal_tx,
        meta_runtime_registry: meta_runtime_registry.clone(),
    };
    let app = Router::new()
        .route("/api/v1/build", get(crate::build_identity::endpoint))
        .route("/api/v1/workspace", get(workspace_context))
        .route(
            "/",
            get(|| async { axum::response::Redirect::temporary("/overview") }),
        )
        .route("/overview", get(dashboard))
        .route(
            "/queues",
            get(|| async { axum::response::Redirect::temporary("/issues") }),
        )
        .route("/issues", get(issues_board))
        .route("/issues/{issue_identifier}", get(issue_detail_page))
        .route("/api/v1/state", get(api_state))
        .route("/api/v1/issues/board", get(api_issue_board))
        .route("/api/v1/issues", post(api_issue_create))
        .route("/api/v1/issues/{issue_identifier}", get(api_issue_detail))
        .route(
            "/api/v1/issues/{issue_identifier}/comments",
            post(api_issue_add_comment)
                .layer(axum::extract::DefaultBodyLimit::max(101 * 1024 * 1024)),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/attachments",
            post(api_issue_upload_attachments)
                .layer(axum::extract::DefaultBodyLimit::max(101 * 1024 * 1024)),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/attachments/{file_name}",
            get(api_issue_attachment_download),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/state",
            patch(api_issue_move),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/workpad",
            put(api_issue_upsert_workpad),
        )
        .route("/api/v1/refresh", post(api_refresh))
        .route(
            "/api/v1/kapellmeister/start-issue",
            post(api_kapellmeister_start_issue),
        )
        .route(
            "/api/v1/kapellmeister/start-final-pr-gate",
            post(api_kapellmeister_start_final_pr_gate),
        )
        .route(
            "/api/v1/kapellmeister/start-independent-review",
            post(api_kapellmeister_start_independent_review),
        )
        .route(
            "/api/v1/kapellmeister/admit-independent-review",
            post(api_kapellmeister_admit_independent_review),
        )
        .route(
            "/api/v1/kapellmeister/complete-independent-review",
            post(api_kapellmeister_complete_independent_review),
        )
        .route(
            "/api/v1/kapellmeister/complete-final-pr-gate",
            post(api_kapellmeister_complete_final_pr_gate),
        )
        .route(
            "/api/v1/kapellmeister/deploy",
            post(api_kapellmeister_deploy),
        )
        .route("/api/v1/ota-update", post(api_ota_update))
        .route("/api/v1/meta/rpc", post(api_meta_rpc));
    let mut app = app.route("/api/v1/{issue_identifier}", get(api_issue));
    if meta_console_enabled(&config) {
        let decision_state = meta_console_decision_state()
            .map_err(|error| anyhow::anyhow!("build Meta console decision state: {error}"))?;
        let console_router = meta_runtime_registry
            .reference_console_router::<()>(decision_state)
            .await
            .map_err(|error| anyhow::anyhow!("build Meta reference console router: {error}"))?;
        let console_config = config.clone();
        app = app.fallback_service(console_router.layer(middleware::from_fn(
            move |request: Request, next: Next| {
                let console_config = console_config.clone();
                async move { authorize_meta_console_request(console_config, request, next).await }
            },
        )));
    }
    if ensemble.is_none() {
        app = app.route("/ensemble", get(ensemble_page));
    }
    let mut app = app.with_state(app_state);
    if let Some(routes) = ensemble {
        app = app.merge(
            routes.layer(middleware::from_fn(move |request: Request, next: Next| {
                let config = config.clone();
                async move {
                    if let Err(response) = authorize_console_browser(&config, request.headers()) {
                        return response;
                    }
                    next.run(request).await
                }
            })),
        );
    }
    if managed_repository {
        app = app.layer(middleware::from_fn(
            crate::ensemble::http::local_origin_guard,
        ));
    }
    Ok(app)
}

async fn authorize_meta_console_request(
    config: ServiceConfig,
    request: Request,
    next: Next,
) -> Response {
    if let Err(response) = authorize_console_browser(&config, request.headers()) {
        return response;
    }
    inject_console_browser_compat(next.run(request).await).await
}

pub(crate) async fn inject_console_browser_compat(response: Response) -> Response {
    inject_console_browser_compat_for_workspace(response, true).await
}

pub(crate) async fn inject_console_browser_compat_for_workspace(
    response: Response,
    queue_enabled: bool,
) -> Response {
    let is_html = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| content_type.starts_with("text/html"));
    if !is_html {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(%error, "failed to read Meta console HTML response");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read Meta console HTML response",
            )
                .into_response();
        }
    };
    let Ok(html) = String::from_utf8(bytes.to_vec()) else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    let html = inject_console_browser_compat_html(&html, queue_enabled);
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(html))
}

fn inject_console_browser_compat_html(html: &str, queue_enabled: bool) -> String {
    let mut patched = html.to_string();

    if let Some(next) = inject_random_uuid_polyfill(&patched) {
        patched = next;
    }
    if let Some(next) = inject_app_chrome(&patched, AppPage::Console, queue_enabled) {
        patched = next;
    }
    patched
}

fn inject_random_uuid_polyfill(html: &str) -> Option<String> {
    const MARKER: &str = "data-fugue-random-uuid-polyfill";
    const SCRIPT: &str = r#"<script data-fugue-random-uuid-polyfill>
(() => {
  const cryptoRef = globalThis.crypto;
  if (!cryptoRef || typeof cryptoRef.randomUUID === "function") {
    return;
  }
  if (typeof cryptoRef.getRandomValues !== "function") {
    return;
  }
  Object.defineProperty(cryptoRef, "randomUUID", {
    configurable: true,
    value: () => {
      const bytes = new Uint8Array(16);
      cryptoRef.getRandomValues(bytes);
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
      return `${hex[0]}${hex[1]}${hex[2]}${hex[3]}-${hex[4]}${hex[5]}-${hex[6]}${hex[7]}-${hex[8]}${hex[9]}-${hex[10]}${hex[11]}${hex[12]}${hex[13]}${hex[14]}${hex[15]}`;
    },
  });
})();
</script>"#;
    if html.contains(MARKER) {
        return None;
    }
    let insertion = html
        .find("<head>")
        .map(|index| index + "<head>".len())
        .or_else(|| html.find("<html>").map(|index| index + "<html>".len()))?;
    let mut patched = String::with_capacity(html.len() + SCRIPT.len());
    patched.push_str(&html[..insertion]);
    patched.push_str(SCRIPT);
    patched.push_str(&html[insertion..]);
    Some(patched)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppPage {
    Home,
    Work,
    Console,
    Ensemble,
    Settings,
    Setup,
}

pub(crate) fn inject_app_chrome(
    html: &str,
    active: AppPage,
    queue_enabled: bool,
) -> Option<String> {
    const MARKER: &str = "data-fugue-console-chrome";
    let style = format!(
        "<style data-fugue-console-chrome>{}</style>",
        include_str!("ui/app.css")
    );
    let mut nav = String::new();
    for (page, href, label) in [
        (AppPage::Home, "/overview", "Home"),
        (AppPage::Work, "/queues", "Board"),
        (AppPage::Console, "/console", "Agents"),
    ] {
        let selected = if page == active {
            " class=\"active\" aria-current=\"page\""
        } else {
            ""
        };
        nav.push_str(&format!(r#"<a href="{href}"{selected}>{label}</a>"#));
    }
    let account = crate::browser_access::logout_url()
        .map(|logout| format!(r#"<a href="{logout}">Sign out of this browser</a>"#))
        .unwrap_or_else(|| "<p>Access is managed by this Fugue host.</p>".to_owned());
    let (area, title) = match active {
        AppPage::Home => ("home", "Home"),
        AppPage::Work => ("board", "Board"),
        AppPage::Console => ("agents", "Agents"),
        AppPage::Ensemble => ("repositories", "Repositories"),
        AppPage::Settings => ("settings", "Repository settings"),
        AppPage::Setup => ("setup", "Connect repository"),
    };
    let repositories_selected = if matches!(
        active,
        AppPage::Ensemble | AppPage::Settings | AppPage::Setup
    ) {
        " class=\"active\" aria-current=\"page\""
    } else {
        ""
    };
    let brand = include_str!("ui/brand.svg");
    let chrome = format!(
        r#"<div class="fugue-console-rail" data-fugue-console-chrome data-area="{area}"><a class="fugue-console-brand" href="/overview" aria-label="Meerkat Fugue home"><span class="fugue-console-mark">{brand}</span><span class="fugue-wordmark"><small>Meerkat</small><span>Fugue</span></span></a><div class="fugue-scope"><label for="fugueScope">Repository</label><select id="fugueScope" aria-label="Repository scope"><option value="">All repositories</option></select></div><nav class="fugue-console-tabs" aria-label="Primary app navigation">{nav}</nav><div class="fugue-console-spacer"></div><a id="fugueRepositories" href="/ensemble"{repositories_selected}>Repositories</a><details class="fugue-account"><summary>Session</summary><div class="fugue-account-menu"><strong>Browser session</strong><p>Signing out leaves your teams running. Repository and AI credentials are managed in Repositories.</p><button class="fugue-button quiet" data-fugue-welcome>Getting started</button>{account}</div></details></div><div class="fugue-breadcrumb"><a id="fugueScopeCrumb" href="/overview">All repositories</a><span aria-hidden="true">/</span><span>{title}</span><a id="fugueRepoSettings" href="./settings" hidden>Repository settings</a></div>"#
    );

    if html.contains(MARKER) {
        return None;
    }

    // Scope page-owned links before adding the origin-wide primary navigation.
    let mut patched =
        browser_document(html).replace("__PORTFOLIO_JS__", include_str!("ui/portfolio.js"));
    let context_script = format!(
        "<script>{}</script>{}<script>{}</script>",
        include_str!("ui/navigation.js"),
        include_str!("ui/welcome.html"),
        include_str!("ui/welcome.js")
    );
    patched = patched.replace("</body>", &format!("{context_script}</body>"));
    if active == AppPage::Work {
        let script = format!("<script>{}</script>", include_str!("ui/work-context.js"));
        patched = patched.replace("</body>", &format!("{script}</body>"));
    }
    if !queue_enabled {
        patched = patched.replace(
            "<div class=\"queue-context\">",
            "<div class=\"queue-context\" hidden>",
        );
        patched = patched.replace("<body>", "<body data-queue-enabled=\"false\">");
    }
    let head_insertion = patched
        .find("</head>")
        .or_else(|| patched.find("<body"))
        .unwrap_or(0);
    patched.insert_str(head_insertion, &style);

    let Some(body_start) = patched.find("<body") else {
        return Some(format!("{style}{chrome}{patched}"));
    };
    let Some(body_tag_end_offset) = patched[body_start..].find('>') else {
        return Some(format!("{style}{chrome}{patched}"));
    };
    let body_tag_end = body_start + body_tag_end_offset;
    let body_tag = &patched[body_start..=body_tag_end];
    let mut replacement = if body_tag.contains("class=") {
        body_tag.replacen("class=\"", "class=\"fugue-console-embedded ", 1)
    } else {
        let mut replacement = body_tag.to_string();
        replacement.insert_str(replacement.len() - 1, " class=\"fugue-console-embedded\"");
        replacement
    };
    if !queue_enabled && !replacement.contains("data-queue-enabled=") {
        replacement.insert_str(replacement.len() - 1, " data-queue-enabled=\"false\"");
    }
    if !replacement.contains("data-bs-theme=") {
        replacement.insert_str(replacement.len() - 1, " data-bs-theme=\"paper\"");
    }
    patched.replace_range(body_start..=body_tag_end, &replacement);
    let body_tag_end = body_start + replacement.len();
    patched.insert_str(body_tag_end, &chrome);
    Some(patched)
}

pub(crate) fn browser_document(html: &str) -> String {
    let script = format!("<script>{}</script>", include_str!("ui/browser-paths.js"));
    html.replacen("<head>", &format!("<head>{script}"), 1)
        .replace("href=\"/", "href=\"./")
}

pub(crate) fn render_home_html(queue_enabled: bool) -> String {
    inject_app_chrome(
        include_str!("ui/overview.html"),
        AppPage::Home,
        queue_enabled,
    )
    .unwrap_or_else(|| include_str!("ui/overview.html").to_owned())
}

async fn dashboard(State(app): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Html(render_home_html(true)).into_response()
}

#[derive(Serialize)]
struct WorkspaceContext {
    tracker: String,
    project: Option<String>,
    repositories: Vec<String>,
}

async fn workspace_context(State(app): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Json(WorkspaceContext {
        tracker: app.config.tracker.kind.clone(),
        project: app.config.tracker.project_slug.clone(),
        repositories: app
            .config
            .kapellmeister
            .shadow_mode
            .authority
            .safe_selectors
            .repositories
            .iter()
            .map(|repo| format!("{}/{}/{}", repo.hostname(), repo.owner.0, repo.name.0))
            .collect(),
    })
    .into_response()
}

async fn issues_board(State(app): State<AppState>, headers: HeaderMap) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Html(render_issues_board_html(&app.config)).into_response()
}

const ISSUE_MARKDOWN_EDITOR_CLASS: &str = "issue-markdown-editor";

fn issue_markdown_editor_html(
    id: &str,
    name: Option<&str>,
    label: &str,
    placeholder: Option<&str>,
    value: &str,
) -> String {
    let name_attr = name
        .map(|name| format!(r#" name="{}""#, html_escape(name)))
        .unwrap_or_default();
    let placeholder_attr = placeholder
        .map(|placeholder| format!(r#" placeholder="{}""#, html_escape(placeholder)))
        .unwrap_or_default();
    format!(
        r#"<div class="form-field editor-field">
            <label for="{id}">{label}</label>
            <textarea id="{id}"{name_attr} class="{ISSUE_MARKDOWN_EDITOR_CLASS}" spellcheck="true"{placeholder_attr}>{value}</textarea>
          </div>"#,
        id = html_escape(id),
        label = html_escape(label),
        value = html_escape(value)
    )
}

fn create_issue_editor_html(lane_options: &str) -> String {
    let description_editor = issue_markdown_editor_html(
        "createDescription",
        Some("description"),
        "Description",
        Some("Describe the change, the expected result, and how to verify it."),
        "",
    );
    format!(
        r#"<form class="create-form issue-editor-form" id="createForm">
        <div class="create-primary-fields">
          <div class="form-field">
            <label for="createTitle">Title</label>
            <input id="createTitle" name="title" autocomplete="off" required>
          </div>
          <div class="form-field">
            <label for="createLane">Lane</label>
            <select id="createLane" name="lane">
              {lane_options}
            </select>
          </div>
        </div>
        <div class="create-editor-shell issue-editor-shell attachment-drop-zone" id="createAttachmentDropZone" data-attachment-drop-zone aria-label="Drop files to attach them to the new issue">
          {description_editor}
          <div class="create-editor-toolbar editor-actions">
            <label class="create-attachment-label ghost" for="createAttachments">
              <span>Attach files</span>
              <input id="createAttachments" name="attachments" type="file" multiple>
            </label>
            <span>Add screenshots, logs, or other files that help explain the issue.</span>
          </div>
          <div class="attachment-drop-affordance" aria-hidden="true">Drop files here to attach them</div>
          <div class="create-attachments" id="createAttachmentList" aria-live="polite"></div>
        </div>
        <div class="create-actions editor-actions">
          <div class="create-error" id="createError" role="alert"></div>
          <button class="primary" id="createSubmit" type="submit">Create issue</button>
          <button class="ghost" id="createCancel" type="button">Cancel</button>
        </div>
      </form>"#
    )
}

fn create_issue_lane_options(config: &ServiceConfig) -> String {
    issue_creation_lanes(config)
        .into_iter()
        .map(|lane| {
            let escaped = html_escape(&lane);
            format!(r#"<option value="{escaped}">{escaped}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n              ")
}

fn render_issues_board_html(config: &ServiceConfig) -> String {
    let lane_options = create_issue_lane_options(config);
    let html = ISSUES_BOARD_HTML
        .replace("__FUGUE_TRACKER_SCOPE__", &config.tracker_context_id())
        .replace(
            "__FUGUE_CREATE_EDITOR__",
            &create_issue_editor_html(&lane_options),
        );
    inject_app_chrome(&html, AppPage::Work, true).unwrap_or(html)
}

async fn issue_detail_page(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(_issue_identifier): AxumPath<String>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let html =
        ISSUE_DETAIL_HTML.replace("__FUGUE_TRACKER_SCOPE__", &app.config.tracker_context_id());
    Html(inject_app_chrome(&html, AppPage::Work, true).unwrap_or(html)).into_response()
}

async fn ensemble_page(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Html(
        inject_app_chrome(ENSEMBLE_LANDING_HTML, AppPage::Ensemble, true)
            .unwrap_or_else(|| ENSEMBLE_LANDING_HTML.to_owned()),
    )
    .into_response()
}

const ENSEMBLE_LANDING_HTML: &str = r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Meerkat Fugue · Ensemble</title></head><body><main style="max-width:760px;margin:64px auto;padding:24px;font-family:var(--bs-font-sans)"><h1>Repository teams</h1><p>No Ensemble is configured for this Fugue instance.</p><p>Start Fugue with <code>--ensemble-config /path/to/ensemble.yaml</code> to bring persistent repository specialists into this app.</p><a href="/overview">Return to overview</a></main></body></html>"#;

const ISSUES_BOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Work queue | Fugue</title>
  <style>
    #createSubmit[hidden] { display: none; }
    :root {
      color-scheme: light;
      --bs-paper: #ECEAE4;
      --bs-paper-soft: #F6F4EE;
      --bs-paper-raised: #FAF8F2;
      --bs-ink: #1C1A17;
      --bs-muted: #6B6760;
      --bs-subtle: #A8A49B;
      --bs-line: #D6D2C8;
      --bs-accent: #D95A1E;
      --bs-accent-dark: #E76A2F;
      --bs-ok: #3E6B4F;
      --bs-warn: #A56A18;
      --bs-crit: #A43F32;
      --bs-font-sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      --bs-font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      --bg: var(--bs-paper);
      --bg-sunken: var(--bs-paper-soft);
      --bg-elev-1: var(--bs-paper-raised);
      --bg-elev-2: var(--bs-paper-soft);
      --line: var(--bs-line);
      --line-strong: var(--bs-ink);
      --ink: var(--bs-ink);
      --ink-muted: var(--bs-muted);
      --ink-dim: var(--bs-subtle);
      --accent: var(--bs-accent);
      --accent-soft: color-mix(in oklch, var(--accent) 14%, var(--bg-elev-1));
      --ok: var(--bs-ok);
      --warn: var(--bs-warn);
      --crit: var(--bs-crit);
      --r-sm: 2px;
      --r-md: 6px;
      --surface: var(--bg-sunken);
      --panel: var(--bg-elev-1);
      --panel-raised: var(--bg-elev-1);
      --panel-hover: var(--bg-elev-2);
      --border: var(--line);
      --border-strong: var(--line-strong);
      --text: var(--ink);
      --muted: var(--ink-muted);
      --quiet: var(--ink-dim);
      --brand: var(--accent);
      --cyan: var(--accent);
      --green: var(--ok);
      --yellow: var(--warn);
      --red: var(--bs-crit);
      --orange: var(--accent-dark);

      --font-display: var(--bs-font-sans);
      --type-micro: 10px;
      --type-rail: 11px;
      --type-meta: 12px;
      --type-body: 13px;
      --type-card: 14px;
      --type-panel-heading: 15px;
      --type-heading: 16px;
      --line-compact: 1.35;
      --control-h: 38px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: var(--bs-font-sans);
      font-size: var(--type-body);
      line-height: var(--line-compact);
      letter-spacing: 0;
      overflow: hidden;
    }
    a { color: inherit; text-decoration: none; }
    button, input, textarea { font: inherit; }
    button {
      min-height: 32px;
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      background: var(--bg-elev-1);
      color: var(--muted);
      font-family: var(--bs-font-mono);
      font-size: var(--type-rail);
      font-weight: 650;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 0 12px;
    }
    button:hover { color: var(--text); border-color: var(--border-strong); background: var(--bg-elev-2); }
    button.primary { color: var(--bg); background: var(--accent); border-color: var(--accent); }
    button.ghost { background: transparent; }
    button.icon { width: 32px; padding: 0; font-size: var(--type-card); }
    button:disabled { cursor: not-allowed; opacity: 0.55; }
    .shell { height: 100vh; display: flex; flex-direction: column; min-width: 0; }
    header {
      height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      padding: 0 18px;
      border-bottom: 1px solid var(--line);
      background: var(--bg);
      z-index: 5;
    }
    .brand { display: flex; align-items: center; gap: 11px; min-width: 0; }
    .mark {
      width: 20px;
      height: 20px;
      border: 1.8px solid #1C1A17;
      border-radius: var(--r-sm);
      transform: rotate(45deg);
      flex: 0 0 auto;
    }
    h1 { margin: 0; font-family: var(--font-display); font-size: var(--type-heading); font-weight: 820; white-space: nowrap; text-transform: uppercase; letter-spacing: 0.08em; }
    nav, .actions { display: flex; gap: 8px; align-items: center; min-width: 0; }
    nav a {
      height: 34px;
      display: inline-flex;
      align-items: center;
      padding: 0 12px;
      border: 1px solid transparent;
      border-radius: var(--r-sm);
      color: var(--muted);
      font-family: var(--bs-font-mono);
      font-size: var(--type-rail);
      font-weight: 650;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    nav a.active { color: var(--text); background: var(--bg-elev-1); border-color: var(--line); }
    .command {
      width: min(420px, 38vw);
      height: 34px;
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--quiet);
      background: var(--bg);
      border: 1px solid var(--line);
      border-radius: var(--r-sm);
      padding: 0 12px;
    }
    .command input {
      width: 100%;
      border: 0;
      outline: 0;
      background: transparent;
      color: var(--text);
      min-width: 60px;
    }
    .kbd {
      min-width: 22px;
      height: 21px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid var(--line);
      border-radius: var(--r-sm);
      background: var(--bg);
      color: var(--muted);
      font-size: var(--type-rail);
      padding: 0 6px;
      font-weight: 700;
    }
    .toolbar {
      min-height: 54px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 10px 18px;
      border-bottom: 1px solid var(--line);
    }
    .create-panel {
      display: none;
      padding: 12px 18px;
      border-bottom: 1px solid var(--line);
      background: var(--bg-sunken);
    }
    .create-panel.open { display: block; }
    .create-form { max-width: 980px; }
    .create-primary-fields {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 180px;
      gap: 10px;
      align-items: end;
    }
    .form-field { display: grid; gap: 5px; min-width: 0; }
    .issue-editor-form { display: grid; gap: 12px; align-items: stretch; }
    .issue-editor-shell { border: 1px solid var(--border); border-radius: var(--r-sm); background: var(--bg-elev-1); padding: 10px; display: grid; gap: 10px; }
    .form-field label { color: var(--quiet); font-size: var(--type-rail); font-weight: 760; text-transform: uppercase; letter-spacing: 0.05em; }
    .form-field input, .form-field textarea {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      background: var(--bg);
      color: var(--text);
      outline: 0;
      padding: 0 10px;
    }
    .form-field input { height: 34px; }
    .form-field textarea {
      min-height: 220px;
      padding: 10px;
      resize: vertical;
      line-height: 1.45;
      font-family: var(--bs-font-mono);
      font-size: var(--type-meta);
    }
    .create-editor-toolbar { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 8px; color: var(--muted); font-size: var(--type-meta); }
    .create-attachment-label { display: inline-flex; align-items: center; gap: 8px; cursor: pointer; }
    .create-attachment-label input { position: absolute; opacity: 0; width: 1px; height: 1px; }
    .attachment-drop-zone { position: relative; transition: border-color 120ms ease, background 120ms ease; }
    .attachment-drop-zone.drag-over { border-color: var(--accent); background: color-mix(in oklch, var(--accent) 8%, var(--bg-elev-1)); }
    .attachment-drop-affordance { border: 1px dashed color-mix(in oklch, var(--accent) 45%, var(--border)); border-radius: var(--r-sm); background: color-mix(in oklch, var(--accent) 5%, transparent); color: var(--muted); padding: 8px 10px; font-size: var(--type-meta); text-align: center; }
    .attachment-drop-zone.drag-over .attachment-drop-affordance { color: var(--accent); background: color-mix(in oklch, var(--accent) 12%, transparent); }
    .create-attachments { display: flex; flex-wrap: wrap; gap: 6px; }
    .create-attachment-chip { border: 1px solid var(--border); border-radius: var(--r-sm); background: var(--bg-sunken); color: var(--muted); padding: 4px 7px; font-size: var(--type-meta); }
    .create-actions { display: flex; flex-wrap: wrap; justify-content: flex-end; align-items: center; gap: 10px; }
    .create-error { color: var(--crit, #A43F32); font-size: var(--type-body); min-height: 18px; margin-right: auto; }
    .summary { display: flex; align-items: center; gap: 14px; color: var(--muted); font-size: var(--type-body); overflow: hidden; }
    .metric { color: var(--text); font-weight: 750; font-variant-numeric: tabular-nums; }
    .updated { color: var(--quiet); white-space: nowrap; }
    .notice {
      position: fixed;
      right: 18px;
      bottom: 18px;
      max-width: min(520px, calc(100vw - 36px));
      border: 1px solid color-mix(in oklch, var(--crit, #A43F32) 35%, var(--line));
      background: color-mix(in oklch, var(--crit, #A43F32) 12%, var(--bg-elev-1));
      color: var(--crit, #A43F32);
      border-radius: var(--r-sm);
      padding: 12px 14px;
      display: none;
      z-index: 20;
      font-size: var(--type-body);
    }
    .workspace {
      min-height: 0;
      flex: 1;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 0 320px;
      transition: grid-template-columns 160ms ease;
    }
    .workspace.drawer-open { grid-template-columns: minmax(0, 1fr) minmax(420px, 34vw) 320px; }
    .board {
      display: grid;
      grid-auto-flow: column;
      grid-auto-columns: minmax(292px, 328px);
      gap: 10px;
      align-items: stretch;
      overflow: auto;
      padding: 12px 18px 18px;
      min-width: 0;
    }
    .lane {
      min-height: 100%;
      background: var(--bg-elev-1);
      border: 1px solid var(--line);
      border-top: 2px solid var(--line-strong);
      border-radius: var(--r-sm);
      padding: 10px;
      display: flex;
      flex-direction: column;
      transition: border-color 120ms ease, background 120ms ease;
    }
    .lane.drop-target {
      border-color: var(--accent);
      background: var(--accent-soft);
    }
    .lane-head {
      min-height: 38px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 0 2px 8px;
      color: var(--muted);
    }
    .lane-title { display: flex; align-items: center; gap: 9px; min-width: 0; }
    .lane-title strong { color: var(--text); font-size: var(--type-body); font-weight: 760; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .count { color: var(--quiet); font-size: var(--type-meta); font-variant-numeric: tabular-nums; }
    .dot { width: 12px; height: 12px; border: 2px solid var(--muted); border-radius: 50%; flex: 0 0 auto; }
    .lane[data-kind="parked"] .dot { border-color: var(--quiet); border-style: dotted; }
    .lane[data-kind="intake"] .dot { border-color: var(--yellow); }
    .lane[data-kind="active"] .dot { border-color: var(--cyan); }
    .lane[data-kind="terminal"] .dot { border-color: var(--green); }
    .cards { display: flex; flex-direction: column; gap: 8px; min-height: 96px; flex: 1; }
    .card {
      display: block;
      width: 100%;
      text-align: left;
      background: var(--bg-elev-1);
      border: 1px solid var(--line);
      border-radius: var(--r-sm);
      padding: 11px;
      color: var(--text);
      cursor: grab;
      user-select: none;
    }
    .card:hover { background: var(--bg-elev-2); border-color: var(--line-strong); }
    .card:active { cursor: grabbing; }
    .card.selected { outline: 2px solid var(--accent); border-color: var(--accent); }
    .card.multi-selected { background: var(--accent-soft); border-color: var(--accent); }
    .card.dragging { opacity: 0.48; }
    .keyrow { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 7px; }
    .key { color: var(--muted); font-size: var(--type-meta); font-weight: 760; letter-spacing: 0; }
    .status-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--quiet); flex: 0 0 auto; }
    .select-dot {
      width: 18px;
      height: 18px;
      border: 1px solid var(--border-strong);
      border-radius: var(--r-md);
      color: transparent;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: var(--type-meta);
      flex: 0 0 auto;
    }
    .card.multi-selected .select-dot { color: var(--ink); background: var(--brand); border-color: var(--brand); }
    .card[data-runtime="running"] .status-dot { background: var(--accent); }
    .card[data-runtime="retrying"] .status-dot { background: var(--orange); }
    .title { font-size: var(--type-card); line-height: 1.28; font-weight: 680; margin-bottom: 11px; overflow-wrap: anywhere; }
    .meta { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .pill {
      min-height: 23px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 0 8px;
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      color: var(--muted);
      background: var(--bg-sunken);
      font-size: var(--type-rail);
      line-height: 1;
      max-width: 100%;
    }
    .pill.run { color: var(--ink); border-color: var(--accent); background: var(--accent-soft); }
    .pill.retry { color: var(--warn); border-color: color-mix(in oklch, var(--warn) 45%, var(--line)); background: color-mix(in oklch, var(--warn) 12%, transparent); }
    .pill.blocked { color: var(--crit); border-color: color-mix(in oklch, var(--crit) 42%, var(--line)); }
    .footer { margin-top: 11px; color: var(--quiet); font-size: var(--type-meta); display: flex; justify-content: space-between; gap: 10px; }
    .empty {
      height: 86px;
      border: 1px dashed var(--line);
      border-radius: var(--r-sm);
      color: var(--ink-dim);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: var(--type-body);
    }
    .loading { padding: 34px 22px; color: var(--muted); }
    .drawer {
      min-width: 0;
      overflow: hidden;
      border-left: 1px solid var(--border);
      background: var(--bg-sunken);
      display: flex;
      flex-direction: column;
    }
    .drawer-inner { width: min(100%, 620px); min-width: 0; height: 100%; display: flex; flex-direction: column; }
    .drawer-head {
      min-height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 0 16px;
      border-bottom: 1px solid var(--border);
    }
    .drawer-title { min-width: 0; }
    .drawer-title .id { color: var(--muted); font-size: var(--type-meta); font-weight: 800; margin-bottom: 4px; }
    .drawer-title h2 { margin: 0; font-size: var(--type-panel-heading); line-height: 1.25; overflow-wrap: anywhere; }
    .drawer-body { min-height: 0; overflow: auto; padding: 14px 16px 24px; }
    .section { border-bottom: 1px solid var(--line); padding: 0 0 16px; margin-bottom: 16px; }
    .section-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 10px; }
    .section h3 { margin: 0; font-size: var(--type-meta); color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
    .description, .comment-body {
      color: var(--ink);
      background: var(--bg-elev-1);
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      padding: 11px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-size: var(--type-body);
      line-height: 1.45;
    }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .field { border: 1px solid var(--border); border-radius: var(--r-sm); padding: 9px 10px; background: var(--bg-elev-1); min-width: 0; }
    .field span { display: block; color: var(--quiet); font-size: var(--type-rail); margin-bottom: 4px; }
    .field strong { display: block; font-size: var(--type-body); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    select, textarea {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      background: var(--bg);
      color: var(--text);
      outline: 0;
    }
    select { height: 34px; padding: 0 10px; }
    textarea {
      min-height: 150px;
      resize: vertical;
      padding: 10px;
      line-height: 1.45;
      font-family: var(--bs-font-mono);
      font-size: var(--type-meta);
    }
    textarea:focus, input:focus, select:focus { border-color: var(--accent); }
    .editor-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px; }
    .comment-list { display: flex; flex-direction: column; gap: 8px; }
    .comment { border: 1px solid var(--border); background: var(--bg-elev-1); border-radius: var(--r-sm); overflow: hidden; }
    .comment-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 8px 10px; border-bottom: 1px solid var(--line); color: var(--muted); font-size: var(--type-meta); }
    .comment-body { border: 0; border-radius: 0; background: transparent; max-height: 260px; overflow: auto; }
    .hidden-panel {
      min-width: 0;
      overflow: auto;
      border-left: 1px solid var(--line);
      padding: 16px 18px;
      background: var(--bg-sunken);
    }
    .hidden-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-weight: 730;
      font-size: var(--type-body);
      margin: 0 0 14px;
    }
    .hidden-list { display: flex; flex-direction: column; gap: 10px; }
    .hidden-row {
      min-height: 42px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border: 1px solid var(--border);
      border-radius: var(--r-sm);
      background: var(--bg-elev-1);
      color: var(--text);
      padding: 0 12px;
      font-weight: 650;
    }
    .hidden-row .left { display: flex; align-items: center; gap: 10px; min-width: 0; }
    .hidden-row .left span:last-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .bulkbar {
      position: fixed;
      left: 50%;
      bottom: 22px;
      transform: translateX(-50%) translateY(120%);
      min-height: 50px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 7px 10px;
      border: 1px solid var(--border);
      border-radius: var(--r-md);
      background: var(--bg-elev-1);
      z-index: 18;
      opacity: 0;
      transition: transform 160ms ease, opacity 160ms ease;
    }
    .bulkbar.open { opacity: 1; transform: translateX(-50%) translateY(0); }
    .bulkbar-count { border: 1px solid var(--border); border-radius: var(--r-md); padding: 9px 13px; font-weight: 720; }
    .bulkbar-divider { width: 1px; height: 28px; background: var(--border); }
    .palette {
      position: fixed;
      inset: 0;
      display: none;
      align-items: flex-start;
      justify-content: center;
      padding-top: 150px;
      background: color-mix(in oklch, var(--ink) 18%, transparent);
      z-index: 26;
    }
    .palette.open { display: flex; }
    .palette-panel {
      width: min(720px, calc(100vw - 32px));
      max-height: min(640px, calc(100vh - 190px));
      border: 1px solid var(--border-strong);
      border-radius: var(--r-md);
      background: var(--bg-elev-1);
      overflow: hidden;
    }
    .palette-top { display: flex; align-items: center; gap: 12px; padding: 14px 16px; border-bottom: 1px solid var(--border); }
    .palette-count { border: 1px solid var(--border); border-radius: var(--r-md); padding: 5px 10px; color: var(--muted); font-weight: 720; }
    .palette-input {
      width: 100%;
      border: 0;
      outline: 0;
      background: transparent;
      color: var(--text);
      font-size: var(--type-body);
    }
    .palette-list { padding: 8px; }
    .palette-row {
      width: 100%;
      min-height: 46px;
      border: 0;
      border-radius: var(--r-sm);
      background: transparent;
      color: var(--ink);
      display: flex;
      align-items: center;
      justify-content: space-between;
      text-align: left;
      padding: 0 12px;
      font-size: var(--type-body);
    }
    .palette-row:hover, .palette-row.active { background: var(--bg-elev-2); color: var(--ink); }
    .palette-muted { color: var(--quiet); }
    .shortcuts {
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      background: color-mix(in oklch, var(--ink) 18%, transparent);
      z-index: 25;
    }
    .shortcuts.open { display: flex; }
    .shortcut-panel {
      width: min(520px, calc(100vw - 32px));
      border: 1px solid var(--border-strong);
      border-radius: var(--r-sm);
      background: var(--bg-elev-1);
      padding: 16px;
    }
    .shortcut-panel h2 { margin: 0 0 12px; font-size: var(--type-panel-heading); }
    .shortcut-row { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 8px 0; color: var(--muted); font-size: var(--type-body); border-top: 1px solid var(--line); }

    .top-rail { height: 38px; padding: 0; gap: 0; border-bottom: 1px solid var(--ink); background: var(--surface); }
    .rail-brand, .rail-tabs a, .rail-kpis span, .rail-refresh, .mono { font-family: var(--bs-font-mono); font-size: var(--type-rail); letter-spacing: .12em; text-transform: uppercase; }
    .rail-brand { height: 38px; display:flex; align-items:center; gap:10px; padding:0 16px; border-right:1px solid var(--line); background:var(--surface); font-weight:500; }
    .rail-brand .mark { width: 12px; height: 12px; }
    .rail-tabs { gap:0; align-self:stretch; }
    .rail-tabs a { height:38px; border:0; border-right:1px solid var(--line); border-radius:0; padding:0 18px; color:var(--ink); }
    .rail-tabs a.active { background:var(--ink); color:var(--bg); border-color:var(--ink); }
    .rail-spacer { flex:1 1 auto; }
    .rail-kpis { display:flex; height:38px; margin-left:auto; }
    .rail-kpis span { display:flex; align-items:center; gap:6px; padding:0 10px; border-left:1px solid var(--line); color:var(--muted); }
    .rail-kpis b { color:var(--ink); font-weight:700; font-variant-numeric:tabular-nums; }
    .rail-refresh { height:38px; min-height:38px; width:38px; border:0; border-left:1px solid var(--ink); border-radius:0; background:var(--surface); color:var(--ink); padding:0; }
    .lane-ribbon { height:40px; display:flex; align-items:stretch; border-bottom:1px solid var(--ink); background:var(--surface); overflow:hidden; }
    .lane-chip { border:0; border-right:1px solid var(--line); border-radius:0; background:transparent; color:var(--muted); min-height:40px; padding:0 12px; font-family:var(--bs-font-mono); font-size:var(--type-rail); letter-spacing:.1em; text-transform:uppercase; }
    .lane-chip[data-count]:not([data-count="0"]) { flex:1 1 0; color:var(--ink); }
    .lane-chip.active { background:var(--ink); color:var(--bg); }
    .lane-chip .chip-count { color:var(--accent); font-variant-numeric:tabular-nums; }
    .sub-toolbar { min-height:38px; height:38px; padding:0 0 0 14px; }
    .toolbar-actions { display:flex; align-items:stretch; height:38px; margin-left:auto; }
    .toolbar-actions button, .toolbar-actions .command { min-height:38px; height:38px; border-radius:0; border:0; border-left:1px solid var(--line); background:var(--surface); }
    .toolbar-actions button.active { background:var(--ink); color:var(--bg); }
    .toolbar-actions #newIssue { background:var(--accent); color:white; border-left-color:var(--accent); }
    .workspace, .workspace.drawer-open { grid-template-columns:minmax(0,1fr); }
    .hidden-panel { display:none; }
    .board { display:flex; gap:4px; align-items:stretch; overflow:hidden; padding:12px 12px 18px; }
    .lane { border-radius:0; padding:0; background:transparent; min-width:0; }
    .lane.full { flex:1 1 0; min-width:120px; margin-right:8px; border:1px solid var(--line); border-top:2px solid var(--ink); background:var(--bg-elev-1); }
    .lane.active-lane { border-top-color:var(--accent); }
    .lane.empty-strip { flex:0 0 32px; width:32px; margin-right:4px; opacity:.7; align-items:center; border:0; border-top:2px solid var(--line); background:transparent; }
    .lane.empty-strip .lane-head { display:block; min-height:30px; padding:8px 0; text-align:center; }
    .lane.empty-strip .rotated { writing-mode:vertical-rl; transform:rotate(180deg); font-family:var(--bs-font-mono); font-size:var(--type-rail); letter-spacing:.12em; color:var(--muted); padding:12px 0; white-space:nowrap; }
    .lane-head { min-height:42px; padding:0 10px; border-bottom:1px solid var(--line); }
    .lane-action-hint { padding:8px 10px; border-bottom:1px solid var(--line); color:var(--muted); font-family:var(--bs-font-mono); font-size:var(--type-micro); letter-spacing:.08em; text-transform:uppercase; }
    .empty.action-drop { margin:8px; height:72px; font-family:var(--bs-font-mono); font-size:var(--type-micro); letter-spacing:.08em; text-transform:uppercase; }
    .card { border-radius:0; border-left:1px solid var(--line); padding:10px 12px; cursor:pointer; }
    .card[data-runtime="running"] { border-left:2px solid var(--accent); }
    .key, .count, .footer, .pill, .label-chip { font-family:var(--bs-font-mono); letter-spacing:.04em; }
    .priority-pill { border:1px solid var(--line); padding:1px 5px; color:var(--muted); font-size:var(--type-micro); }
    .label-chip { border:1px solid color-mix(in oklch, var(--accent) 35%, var(--line)); padding:1px 6px; color:var(--accent); background:transparent; font-size:var(--type-micro); line-height:1.4; }
    .label-chip.dim { color:var(--muted); border-color:var(--line); }
    .run-state { color:var(--accent); font-family:var(--bs-font-mono); font-size:var(--type-rail); letter-spacing:.08em; text-transform:uppercase; }
    .run-dot { width:6px; height:6px; background:var(--accent); display:inline-block; border-radius:50%; margin-right:5px; }

    @media (max-width: 940px) {
      body { overflow: auto; }
      .shell { height: auto; min-height: 100vh; }
      header { height: auto; align-items: flex-start; flex-direction: column; padding: 12px; }
      nav, .actions, .command { width: 100%; }
      .command { width: 100%; }
      .workspace, .workspace.drawer-open { display: block; }
      .board { grid-auto-columns: minmax(270px, 88vw); min-height: 58vh; }
      .drawer { position: fixed; inset: 0; z-index: 15; display: none; }
      .workspace.drawer-open .drawer { display: flex; }
      .hidden-panel { display: none; }
      .fields { grid-template-columns: 1fr; }
      .create-primary-fields { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body data-bs-theme="paper" class="banana-sorbet ims-subsystem">
  <div class="shell">
    <header class="top-rail">
      <a class="rail-brand" href="/overview" aria-label="Meerkat Fugue overview"><span class="mark" aria-hidden="true"></span><span>MEERKAT FUGUE</span></a>
      <nav class="rail-tabs" aria-label="Primary app navigation">
        <a href="/overview">OVERVIEW</a>
        <a class="active" href="/issues">IMS</a>
        <a href="/console">CONSOLE</a>
        <a href="/ensemble">ENSEMBLE</a>
      </nav>
      <div class="rail-spacer"></div>
      <div class="rail-kpis" aria-label="Runtime metrics"><span>RUN <b id="kpiRun">—</b></span><span>RETRY <b id="kpiRetry">—</b></span><span>TOK <b id="kpiTok">unavailable</b></span><span>T <b id="kpiTime">unavailable</b></span><span>SHAD <b id="kpiShad">unavailable</b></span></div>
      <button class="rail-refresh" id="refresh" type="button" title="Refresh" aria-label="Refresh">↻</button>
    </header>
    <section class="work-heading"><div><h1>Work queue</h1><p id="queueConnection">Loading queue connection...</p></div><div class="fugue-actions"><a class="fugue-button" href="/ensemble">Repository teams</a><a class="fugue-button" href="/console">Talk to queue coordinator</a></div></section>
    <section class="lane-ribbon" id="laneRibbon" aria-label="Lane filter ribbon"></section>
    <section class="toolbar sub-toolbar">
      <div class="summary" id="summary"><span><span class="metric">0</span> shown</span></div>
      <div class="toolbar-actions"><label class="command" for="search"><span class="kbd">/</span><input id="search" autocomplete="off" placeholder="to search…"></label><button id="newIssue" type="button" class="primary">+ New issue</button><button class="icon ghost" id="shortcutButton" type="button" title="Shortcuts" aria-label="Shortcuts">?</button><span class="updated" id="selectionHint"></span></div>
    </section>
    <section class="create-panel" id="createPanel" aria-label="Create issue">
      __FUGUE_CREATE_EDITOR__
    </section>
    <div class="notice" id="notice"></div>
    <div class="workspace" id="workspace">
      <main class="board" id="board">
        <div class="loading">loading…</div>
      </main>
      <aside class="drawer" id="drawer" aria-label="Issue detail">
        <div class="drawer-inner" id="drawerInner"></div>
      </aside>
      <aside class="hidden-panel" aria-label="Hidden columns">
        <div class="hidden-head">
          <span>Hidden columns</span>
          <span class="updated" id="hiddenCount">0</span>
        </div>
        <div class="hidden-list" id="hiddenList"></div>
      </aside>
    </div>
  </div>
  <div class="bulkbar" id="bulkbar">
    <span class="bulkbar-count" id="bulkCount">0 selected</span>
    <button class="icon ghost" id="clearSelection" type="button" title="Clear selection" aria-label="Clear selection">x</button>
    <span class="bulkbar-divider"></span>
    <button id="bulkActions" type="button"><span class="kbd">&#8984;</span> Actions</button>
  </div>
  <div class="palette" id="palette">
    <div class="palette-panel">
      <div class="palette-top">
        <span class="palette-count" id="paletteCount">0 issues</span>
        <input class="palette-input" id="paletteInput" autocomplete="off" placeholder="Type an action">
        <span class="palette-muted">Tab</span>
      </div>
      <div class="palette-list" id="paletteList"></div>
    </div>
  </div>
  <div class="shortcuts" id="shortcuts">
    <div class="shortcut-panel">
      <h2>Shortcuts</h2>
      <div class="shortcut-row"><span>Search</span><span class="kbd">/</span></div>
      <div class="shortcut-row"><span>Refresh</span><span class="kbd">R</span></div>
      <div class="shortcut-row"><span>Next or previous issue</span><span><span class="kbd">J</span> <span class="kbd">K</span></span></div>
      <div class="shortcut-row"><span>Open selected issue</span><span class="kbd">Enter</span></div>
      <div class="shortcut-row"><span>Focus Workpad editor</span><span class="kbd">E</span></div>
      <div class="shortcut-row"><span>Focus comment composer</span><span class="kbd">C</span></div>
      <div class="shortcut-row"><span>Close panel</span><span class="kbd">Esc</span></div>
    </div>
  </div>
  <script>
    const board = document.getElementById("board");
    const notice = document.getElementById("notice");
    const summary = document.getElementById("summary");
    const refresh = document.getElementById("refresh");
    const newIssue = document.getElementById("newIssue");
    const createPanel = document.getElementById("createPanel");
    const createForm = document.getElementById("createForm");
    const createTitle = document.getElementById("createTitle");
    const createDescription = document.getElementById("createDescription");
    const createAttachmentDropZone = document.getElementById("createAttachmentDropZone");
    const createAttachments = document.getElementById("createAttachments");
    const createAttachmentList = document.getElementById("createAttachmentList");
    const createLane = document.getElementById("createLane");
    const createSubmit = document.getElementById("createSubmit");
    const createCancel = document.getElementById("createCancel");
    const createError = document.getElementById("createError");
    const createAttachmentState = [];
    let createdIssueReceipt = null;
    let creationWarning = null;
    const trackerScope = "__FUGUE_TRACKER_SCOPE__";
    let creationUploadBlocked = false;
    const creationReceiptKey = `fugue.created-issue:v2:${location.pathname}:${trackerScope}`;
    const detailAttachmentState = [];
    const maxCreateAttachmentCount = 20;
    const maxCreateAttachmentBytes = 25 * 1024 * 1024;
    const maxCreateAttachmentTotalBytes = 100 * 1024 * 1024;
    const search = document.getElementById("search");
    const workspace = document.getElementById("workspace");
    const drawerInner = document.getElementById("drawerInner");
    const selectionHint = document.getElementById("selectionHint");
    const laneRibbon = document.getElementById("laneRibbon");
    const kpiRun = document.getElementById("kpiRun");
    const kpiRetry = document.getElementById("kpiRetry");
    const kpiTok = document.getElementById("kpiTok");
    const shortcuts = document.getElementById("shortcuts");
    const shortcutButton = document.getElementById("shortcutButton");
    const hiddenList = document.getElementById("hiddenList");
    const hiddenCount = document.getElementById("hiddenCount");
    const bulkbar = document.getElementById("bulkbar");
    const bulkCount = document.getElementById("bulkCount");
    const clearSelection = document.getElementById("clearSelection");
    const bulkActions = document.getElementById("bulkActions");
    const palette = document.getElementById("palette");
    const paletteInput = document.getElementById("paletteInput");
    const paletteList = document.getElementById("paletteList");
    const paletteCount = document.getElementById("paletteCount");

    const app = {
      board: null,
      flatIssues: [],
      selected: null,
      detail: null,
      filter: "",
      dragging: null,
      selectedIssues: new Set(),
      paletteActions: [],
      laneFilter: "ALL",
    };

    const escapeHtml = (value) => String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

    const formatDate = (value) => {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(date);
    };

    const api = async (url, options = {}) => {
      const response = await fugueFetch(url, {
        ...options,
        headers: {
          "Accept": "application/json",
          "X-Fugue-Tracker-Scope": trackerScope,
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...(options.headers || {}),
        },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error?.message || `Request failed: ${response.status}`);
      }
      return payload;
    };

    const showNotice = (message) => {
      notice.textContent = message;
      notice.style.display = "block";
      window.clearTimeout(showNotice.timer);
      showNotice.timer = window.setTimeout(() => { notice.style.display = "none"; }, 6000);
    };


    const isActionLane = (name) => app.board?.lanes.find(lane=>lane.name===name)?.action;
    const laneGlyph = (name) => ({
      Backlog: "○",
      Queue: "◉",
      Active: "◐",
      Review: "◇",
      Deploy: "↑",
      Blocked: "!",
      Done: "✓",
      Canceled: "×",
    }[name] || "○");
    const labelChip = (label, running = false) => {
      const dim = !running || !String(label).startsWith("fugue:phase:");
      return `<span class="label-chip${dim ? " dim" : ""}">${escapeHtml(label)}</span>`;
    };

    const runtimePill = (runtime) => {
      if (!runtime) return "";
      if (runtime.kind === "running") {
        const turns = runtime.turn_count === 1 ? "1 turn" : `${runtime.turn_count} turns`;
        return `<span class="pill run">Running &middot; ${escapeHtml(turns)}</span>`;
      }
      if (runtime.kind === "retrying") {
        return `<span class="pill retry">Retry ${escapeHtml(runtime.attempt)}</span>`;
      }
      return "";
    };

    const runtimeKind = (runtime) => runtime ? runtime.kind : "none";
    const newtype = (value) => value && typeof value === "object" && "0" in value ? value["0"] : value;
    const issueByIdentifier = (identifier) => app.flatIssues.find((issue) => issue.identifier === identifier);
    const selectedIssueList = () => [...app.selectedIssues].map(issueByIdentifier).filter(Boolean);

    const issueMatchesFilter = (issue) => {
      const needle = app.filter.trim().toLowerCase();
      if (!needle) return true;
      return [issue.identifier, issue.title, issue.lane, issue.branch_name, ...(issue.labels || [])]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(needle));
    };

    const renderIssue = (issue) => {
      const running = runtimeKind(issue.runtime) === "running";
      const labels = (issue.labels || []).slice(0, 3).map((label) => labelChip(label, running)).join("");
      const priority = issue.priority ? `<span class="priority-pill">P${escapeHtml(issue.priority)}</span>` : "";
      const updated = formatDate(issue.updated_at);
      const selected = issue.identifier === app.selected ? " selected" : "";
      const multiSelected = app.selectedIssues.has(issue.identifier) ? " multi-selected" : "";
      // rework-note: non-running issue cards do not have measured turn evidence in this payload; avoid rendering a fabricated zero-turn count.
      const turns = issue.runtime?.kind === "running" ? `${escapeHtml(issue.runtime.turn_count)}t` : "";
      return `
        <button class="card${selected}${multiSelected}" data-issue="${escapeHtml(issue.identifier)}" data-runtime="${runtimeKind(issue.runtime)}" draggable="true" type="button">
          <div class="keyrow"><span class="key">${escapeHtml(issue.identifier)}</span>${priority}</div>
          <div class="title">${escapeHtml(issue.title)}</div>
          <div class="meta">${labels}</div>
          <div class="footer"><span class="${running ? "run-state" : ""}">${running ? '<span class="run-dot"></span>Running · ' : ""}${turns}</span><span>${updated ? escapeHtml(updated) : ""}</span></div>
        </button>
      `;
    };

    const renderLane = (lane) => {
      const issues = lane.issues.filter(issueMatchesFilter);
      const action = isActionLane(lane.name);
      const cards = issues.length ? issues.map(renderIssue).join("") : '<div class="empty">No issues in this lane</div>';
      const hint = action ? `<div class="lane-action-hint">▸ Drop to run ${escapeHtml(lane.action)} procedure</div>` : "";
      return `
        <section class="lane full ${lane.name === "Active" && issues.length ? "active-lane" : ""}" data-lane="${escapeHtml(lane.name)}" data-kind="${escapeHtml(lane.kind)}">
          <div class="lane-head"><div class="lane-title"><span aria-hidden="true">${laneGlyph(lane.name)}</span><strong>${escapeHtml(lane.name)}</strong></div><span class="count">${issues.length}</span></div>
          ${hint}
          <div class="cards">${cards}</div>
        </section>
      `;
    };

    const renderBoard = () => {
      if (!app.board) return;
      notice.style.display = "none";
      const filteredCount = app.flatIssues.filter(issueMatchesFilter).length;
      kpiRun.textContent = app.board.totals.running;
      kpiRetry.textContent = app.board.totals.retrying;
      // rework-note: the board totals payload does not currently expose token, elapsed-time, or shadow-recommendation metrics; render explicit unavailable placeholders instead of coercing missing values to zero.
      kpiTok.textContent = app.board.totals.tokens ?? "unavailable";
      kpiTime.textContent = app.board.totals.elapsed_seconds != null ? `${Number(app.board.totals.elapsed_seconds).toFixed(1)}s` : "unavailable";
      kpiShad.textContent = app.board.totals.shadow_recommendations != null ? `${app.board.totals.shadow_recommendations}r` : "unavailable";
      summary.innerHTML = `<span><span class="metric">${filteredCount}</span> shown · <span class="metric">${app.board.totals.issues}</span> total · <span class="metric">${app.board.totals.running}</span> running · <span class="metric">${app.board.totals.retrying}</span> retrying · grouped by lane · sorted by updated</span>`;
      selectionHint.textContent = app.selected ? app.selected : "";
      const allCount = app.board.lanes.reduce((sum, lane) => sum + lane.issues.filter(issueMatchesFilter).length, 0);
      laneRibbon.innerHTML = [`<button class="lane-chip ${app.laneFilter === "ALL" ? "active" : ""}" data-filter="ALL" data-count="${allCount}" type="button">ALL · <span class="chip-count">${allCount}</span></button>`]
        .concat(app.board.lanes.map((lane) => { const count = lane.issues.filter(issueMatchesFilter).length; return `<button class="lane-chip ${app.laneFilter === lane.name ? "active" : ""}" data-filter="${escapeHtml(lane.name)}" data-count="${count}" type="button">${laneGlyph(lane.name)} ${escapeHtml(lane.name)} <span class="chip-count">${count}</span></button>`; }))
        .join("");
      const lanes = app.laneFilter === "ALL" ? app.board.lanes : app.board.lanes.filter((lane) => lane.name === app.laneFilter);
      board.innerHTML = lanes.map(renderLane).join("");
      renderHiddenColumns([]);
      renderBulkbar();
    };

    const renderHiddenColumns = (lanes) => {
      hiddenCount.textContent = lanes.length;
      hiddenList.innerHTML = lanes.map((lane) => `
        <div class="hidden-row" data-kind="${escapeHtml(lane.kind)}">
          <span class="left">
            <span class="dot" aria-hidden="true"></span>
            <span>${escapeHtml(lane.name)}</span>
          </span>
          <span class="count">${lane.issues.length}</span>
        </div>
      `).join("");
    };

    const renderBulkbar = () => {
      const count = app.selectedIssues.size;
      bulkCount.textContent = count === 1 ? "1 selected" : `${count} selected`;
      bulkbar.classList.toggle("open", count > 0);
      paletteCount.textContent = count === 1 ? "1 issue" : `${count} issues`;
    };

    const refreshFlatIssues = () => {
      app.flatIssues = app.board
        ? app.board.lanes.flatMap((lane) => lane.issues)
        : [];
      for (const identifier of [...app.selectedIssues]) {
        if (!app.flatIssues.some((issue) => issue.identifier === identifier)) {
          app.selectedIssues.delete(identifier);
        }
      }
      if (!app.selected && app.flatIssues.length) {
        app.selected = app.flatIssues[0].identifier;
      }
    };

    const toggleIssueSelection = (identifier, force) => {
      if (force === true || (force === undefined && !app.selectedIssues.has(identifier))) {
        app.selectedIssues.add(identifier);
      } else if (force === false || force === undefined) {
        app.selectedIssues.delete(identifier);
      }
      app.selected = identifier;
      renderBoard();
    };

    const clearIssueSelection = () => {
      app.selectedIssues.clear();
      renderBoard();
    };

    const loadBoard = async () => {
      refresh.disabled = true;
      try {
        app.board = await api("/api/v1/issues/board");
        refreshFlatIssues();
        renderBoard();
        if (app.selected && workspace.classList.contains("drawer-open")) {
          await loadDetail(app.selected);
        }
      } catch (error) {
        showNotice(error.message);
        if (!board.querySelector(".lane")) {
          board.innerHTML = '<div class="loading">Issue board unavailable.</div>';
        }
      } finally {
        refresh.disabled = false;
      }
    };

    const selectedIndex = () => app.flatIssues.findIndex((issue) => issue.identifier === app.selected);

    const selectByOffset = async (offset) => {
      const visible = app.flatIssues.filter(issueMatchesFilter);
      if (!visible.length) return;
      const current = Math.max(0, visible.findIndex((issue) => issue.identifier === app.selected));
      const next = Math.min(visible.length - 1, Math.max(0, current + offset));
      app.selected = visible[next].identifier;
      renderBoard();
      document.querySelector(`[data-issue="${CSS.escape(app.selected)}"]`)?.scrollIntoView({ block: "nearest", inline: "nearest" });
      if (workspace.classList.contains("drawer-open")) {
        await loadDetail(app.selected);
      }
    };

    const loadDetail = async (identifier) => {
      app.selected = identifier;
      renderBoard();
      workspace.classList.add("drawer-open");
      drawerInner.innerHTML = '<div class="loading">loading…</div>';
      try {
        app.detail = await api(`/api/v1/issues/${encodeURIComponent(identifier)}`);
        renderDetail();
      } catch (error) {
        drawerInner.innerHTML = `<div class="loading">${escapeHtml(error.message)}</div>`;
      }
    };

    const openFullDetail = (identifier = app.selected) => {
      if (!identifier) return;
      window.fugueNavigate(`/issues/${encodeURIComponent(identifier)}`);
    };

    const renderDetail = () => {
      const detail = app.detail;
      if (!detail) return;
      const issue = detail.issue;
      const workpad = detail.workpad?.body_markdown || "";
      const laneOptions = detail.lanes.map((lane) =>
        `<option value="${escapeHtml(lane)}" ${lane === issue.state.name ? "selected" : ""}>${escapeHtml(lane)}</option>`
      ).join("");
      const labels = issue.labels.map((label) => `<span class="pill">${escapeHtml(label)}</span>`).join("");
      const activity = detail.comments.filter(comment => newtype(comment.comment_id) !== newtype(detail.workpad?.comment_id));
      const comments = activity.length
        ? activity.map(renderComment).join("")
        : '<div class="empty">No comments</div>';
      drawerInner.innerHTML = `
        <div class="drawer-head">
          <div class="drawer-title">
            <div class="id">${escapeHtml(newtype(issue.identifier))}</div>
            <h2>${escapeHtml(issue.title)}</h2>
          </div>
          <button class="icon ghost" id="closeDrawer" type="button" title="Close" aria-label="Close">x</button>
        </div>
        <div class="drawer-body">
          <section class="section">
            <div class="section-head">
              <h3>Issue</h3>
              ${/^https?:\/\//.test(issue.url || "") ? `<a class="pill" href="${escapeHtml(issue.url)}" target="_blank" rel="noopener">Open in tracker</a>` : ""}
            </div>
            <div class="fields">
              <div class="field"><span>Status</span><strong>${escapeHtml(issue.state.name)}</strong></div>
              <div class="field"><span>Assignee</span><strong>${escapeHtml(issue.assignee?.name || "Unassigned")}</strong></div>
              <div class="field"><span>Updated</span><strong>${escapeHtml(formatDate(issue.updated_at) || "Unknown")}</strong></div>
              <div class="field"><span>Branch</span><strong>${escapeHtml(newtype(issue.branch_name) || "None")}</strong></div>
            </div>
            <div class="meta" style="margin-top:10px">${labels}</div>
          </section>
          <section class="section">
            <div class="section-head"><h3>Move</h3></div>
            <select id="laneSelect">${laneOptions}</select>
            <div class="editor-actions">
              <button id="moveIssue" type="button">Move issue</button>
            </div>
          </section>
          <section class="section">
            <div class="section-head"><h3>Description</h3></div>
            <div class="description">${escapeHtml(issue.description || "No description")}</div>
          </section>
          <section class="section">
            <details><summary>Agent workpad</summary>
            <textarea id="workpadEditor" class="issue-markdown-editor" spellcheck="false">${escapeHtml(workpad)}</textarea>
            <div class="editor-actions">
              <button id="saveWorkpad" class="primary" type="button">Save Workpad</button>
            </div></details>
          </section>
          <section class="section attachment-drop-zone" id="detailAttachmentDropZone" data-attachment-drop-zone aria-label="Drop files to attach them to this issue">
            <div class="section-head"><h3>Attachments</h3></div>
            <label class="create-attachment-label ghost" for="detailAttachmentInput"><span>Choose files</span><input id="detailAttachmentInput" type="file" multiple></label>
            <div class="attachment-drop-affordance" aria-hidden="true">Drop files here to attach them to this issue</div>
            <div class="create-attachments" id="detailAttachmentList" aria-live="polite"></div>
            <div class="create-error" id="detailAttachmentError" role="alert"></div>
          </section>
          <section class="section">
            <div class="section-head"><h3>New comment</h3></div>
            <textarea id="commentEditor" class="issue-markdown-editor" spellcheck="true"></textarea>
            <div class="editor-actions">
              <button id="addComment" class="primary" type="button">Add comment</button>
            </div>
          </section>
          <section class="section">
            <div class="section-head"><h3>Comments</h3></div>
            <div class="comment-list">${comments}</div>
          </section>
        </div>
      `;
      renderDetailAttachments();
      installAttachmentDropTarget({
        dropZone: document.getElementById("detailAttachmentDropZone"),
        input: document.getElementById("detailAttachmentInput"),
        state: detailAttachmentState,
        validate: validateDetailAttachment,
        render: renderDetailAttachments,
        errorTarget: document.getElementById("detailAttachmentError"),
        onAdd: async (added) => {
          const identifier = newtype(app.detail?.issue?.identifier);
          await uploadIssueAttachments(identifier, added, document.getElementById("detailAttachmentError"));
          if (identifier) await loadDetail(identifier);
        }
      });
    };

    const renderComment = (comment) => `
      <article class="comment">
        <div class="comment-head">
          <span>${escapeHtml(newtype(comment.comment_id))}</span>
          ${comment.url ? `<a class="pill" href="${escapeHtml(comment.url)}">Open</a>` : ""}
        </div>
        <div class="comment-body">${escapeHtml(comment.body_markdown)}</div>
      </article>
    `;

    const formatFileSize = (bytes) => {
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
      return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
    };

    const renderAttachmentList = (list, state, emptyLabel) => {
      if (!list) return;
      list.innerHTML = state.length
        ? state.map((attachment) => `<span class="create-attachment-chip">${escapeHtml(attachment.name)} · ${escapeHtml(formatFileSize(attachment.size))}</span>`).join("")
        : `<span class="updated">${escapeHtml(emptyLabel)}</span>`;
    };

    const renderCreateAttachments = () => {
      renderAttachmentList(createAttachmentList, createAttachmentState, "No attachments selected.");
      syncCreateControls();
    };
    const renderDetailAttachments = () => renderAttachmentList(document.getElementById("detailAttachmentList"), detailAttachmentState, "No attachments added for this issue editor.");

    const dataTransferHasFiles = (event) => Array.from(event.dataTransfer?.types || []).includes("Files");

    const validateAttachmentMetadata = (file, state) => {
      if (state.length >= maxCreateAttachmentCount) {
        throw new Error("At most 20 attachments can be added.");
      }
      if (file.size > maxCreateAttachmentBytes) {
        throw new Error(`${file.name} exceeds the 25 MiB per-file attachment limit.`);
      }
      const nextTotal = state.reduce((sum, attachment) => sum + attachment.size, 0) + file.size;
      if (nextTotal > maxCreateAttachmentTotalBytes) {
        throw new Error("Attachments exceed the 100 MiB total attachment limit.");
      }
      return { name: file.name, size: file.size, content_type: file.type || null, file };
    };

    const validateCreateAttachment = (file) => validateAttachmentMetadata(file, createAttachmentState);
    const validateDetailAttachment = (file) => validateAttachmentMetadata(file, detailAttachmentState);

    const addFilesToAttachmentState = ({ files, state, validate, render, errorTarget }) => {
      const incoming = Array.from(files || []);
      const added = [];
      if (!incoming.length) return added;
      if (errorTarget) errorTarget.textContent = "";
      for (const file of incoming) {
        const attachment = validate(file);
        state.push(attachment);
        added.push(attachment);
      }
      render();
      return added;
    };

    const installAttachmentDropTarget = ({ dropZone, input, state, validate, render, errorTarget, onAdd }) => {
      if (!dropZone) return;
      const setActive = (active) => dropZone.classList.toggle("drag-over", active);
      for (const type of ["dragenter", "dragover"]) {
        dropZone.addEventListener(type, (event) => {
          if (!dataTransferHasFiles(event)) return;
          event.preventDefault();
          event.stopPropagation();
          event.dataTransfer.dropEffect = "copy";
          setActive(true);
        });
      }
      dropZone.addEventListener("dragleave", (event) => {
        if (!dropZone.contains(event.relatedTarget)) setActive(false);
      });
      dropZone.addEventListener("drop", async (event) => {
        if (!dataTransferHasFiles(event)) return;
        event.preventDefault();
        event.stopPropagation();
        setActive(false);
        try {
          const added = addFilesToAttachmentState({ files: event.dataTransfer.files, state, validate, render, errorTarget });
          if (added.length && onAdd) await onAdd(added);
        } catch (error) {
          if (errorTarget) errorTarget.textContent = error.message;
          showNotice(error.message);
        }
      });
      input?.addEventListener("change", async () => {
        try {
          const added = addFilesToAttachmentState({ files: input.files, state, validate, render, errorTarget });
          if (added.length && onAdd) await onAdd(added);
        } catch (error) {
          if (errorTarget) errorTarget.textContent = error.message;
          showNotice(error.message);
        } finally {
          input.value = "";
        }
      });
    };

    document.addEventListener("dragover", (event) => {
      if (dataTransferHasFiles(event)) event.preventDefault();
    }, true);
    document.addEventListener("drop", (event) => {
      if (dataTransferHasFiles(event)) event.preventDefault();
    }, true);

    const createAttachmentPayload = () => createAttachmentState.map((attachment) => ({
      name: attachment.name,
      size: attachment.size,
      content_type: attachment.content_type,
    }));

    const uploadIssueAttachments = async (identifier, attachments, errorTarget) => {
      if (!identifier || !attachments.length) return null;
      const form = new FormData();
      for (const attachment of attachments) {
        form.append("attachments", attachment.file, attachment.name);
      }
      const payload = await fugueIssueMutation(`/api/v1/issues/${encodeURIComponent(identifier)}/attachments`, {
        method: "POST", headers: {"Accept":"application/json", "X-Fugue-Tracker-Scope":trackerScope}, body: form,
      }, {kind:"upload", fileCount:attachments.length});
      const message = payload.warning?.message || `Attached ${payload.attachments?.length || attachments.length} file(s).`;
      if (errorTarget) errorTarget.textContent = message;
      showNotice(message);
      return payload;
    };

    const syncCreateControls = () => {
      const created = !!createdIssueReceipt;
      for (const field of [createTitle, createDescription, createLane]) field.disabled = created;
      createSubmit.textContent = created ? "Retry attachments" : "Create issue";
      createSubmit.hidden = created && (creationUploadBlocked || !createAttachmentState.length);
      createCancel.textContent = created ? "Done" : "Cancel";
    };

    const creationReceipt = (issue) => {
      if (!issue || typeof issue.id !== "string" || !issue.id || typeof issue.identifier !== "string" || !issue.identifier) {
        throw new Error("Creation returned no usable issue reference. Check your tracker before creating the issue again.");
      }
      return {id: issue.id, identifier: issue.identifier};
    };

    const saveCreationReceipt = () => {
      try {
        if (createdIssueReceipt) {
          sessionStorage.setItem(creationReceiptKey, JSON.stringify({
            issue: createdIssueReceipt, warning: creationWarning, uploadBlocked: creationUploadBlocked,
            title: createTitle.value, description: createDescription.value, lane: createLane.value,
          }));
        } else {
          sessionStorage.removeItem(creationReceiptKey);
        }
        return true;
      } catch (error) {
        console.warn("Could not persist the issue creation receipt", error);
        return false;
      }
    };

    const showCreatedIssue = (message) => {
      createError.replaceChildren(document.createTextNode(message + " "));
      const link = document.createElement("a");
      link.href = fugueUrl(`/issues/${encodeURIComponent(createdIssueReceipt.identifier)}`);
      link.textContent = `Open ${createdIssueReceipt.identifier}`;
      createError.append(link);
      syncCreateControls();
    };

    const resetCreateForm = () => {
      createdIssueReceipt = null;
      creationWarning = null;
      creationUploadBlocked = false;
      saveCreationReceipt();
      createForm.reset();
      createAttachmentState.length = 0;
      renderCreateAttachments();
    };

    const restoreCreationReceipt = () => {
      try {
        const saved = JSON.parse(sessionStorage.getItem(creationReceiptKey) || "null");
        if (!saved) return;
        createdIssueReceipt = creationReceipt(saved.issue);
        creationUploadBlocked = saved.uploadBlocked === true;
        creationWarning = typeof saved.warning === "string" ? saved.warning : null;
        createTitle.value = typeof saved.title === "string" ? saved.title : "";
        createDescription.value = typeof saved.description === "string" ? saved.description : "";
        createLane.value = typeof saved.lane === "string" ? saved.lane : createLane.value;
        createPanel.classList.add("open");
        showCreatedIssue(creationWarning || `Created ${createdIssueReceipt.identifier}. Re-select any files that still need uploading, or choose Done.`);
      } catch (error) {
        console.warn("Could not read the saved issue creation receipt", error);
        showNotice("Saved creation details could not be read. Check your tracker before creating the issue again.");
      }
    };

    const createIssue = async () => {
      if (createSubmit.disabled || creationUploadBlocked) return;
      const title = createTitle.value.trim();
      createError.textContent = "";
      if (!createdIssueReceipt && !title) {
        createError.textContent = "Title is required.";
        createTitle.focus();
        return;
      }
      if (createdIssueReceipt && !createAttachmentState.length) {
        showCreatedIssue(creationWarning || `Created ${createdIssueReceipt.identifier}. Choose Done or open the issue.`);
        return;
      }
      createSubmit.disabled = true;
      createCancel.disabled = true;
      newIssue.disabled = true;
      try {
        if (!createdIssueReceipt) {
          const payload = await api("/api/v1/issues", {
            method: "POST",
            body: JSON.stringify({
              title,
              description: createDescription.value,
              lane: createLane.value,
              attachments: createAttachmentPayload(),
            }),
          });
          createdIssueReceipt = creationReceipt(payload.created_issue || payload.issue);
          creationWarning = typeof payload.warning?.message === "string" ? payload.warning.message : null;
          if (!saveCreationReceipt()) {
            creationWarning = `Created ${createdIssueReceipt.identifier}. Keep this tab open until you finish; its recovery details could not be saved.`;
          }
          syncCreateControls();
        }
        const createdIdentifier = createdIssueReceipt.identifier;
        if (createAttachmentState.length) {
          const pending = createAttachmentState.slice();
          const previousWarning = creationWarning;
          creationUploadBlocked = true;
          creationWarning = "The upload is not confirmed. Check the existing issue before uploading any files again.";
          if (!saveCreationReceipt()) throw Object.assign(new Error(creationWarning + " Recovery details could not be saved."), {uploadUncertain:true});
          let upload;
          try { upload = await uploadIssueAttachments(createdIdentifier, pending, createError); }
          catch (error) {
            if (error.confirmedRejected) {
              creationUploadBlocked = false;
              creationWarning = previousWarning;
              saveCreationReceipt();
            }
            throw error;
          }
          creationUploadBlocked = false;
          creationWarning = previousWarning;
          saveCreationReceipt();
          if (upload?.warning) {
            creationWarning = upload.warning.message;
            creationUploadBlocked = true;
            saveCreationReceipt();
          }
          for (const attachment of pending) {
            const index = createAttachmentState.indexOf(attachment);
            if (index >= 0) createAttachmentState.splice(index, 1);
          }
          renderCreateAttachments();
        }
        app.selected = createdIdentifier;
        if (creationWarning || createAttachmentState.length) {
          showCreatedIssue(creationWarning || `Created ${createdIdentifier}. Upload the remaining attachments or choose Done.`);
          await loadBoard();
          return;
        }
        resetCreateForm();
        createPanel.classList.remove("open");
        await loadBoard();
        showNotice(`Created ${createdIdentifier}.`);
      } catch (error) {
        if (createdIssueReceipt) {
          if (error.uploadUncertain) {
            creationUploadBlocked = true;
            creationWarning = error.message;
            saveCreationReceipt();
          }
          showCreatedIssue(`${creationWarning || `Created ${createdIssueReceipt.identifier}.`} ${error.message}${creationUploadBlocked ? "" : " Retry attachments here or open the existing issue."}`);
        } else {
          createError.textContent = error.message;
        }
        showNotice(error.message);
      } finally {
        createSubmit.disabled = false;
        createCancel.disabled = false;
        newIssue.disabled = false;
        syncCreateControls();
      }
    };

    const moveIssue = async (identifier, lane) => {
      await api(`/api/v1/issues/${encodeURIComponent(identifier)}/state`, {
        method: "PATCH",
        body: JSON.stringify({ state: lane }),
      });
      await loadBoard();
      if (workspace.classList.contains("drawer-open")) {
        await loadDetail(identifier);
      }
    };

    const selectedMarkdown = () => selectedIssueList()
      .map((issue) => `- ${issue.identifier}: ${issue.title}`)
      .join("\n");

    const copySelectedMarkdown = async () => {
      const markdown = selectedMarkdown();
      if (!markdown) return;
      await navigator.clipboard.writeText(markdown);
      showNotice("Copied selected issues as Markdown.");
    };

    const buildPaletteActions = () => {
      const count = app.selectedIssues.size;
      const single = app.selected || [...app.selectedIssues][0];
      const lanes = app.board ? app.board.lanes.map((lane) => lane.name) : [];
      const actions = [
        {
          label: "Open details",
          hint: "Enter",
          enabled: Boolean(single),
          run: () => openFullDetail(single),
        },
        {
          label: "Open sidebar details",
          hint: "Cmd I",
          enabled: Boolean(single),
          run: () => loadDetail(single),
        },
        {
          label: "Copy issue descriptions as Markdown",
          hint: "Cmd C",
          enabled: count > 0,
          run: copySelectedMarkdown,
        },
      ];
      for (const lane of lanes) {
        actions.push({
          label: `Move selected to ${lane}`,
          hint: "M",
          enabled: count > 0,
          run: async () => {
            for (const identifier of [...app.selectedIssues]) {
              await moveIssue(identifier, lane);
            }
          },
        });
      }
      actions.push({
        label: "Clear selection",
        hint: "Esc",
        enabled: count > 0,
        run: clearIssueSelection,
      });
      return actions;
    };

    const openPalette = () => {
      app.paletteActions = buildPaletteActions();
      palette.classList.add("open");
      paletteInput.value = "";
      renderPalette();
      paletteInput.focus();
    };

    const closePalette = () => {
      palette.classList.remove("open");
    };

    const renderPalette = () => {
      const needle = paletteInput.value.trim().toLowerCase();
      const actions = app.paletteActions.filter((action) =>
        action.label.toLowerCase().includes(needle)
      );
      paletteList.innerHTML = actions.map((action, index) => `
        <button class="palette-row${index === 0 ? " active" : ""}" data-action="${index}" ${action.enabled ? "" : "disabled"} type="button">
          <span>${escapeHtml(action.label)}</span>
          <span class="palette-muted">${escapeHtml(action.hint || "")}</span>
        </button>
      `).join("");
    };

    const runPaletteAction = async (index = 0) => {
      const needle = paletteInput.value.trim().toLowerCase();
      const actions = app.paletteActions.filter((action) =>
        action.label.toLowerCase().includes(needle)
      );
      const action = actions[index];
      if (!action || !action.enabled) return;
      closePalette();
      try {
        await action.run();
      } catch (error) {
        showNotice(error.message);
      }
    };

    board.addEventListener("click", (event) => {
      const card = event.target.closest(".card");
      if (card) {
        if (event.metaKey || event.ctrlKey || event.shiftKey) {
          toggleIssueSelection(card.dataset.issue);
        } else {
          app.selected = card.dataset.issue;
          renderBoard();
          // rework-note: normal IMS card clicks open the inset issue sheet route; the legacy drawer remains only for old keyboard/detail helpers until fully removed.
          openFullDetail(card.dataset.issue);
        }
      }
    });

    board.addEventListener("dragstart", (event) => {
      const card = event.target.closest(".card");
      if (!card) return;
      app.dragging = card.dataset.issue;
      card.classList.add("dragging");
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", app.dragging);
    });

    board.addEventListener("dragend", () => {
      app.dragging = null;
      document.querySelectorAll(".dragging, .drop-target").forEach((node) => node.classList.remove("dragging", "drop-target"));
    });

    board.addEventListener("dragover", (event) => {
      const lane = event.target.closest(".lane");
      if (!lane || !app.dragging) return;
      event.preventDefault();
      lane.classList.add("drop-target");
    });

    board.addEventListener("dragleave", (event) => {
      event.target.closest(".lane")?.classList.remove("drop-target");
    });

    board.addEventListener("drop", async (event) => {
      const lane = event.target.closest(".lane");
      if (!lane || !app.dragging) return;
      event.preventDefault();
      const identifier = app.dragging;
      const targetLane = lane.dataset.lane;
      lane.classList.remove("drop-target");
      try {
        await moveIssue(identifier, targetLane);
      } catch (error) {
        showNotice(error.message);
      }
    });

    drawerInner.addEventListener("click", async (event) => {
      if (event.target.id === "closeDrawer") {
        workspace.classList.remove("drawer-open");
        return;
      }
      if (!app.detail) return;
      const identifier = newtype(app.detail.issue.identifier);
      if (event.target.id === "moveIssue") {
        try {
          await moveIssue(identifier, document.getElementById("laneSelect").value);
        } catch (error) {
          showNotice(error.message);
        }
      }
      if (event.target.id === "saveWorkpad") {
        event.target.disabled = true;
        try {
          await api(`/api/v1/issues/${encodeURIComponent(identifier)}/workpad`, {
            method: "PUT",
            body: JSON.stringify({ body_markdown: document.getElementById("workpadEditor").value, create_if_missing: true }),
          });
          await loadDetail(identifier);
        } catch (error) {
          showNotice(error.message);
        } finally {
          event.target.disabled = false;
        }
      }
      if (event.target.id === "addComment") {
        const editor = document.getElementById("commentEditor");
        event.target.disabled = true;
        try {
          await api(`/api/v1/issues/${encodeURIComponent(identifier)}/comments`, {
            method: "POST",
            body: JSON.stringify({ body_markdown: editor.value }),
          });
          await loadDetail(identifier);
        } catch (error) {
          showNotice(error.message);
        } finally {
          event.target.disabled = false;
        }
      }
    });

    search.addEventListener("input", () => {
      app.filter = search.value;
      renderBoard();
    });

    refresh.addEventListener("click", loadBoard);
    laneRibbon.addEventListener("click", (event) => {
      const chip = event.target.closest(".lane-chip");
      if (!chip) return;
      app.laneFilter = chip.dataset.filter || "ALL";
      renderBoard();
    });
    newIssue.addEventListener("click", () => {
      if (!createdIssueReceipt) createError.textContent = "";
      createPanel.classList.toggle("open");
      if (createPanel.classList.contains("open")) createTitle.focus();
    });
    createCancel.addEventListener("click", () => {
      if (createdIssueReceipt) resetCreateForm();
      createPanel.classList.remove("open");
      createError.textContent = "";
    });
    installAttachmentDropTarget({
      dropZone: createAttachmentDropZone,
      input: createAttachments,
      state: createAttachmentState,
      validate: validateCreateAttachment,
      render: renderCreateAttachments,
      errorTarget: createError,
    });
    renderCreateAttachments();
    restoreCreationReceipt();
    createForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      await createIssue();
    });
    shortcutButton.addEventListener("click", () => shortcuts.classList.add("open"));
    clearSelection.addEventListener("click", clearIssueSelection);
    bulkActions.addEventListener("click", openPalette);
    paletteInput.addEventListener("input", renderPalette);
    palette.addEventListener("click", (event) => {
      if (event.target === palette) {
        closePalette();
        return;
      }
      const row = event.target.closest(".palette-row");
      if (row) {
        runPaletteAction(Number(row.dataset.action));
      }
    });
    shortcuts.addEventListener("click", (event) => {
      if (event.target === shortcuts) shortcuts.classList.remove("open");
    });

    document.addEventListener("keydown", async (event) => {
      const target = event.target;
      const editing = target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement;
      if (event.key === "Escape") {
        if (palette.classList.contains("open")) {
          closePalette();
          return;
        }
        shortcuts.classList.remove("open");
        if (!editing) workspace.classList.remove("drawer-open");
        return;
      }
      if (palette.classList.contains("open")) {
        if (event.key === "Enter") {
          event.preventDefault();
          await runPaletteAction(0);
        }
        return;
      }
      if (editing) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        openPalette();
      } else if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "i") {
        event.preventDefault();
        if (app.selected) await loadDetail(app.selected);
      } else if (event.key === "/") {
        event.preventDefault();
        search.focus();
      } else if (event.key === "?") {
        shortcuts.classList.add("open");
      } else if (event.key.toLowerCase() === "x" && app.selected) {
        toggleIssueSelection(app.selected);
      } else if (event.key.toLowerCase() === "r") {
        await loadBoard();
      } else if (event.key.toLowerCase() === "j") {
        await selectByOffset(1);
      } else if (event.key.toLowerCase() === "k") {
        await selectByOffset(-1);
      } else if (event.key === "Enter" && app.selected) {
        openFullDetail(app.selected);
      } else if (event.key.toLowerCase() === "e") {
        document.getElementById("workpadEditor")?.focus();
      } else if (event.key.toLowerCase() === "c") {
        document.getElementById("commentEditor")?.focus();
      }
    });

    loadBoard();
    window.setInterval(loadBoard, 2000);
  </script>
</body>
</html>"#;

// rework-note: the static issue-sheet shell cannot read live dashboard KPIs before the client loads issue data, so it renders explicit unavailable placeholders and lets the client fill per-issue runtime values after `issue_detail_snapshot` loads.
const ISSUE_DETAIL_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Meerkat Fugue · Issue sheet</title>
  <style>
    :root { color-scheme: light; --bs-paper: #ECEAE4; --bs-paper-soft: #F6F4EE; --bs-paper-raised: #FAF8F2; --bs-ink: #1C1A17; --bs-muted: #6B6760; --bs-subtle: #A8A49B; --bs-line: #D6D2C8; --bs-accent: #D95A1E; --bs-accent-dark: #E76A2F; --bs-ok: #3E6B4F; --bs-warn: #A56A18; --bs-crit: #A43F32; --bs-font-sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; --bs-font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; --bg: var(--bs-paper); --surface: var(--bs-paper-soft); --paper: var(--bs-paper-raised); --ink: var(--bs-ink); --muted: var(--bs-muted); --subtle: var(--bs-subtle); --hair: var(--bs-line); --accent: var(--bs-accent); --ok: var(--bs-ok); --warn: var(--bs-warn); --crit: var(--bs-crit); --shadow-sheet: 0 24px 60px rgba(28,26,23,.28); --r-sm: 2px; --r-md: 6px; --type-micro: 10px; --type-rail: 11px; --type-meta: 12px; --type-body: 13px; --type-card: 14px; --type-panel-heading: 15px; --type-heading: 16px; --line-compact: 1.35; --control-h: 38px; --font-display: var(--bs-font-sans); }
    * { box-sizing:border-box; } body { margin:0; min-height:100vh; background:var(--bg); color:var(--ink); font-family:var(--bs-font-sans); font-size:var(--type-body); line-height:var(--line-compact); overflow:hidden; } a { color:inherit; text-decoration:none; } button, select, textarea { font:inherit; }
    .mono, .top-rail, .label, .meta, button { font-family:var(--bs-font-mono); font-size:var(--type-rail); letter-spacing:.12em; text-transform:uppercase; }
    .top-rail { height:var(--control-h); display:flex; align-items:center; border-bottom:1px solid var(--ink); background:var(--surface); } .brand { height:var(--control-h); display:flex; align-items:center; gap:10px; padding:0 16px; border-right:1px solid var(--hair); } .mark { width:12px; height:12px; border:1px solid var(--ink); transform:rotate(45deg); } .tabs { display:flex; height:var(--control-h); } .tabs a { display:flex; align-items:center; padding:0 18px; border-right:1px solid var(--hair); } .tabs a.active { background:var(--ink); color:var(--bg); } .spacer{flex:1}.kpis{display:flex;height:var(--control-h)}.kpis span{display:flex;align-items:center;padding:0 10px;border-left:1px solid var(--hair);color:var(--muted)}
    .sheet { position:fixed; top:56px; bottom:32px; left:32px; right:32px; background:var(--bg); border:1px solid var(--ink); box-shadow:var(--shadow-sheet); display:grid; grid-template-rows:42px minmax(0,1fr) 56px; }
    .sheet-head { display:flex; align-items:center; border-bottom:1px solid var(--ink); background:var(--surface); } .state-chip, .issue-id, .close { height:42px; display:flex; align-items:center; padding:0 14px; border-right:1px solid var(--hair); } .state-chip { color:var(--accent); } .sheet-title { flex:1; padding:0 14px; font-size:var(--type-panel-heading); font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .dirty { color:var(--warn); padding:0 14px; } .close { border-right:0; border-left:1px solid var(--ink); background:transparent; }
    .body { min-height:0; display:grid; grid-template-columns:300px minmax(0,1fr) 320px; } .props { border-right:1px solid var(--hair); overflow:auto; } .middle { overflow:auto; padding:24px; } .lanes { border-left:1px solid var(--hair); overflow:auto; }
    .field { padding:12px 14px; border-bottom:1px solid var(--hair); } .field.dirty { background:color-mix(in oklch, var(--accent) 5%, transparent); border-left:2px solid var(--accent); color:var(--ink); } .field-head { display:flex; justify-content:space-between; color:var(--muted); margin-bottom:6px; } .value del { color:var(--subtle); margin-right:8px; } .value strong { color:var(--accent); }
    .section-title { border-bottom:1px solid var(--ink); padding-bottom:8px; margin:0 0 14px; font-size:var(--type-panel-heading); } .description, .comment { white-space:pre-wrap; font-size:var(--type-body); line-height:1.45; border-bottom:1px solid var(--hair); padding:0 0 18px; margin:0 0 18px; } textarea { width:100%; min-height:150px; border:1px solid var(--hair); background:var(--paper); padding:10px; font-family:var(--bs-font-mono); font-size:var(--type-meta); }
    .comment-composer { border:1px solid var(--hair); background:var(--surface); padding:12px; margin:0 0 22px; } .comment-composer textarea { min-height:110px; background:var(--paper); } .comment-actions { display:flex; align-items:center; justify-content:flex-end; gap:12px; margin-top:8px; } .comment-error { flex:1; min-height:18px; color:var(--crit); text-transform:none; letter-spacing:0; } .comment-submit { height:38px; border:1px solid var(--ink); background:var(--accent); color:white; padding:0 16px; } .comment-submit:disabled { opacity:.5; cursor:not-allowed; }
    .attachment-input-label { display:inline-flex; align-items:center; gap:8px; margin:0 0 8px; cursor:pointer; color:var(--accent); } .attachment-input-label input { position:absolute; opacity:0; width:1px; height:1px; }
    .attachment-drop-zone { border:1px solid var(--hair); background:var(--surface); padding:12px; margin:0 0 22px; transition:border-color 120ms ease, background 120ms ease; } .attachment-drop-zone.drag-over { border-color:var(--accent); background:color-mix(in oklch, var(--accent) 8%, var(--surface)); } .attachment-drop-affordance { border:1px dashed color-mix(in oklch, var(--accent) 45%, var(--hair)); background:color-mix(in oklch, var(--accent) 5%, transparent); color:var(--muted); padding:8px 10px; text-align:center; } .attachment-list { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; } .attachment-chip { border:1px solid var(--hair); background:var(--paper); color:var(--muted); padding:4px 7px; font-family:var(--bs-font-mono); font-size:var(--type-meta); } .attachment-error { min-height:18px; margin-top:8px; color:var(--crit); text-transform:none; letter-spacing:0; }
    .lane-row { width:100%; min-height:50px; font-size:var(--type-meta); border:0; border-bottom:1px solid var(--hair); background:transparent; display:grid; grid-template-columns:28px 1fr auto; align-items:center; text-align:left; padding:0 14px; color:var(--ink); } .lane-row.target { border-left:2px solid var(--accent); background:color-mix(in oklch, var(--accent) 8%, transparent); } .lane-row .sub { color:var(--muted); display:block; margin-top:2px; }
    .footer { display:flex; align-items:center; border-top:1px solid var(--ink); background:var(--surface); } .diff { flex:1; padding:0 16px; color:var(--muted); } .trigger { color:var(--accent); } .footer button { height:56px; border:0; border-left:1px solid var(--ink); border-radius:0; padding:0 24px; } .cancel { width:180px; background:var(--bg); color:var(--ink); } .save { width:240px; background:var(--accent); color:white; font-weight:600; } .save:disabled { opacity:.5; cursor:not-allowed; }
    .label-chip { display:inline-flex; border:1px solid color-mix(in oklch, var(--accent) 35%, var(--hair)); color:var(--accent); padding:1px 6px; margin:0 6px 6px 0; font-family:var(--bs-font-mono); font-size:var(--type-micro); }
    .notice { position:fixed; right:18px; bottom:18px; display:none; color:var(--crit); background:var(--paper); border:1px solid var(--crit); padding:12px 14px; z-index:9; }
  </style>
</head>
<body data-bs-theme="paper" class="banana-sorbet issue-subsystem">
  <div class="top-rail"><a class="brand" href="/overview"><span class="mark"></span><span>MEERKAT FUGUE</span></a><nav class="tabs"><a href="/overview">OVERVIEW</a><a class="active" href="/issues">IMS</a><a href="/console">CONSOLE</a><a href="/ensemble">ENSEMBLE</a></nav><div class="spacer"></div><div class="kpis"><span>RUN&nbsp;<b id="kpiRun">—</b></span><span>RETRY&nbsp;<b id="kpiRetry">—</b></span><span>TOK&nbsp;<b id="kpiTok">—</b></span><span>T&nbsp;<b id="kpiTime">unavailable</b></span><span>SHAD&nbsp;<b id="kpiShadow">unavailable</b></span></div></div>
  <div class="notice" id="notice"></div>
  <main class="sheet" id="sheet">
    <header class="sheet-head"><div class="state-chip" id="stateChip">Loading</div><div class="issue-id" id="issueId">ISSUE</div><div class="sheet-title" id="title">loading…</div><div class="dirty" id="dirtyFlag" hidden>● UNSAVED · 1 CHANGE</div><button class="close" id="close" type="button">×</button></header>
    <section class="body"><aside class="props" id="props"></aside><section class="middle" id="middle"><div class="meta">loading…</div></section><aside class="lanes" id="lanePicker"></aside></section>
    <footer class="footer"><div class="diff" id="diff">No changes</div><button class="cancel" id="cancel" type="button">CANCEL <span class="meta">ESC</span></button><button class="save" id="save" type="button" disabled>SAVE CHANGES <span class="meta">⌘S</span></button></footer>
  </main>
<script>
  const issueIdentifier = decodeURIComponent(window.location.pathname.split("/").pop());
  const $ = (id) => document.getElementById(id);
  let detail = null; let targetLane = null; const issueAttachmentState = []; const commentAttachmentState = []; const maxAttachmentCount = 20; const maxAttachmentBytes = 25 * 1024 * 1024; const maxAttachmentTotalBytes = 100 * 1024 * 1024;
  const escapeHtml = (value) => String(value ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
  const newtype = (value) => value && typeof value === "object" && "0" in value ? value["0"] : value;
  const trackerScope = "__FUGUE_TRACKER_SCOPE__";
  const api = async (url, options = {}) => { const response = await fugueFetch(url, { ...options, headers: { "Accept":"application/json", "X-Fugue-Tracker-Scope":trackerScope, ...(options.body ? {"Content-Type":"application/json"} : {}), ...(options.headers || {}) }}); const payload = await response.json().catch(() => ({})); if (!response.ok) throw new Error(payload?.error?.message || `Request failed: ${response.status}`); return payload; };
  const showNotice = (message) => { $("notice").textContent = message; $("notice").style.display = "block"; };
  const clearNotice = () => { $("notice").textContent = ""; $("notice").style.display = "none"; };
  const laneGlyph = (name) => ({Backlog:"○", Queue:"◉", Active:"◐", Review:"◇", Deploy:"↑", Blocked:"!", Done:"✓", Canceled:"×"}[name] || "○");
  const isAction = (lane) => detail?.lane_actions?.[lane];
  const renderKpis = () => { const runtime = detail.runtime; $("kpiTime").textContent = "unavailable"; $("kpiShadow").textContent = "unavailable"; if (!runtime) { $("kpiRun").textContent = "unavailable"; $("kpiRetry").textContent = "unavailable"; $("kpiTok").textContent = "unavailable"; return; } if (runtime.kind === "running") { $("kpiRun").textContent = "1"; $("kpiRetry").textContent = "0"; $("kpiTok").textContent = String(runtime.token_total ?? "unavailable"); return; } if (runtime.kind === "retrying") { $("kpiRun").textContent = "0"; $("kpiRetry").textContent = String(runtime.attempt ?? "1"); $("kpiTok").textContent = "unavailable"; return; } $("kpiRun").textContent = "unavailable"; $("kpiRetry").textContent = "unavailable"; $("kpiTok").textContent = "unavailable"; };
  const formatFileSize = (bytes) => bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  const attachmentCommentId = (attachment) => newtype(attachment?.source?.Comment?.comment_id ?? attachment?.source?.comment?.comment_id ?? attachment?.source?.comment_id ?? null);
  const persistedCommentAttachments = (commentId) => (detail.attachments || []).filter((attachment) => attachmentCommentId(attachment) === commentId);
  const renderPersistedAttachmentChip = (attachment) => { const label = attachment.title || attachment.url || "attachment"; const size = attachment.byte_len ? ` · ${escapeHtml(formatFileSize(attachment.byte_len))}` : ""; return `<a class="attachment-chip" href="${escapeHtml(fugueUrl(attachment.url))}" target="_blank" rel="noopener">${escapeHtml(label)}${size}</a>`; };
  const renderComment = (comment) => { const attachments = persistedCommentAttachments(newtype(comment.comment_id)); const chips = attachments.length ? `<div class="attachment-list comment-attachments" aria-label="Comment attachments">${attachments.map(renderPersistedAttachmentChip).join("")}</div>` : ""; return `<article class="comment"><span class="meta">${escapeHtml(newtype(comment.comment_id))}</span><br>${escapeHtml(comment.body_markdown)}${chips}</article>`; };
  const dataTransferHasFiles = (event) => Array.from(event.dataTransfer?.types || []).includes("Files");
  const renderIssueAttachments = () => { const list = $("issueAttachmentList"); if (!list) return; const persistedHtml = (detail.attachments || []).map(renderPersistedAttachmentChip).join(""); const pendingHtml = issueAttachmentState.length ? issueAttachmentState.map((attachment) => `<span class="attachment-chip">${escapeHtml(attachment.name)} · ${escapeHtml(formatFileSize(attachment.size))}</span>`).join("") : ""; list.innerHTML = persistedHtml || pendingHtml ? `${persistedHtml}${pendingHtml}` : '<span class="meta">No attachments added for this issue editor.</span>'; };
  const validateIssueAttachment = (file, state = issueAttachmentState) => { if (state.length >= maxAttachmentCount) throw new Error("At most 20 attachments can be added."); if (file.size > maxAttachmentBytes) throw new Error(`${file.name} exceeds the 25 MiB per-file attachment limit.`); const nextTotal = state.reduce((sum, attachment) => sum + attachment.size, 0) + file.size; if (nextTotal > maxAttachmentTotalBytes) throw new Error("Attachments exceed the 100 MiB total attachment limit."); return { name:file.name, size:file.size, content_type:file.type || null, file }; };
  const recoveryKey = `fugue.issue-mutations:v1:${location.pathname}:${trackerScope}`;
  let issueUploadPending = false;
  let issueUploadWarning = "";
  let commentSubmissionPending = false;
  let commentSubmissionUncertain = false;
  let commentSubmissionWarning = "";
  let recoveredCommentDraft = "";
  const uncertainMessage = "The result was not confirmed. Check the existing issue before posting the comment or uploading files again.";
  try {
    const saved = JSON.parse(sessionStorage.getItem(recoveryKey) || "null");
    if (saved) {
      if (typeof saved.uploadWarning !== "string" || typeof saved.commentWarning !== "string" || typeof saved.commentDraft !== "string") throw new Error("Invalid recovery record");
      issueUploadWarning = saved.uploadWarning;
      commentSubmissionWarning = saved.commentWarning;
      commentSubmissionUncertain = Boolean(commentSubmissionWarning);
      recoveredCommentDraft = saved.commentDraft;
    }
  } catch {
    issueUploadWarning = commentSubmissionWarning = uncertainMessage;
    commentSubmissionUncertain = true;
  }
  const persistMutationRecovery = () => {
    sessionStorage.setItem(recoveryKey, JSON.stringify({
      uploadWarning:issueUploadWarning, commentWarning:commentSubmissionWarning,
      commentDraft:$("commentEditor")?.value ?? recoveredCommentDraft,
    }));
  };
  const renderUploadAvailability = () => {
    $("issueAttachmentInput").disabled = issueUploadPending || Boolean(issueUploadWarning);
    $("issueAttachmentError").textContent = issueUploadWarning;
    $("resolveIssueUpload").hidden = !issueUploadWarning || issueUploadPending;
  };
  const renderCommentAvailability = () => {
    $("postComment").disabled = commentSubmissionPending || commentSubmissionUncertain;
    $("commentEditor").disabled = commentSubmissionPending;
    $("commentAttachmentInput").disabled = commentSubmissionPending || commentSubmissionUncertain;
    $("commentError").textContent = commentSubmissionWarning;
    $("resolveComment").hidden = !commentSubmissionUncertain || commentSubmissionPending;
  };
  const uploadIssueAttachments = async (attachments) => {
    const form = new FormData();
    for (const attachment of attachments) form.append("attachments", attachment.file, attachment.name);
    // Record the in-flight ambiguity before dispatch, including reloads while
    // the request is pending. A storage failure must prevent dispatch.
    issueUploadWarning = uncertainMessage;
    persistMutationRecovery();
    try {
      const payload = await fugueIssueMutation(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/attachments`, {
        method:"POST", headers:{"Accept":"application/json","X-Fugue-Tracker-Scope":trackerScope}, body:form
      }, {kind:"upload", fileCount:attachments.length});
      issueUploadWarning = payload.warning?.message || "";
      persistMutationRecovery();
      return payload;
    } catch (error) {
      issueUploadWarning = error.confirmedRejected ? "" : uncertainMessage;
      persistMutationRecovery();
      throw error;
    }
  };
  const renderCommentAttachments = () => { const list = $("commentAttachmentList"); if (!list) return; list.innerHTML = commentAttachmentState.length ? commentAttachmentState.map((attachment, index) => `<span class="attachment-chip">${escapeHtml(attachment.name)} · ${escapeHtml(formatFileSize(attachment.size))} <button type="button" data-comment-attachment-remove="${index}" aria-label="Remove ${escapeHtml(attachment.name)}">×</button></span>`).join("") : '<span class="meta">No files selected for this comment.</span>'; };
  const installCommentAttachmentInput = () => { const input = $("commentAttachmentInput"); if (!input || input.dataset.attachmentReady === "true") return; input.dataset.attachmentReady = "true"; input.addEventListener("change", () => { const error = $("commentError"); try { for (const file of Array.from(input.files || [])) commentAttachmentState.push(validateIssueAttachment(file, commentAttachmentState)); renderCommentAttachments(); } catch (caught) { if (error) error.textContent = caught.message; showNotice(caught.message); } finally { input.value = ""; } }); };
  const installAttachmentDropTarget = () => {
    const zone = $("issueAttachmentDropZone");
    if (!zone || zone.dataset.dropReady === "true") return;
    zone.dataset.dropReady = "true";
    const setActive = (active) => zone.classList.toggle("drag-over", active);
    for (const type of ["dragenter", "dragover"]) zone.addEventListener(type, (event) => {
      if (!dataTransferHasFiles(event)) return;
      event.preventDefault(); event.stopPropagation();
      event.dataTransfer.dropEffect = issueUploadPending || issueUploadWarning ? "none" : "copy";
      setActive(!issueUploadPending && !issueUploadWarning);
    });
    zone.addEventListener("dragleave", (event) => { if (!zone.contains(event.relatedTarget)) setActive(false); });
    const addFiles = async (files) => {
      if (issueUploadPending || issueUploadWarning) return;
      const added = [];
      let attemptError = "";
      try {
        for (const file of Array.from(files || [])) added.push(validateIssueAttachment(file, added));
        if (!added.length) return;
        issueAttachmentState.push(...added);
        issueUploadPending = true;
        renderIssueAttachments(); renderUploadAvailability();
        const payload = await uploadIssueAttachments(added);
        showNotice(issueUploadWarning || `Attached ${payload.attachments?.length || added.length} file(s).`);
        await load(true);
      } catch (caught) {
        attemptError = caught.message;
        showNotice(caught.message);
        $("issueAttachmentError").textContent = caught.message;
      } finally {
        for (const attachment of added) {
          const index = issueAttachmentState.indexOf(attachment);
          if (index >= 0) issueAttachmentState.splice(index, 1);
        }
        issueUploadPending = false;
        renderIssueAttachments();
        renderUploadAvailability();
        if (!issueUploadWarning && attemptError) $("issueAttachmentError").textContent = attemptError;
      }
    };
    zone.addEventListener("drop", async (event) => {
      if (!dataTransferHasFiles(event)) return;
      event.preventDefault(); event.stopPropagation(); setActive(false);
      await addFiles(event.dataTransfer.files);
    });
    const input = $("issueAttachmentInput");
    input?.addEventListener("change", async () => {
      await addFiles(input.files);
      input.value = "";
    });
  };
  document.addEventListener("dragover", (event) => { if (dataTransferHasFiles(event)) event.preventDefault(); }, true); document.addEventListener("drop", (event) => { if (dataTransferHasFiles(event)) event.preventDefault(); }, true);
  const render = () => { const commentDraft = $("commentEditor")?.value ?? recoveredCommentDraft; const workpadOpen = $("agentWorkpad")?.open || false; renderKpis(); const issue = detail.issue; const id = newtype(issue.identifier); const current = issue.state.name; targetLane = targetLane || current; $("issueId").textContent = id; $("title").textContent = issue.title; $("stateChip").textContent = `${laneGlyph(current)} ${current.toUpperCase()}`; const dirty = targetLane !== current; $("dirtyFlag").hidden = !dirty; $("save").disabled = !dirty; $("save").innerHTML = dirty && isAction(targetLane) ? `SAVE & RUN ${escapeHtml(isAction(targetLane).toUpperCase())} <span class="meta">⌘S</span>` : `SAVE CHANGES <span class="meta">⌘S</span>`; $("diff").innerHTML = dirty ? `● 1 UNSAVED CHANGE&nbsp;&nbsp; Lane: ${escapeHtml(current)} → ${escapeHtml(targetLane)} ${isAction(targetLane) ? `<span class="trigger">▸ triggers ${escapeHtml(isAction(targetLane))} procedure</span>` : ""}` : "No changes"; $("props").innerHTML = `<div class="field ${dirty ? "dirty" : ""}"><div class="field-head"><span>LANE</span>${dirty ? "<span>● CHANGED</span>" : ""}</div><div class="value">${dirty ? `<del>${escapeHtml(current)}</del> → <strong>${escapeHtml(targetLane)}</strong>` : escapeHtml(current)}</div></div><div class="field"><div class="field-head"><span>LABELS</span></div><div>${issue.labels.map((l) => `<span class="label-chip">${escapeHtml(l)}</span>`).join("")}</div></div><div class="field"><div class="field-head"><span>ASSIGNEE</span></div><div>${escapeHtml(issue.assignee?.name || "Unassigned")}</div></div><div class="field"><div class="field-head"><span>BRANCH</span></div><div>${escapeHtml(newtype(issue.branch_name) || "None")}</div></div>`; $("middle").innerHTML = `<h2 class="section-title mono">DESCRIPTION</h2><div class="description">${escapeHtml(issue.description || "No description")}</div><h2 class="section-title mono">ACTIVITY</h2>${detail.comments.filter(comment => newtype(comment.comment_id) !== newtype(detail.workpad?.comment_id)).map(renderComment).join("") || '<div class="meta">No comments</div>'}<h2 class="section-title mono">ATTACHMENTS</h2><section class="attachment-drop-zone" id="issueAttachmentDropZone" data-attachment-drop-zone aria-label="Drop files to attach them to this issue"><label class="attachment-input-label" for="issueAttachmentInput"><span>Choose files</span><input id="issueAttachmentInput" type="file" multiple></label><div class="attachment-drop-affordance" aria-hidden="true">Drop files here to attach them to this issue</div><div class="attachment-list" id="issueAttachmentList" aria-live="polite"></div><div class="attachment-error meta" id="issueAttachmentError" role="alert"></div><button id="resolveIssueUpload" type="button" hidden>I've checked the issue</button></section><h2 class="section-title mono">NEW COMMENT</h2><section class="comment-composer" aria-label="New comment"><textarea id="commentEditor" class="issue-markdown-editor" spellcheck="true" placeholder="Add a comment…"></textarea><label class="attachment-input-label" for="commentAttachmentInput"><span>Attach files to this comment</span><input id="commentAttachmentInput" type="file" multiple></label><div class="attachment-list" id="commentAttachmentList" aria-live="polite"></div><div class="comment-actions"><div class="comment-error meta" id="commentError" role="alert"></div><button id="resolveComment" type="button" hidden>I've checked the issue</button><button class="comment-submit" id="postComment" type="button">POST COMMENT <span class="meta">C</span></button></div></section><details id="agentWorkpad" ${workpadOpen ? "open" : ""}><summary class="section-title mono">AGENT WORKPAD</summary><p class="meta">Execution notes and validation evidence maintained by the agents.</p><textarea id="workpadEditor" class="issue-markdown-editor" aria-label="Agent workpad" readonly>${escapeHtml(detail.workpad?.body_markdown || "No workpad yet.")}</textarea></details>`; $("lanePicker").innerHTML = detail.lanes.map((lane) => `<button class="lane-row ${targetLane === lane ? "target" : ""}" data-lane="${escapeHtml(lane)}" type="button"><span>${laneGlyph(lane)}</span><span>${escapeHtml(lane)}${isAction(lane) ? `<span class="sub">▸ runs ${escapeHtml(isAction(lane))} procedure</span>` : ""}</span><span class="meta">${lane === current ? "FROM" : targetLane === lane ? "NEW LANE" : ""}</span></button>`).join(""); $("commentEditor").value = commentDraft; renderIssueAttachments(); renderCommentAttachments(); installAttachmentDropTarget(); installCommentAttachmentInput(); renderCommentAvailability(); renderUploadAvailability(); };
  const load = async (preserveTarget = false) => { const previousTarget = targetLane; try { detail = await api(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}`); targetLane = preserveTarget && previousTarget ? previousTarget : detail.issue.state.name; render(); } catch (error) { showNotice(error.message); } };
  const postComment = async () => {
    const editor = $("commentEditor");
    if (!editor || commentSubmissionUncertain || commentSubmissionPending) return;
    const body = editor.value.trim();
    clearNotice();
    if (!body && commentAttachmentState.length === 0) {
      $("commentError").textContent = "Comment or attachment is required.";
      editor.focus();
      return;
    }
    const pending = commentAttachmentState.slice();
    let attemptError = "";
    commentSubmissionPending = true;
    commentSubmissionUncertain = true;
    commentSubmissionWarning = uncertainMessage;
    renderCommentAvailability();
    try {
      persistMutationRecovery();
      let options;
      if (pending.length) {
        const form = new FormData();
        form.append("body_markdown", body);
        for (const attachment of pending) form.append("attachments", attachment.file, attachment.name);
        options = {method:"POST", headers:{"Accept":"application/json","X-Fugue-Tracker-Scope":trackerScope}, body:form};
      } else {
        options = {method:"POST", headers:{"Accept":"application/json","Content-Type":"application/json","X-Fugue-Tracker-Scope":trackerScope}, body:JSON.stringify({body_markdown:body})};
      }
      const payload = await fugueIssueMutation(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/comments`, options, {kind:"comment", fileCount:pending.length});
      for (const attachment of pending) {
        const index = commentAttachmentState.indexOf(attachment);
        if (index >= 0) commentAttachmentState.splice(index, 1);
      }
      renderCommentAttachments();
      commentSubmissionWarning = payload.warning?.message || "";
      commentSubmissionUncertain = Boolean(commentSubmissionWarning);
      if (!commentSubmissionUncertain) {
        $("commentEditor").value = "";
        recoveredCommentDraft = "";
      }
      persistMutationRecovery();
      if (commentSubmissionWarning) showNotice(commentSubmissionWarning);
      await load(true);
    } catch (caught) {
      // Keep the pre-dispatch record on storage failure as well. Only a known
      // HTTP rejection permits another submission.
      if (caught.confirmedRejected) {
        commentSubmissionUncertain = false;
        commentSubmissionWarning = "";
      }
      attemptError = caught.message;
      try { persistMutationRecovery(); } catch (storageError) {
        commentSubmissionUncertain = true;
        commentSubmissionWarning = uncertainMessage;
        console.warn("Could not save comment recovery state", storageError);
      }
      showNotice(caught.message);
      $("commentError").textContent = caught.message;
    } finally {
      commentSubmissionPending = false;
      renderCommentAvailability();
      if (!commentSubmissionUncertain && attemptError) $("commentError").textContent = attemptError;
    }
  };
  document.addEventListener("click", (event) => {
    const upload = event.target.id === "resolveIssueUpload";
    const comment = event.target.id === "resolveComment";
    if ((!upload && !comment) || issueUploadPending || commentSubmissionPending) return;
    if (!window.confirm("Have you checked the existing issue for the comment and files? Continue only after reconciling the result. This clears the warning and pending files without resending anything. Your comment draft is kept.")) return;
    const previousUploadWarning = issueUploadWarning;
    const previousCommentWarning = commentSubmissionWarning;
    if (upload) issueUploadWarning = "";
    if (comment) { commentSubmissionWarning = ""; commentSubmissionUncertain = false; }
    try {
      persistMutationRecovery();
      if (comment) commentAttachmentState.length = 0;
      renderCommentAttachments(); renderCommentAvailability(); renderUploadAvailability();
      clearNotice();
    } catch (error) {
      issueUploadWarning = previousUploadWarning;
      commentSubmissionWarning = previousCommentWarning;
      commentSubmissionUncertain = Boolean(previousCommentWarning);
      showNotice("The reconciliation could not be saved. " + error.message);
    }
  });
  document.addEventListener("click", async (event) => { if (event.target.id === "close" || event.target.id === "cancel" || event.target === document.body) fugueNavigate("/issues"); const row = event.target.closest(".lane-row"); if (row) { targetLane = row.dataset.lane; render(); } if (event.target.dataset.commentAttachmentRemove) { commentAttachmentState.splice(Number(event.target.dataset.commentAttachmentRemove), 1); renderCommentAttachments(); } if (event.target.id === "postComment") { await postComment(); } if (event.target.id === "save" && detail && targetLane !== detail.issue.state.name) { try { await api(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/state`, { method:"PATCH", body: JSON.stringify({ state: targetLane }) }); fugueNavigate("/issues"); } catch (error) { showNotice(error.message); } } });
  document.addEventListener("keydown", async (event) => { const target = event.target; const editing = target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement; if (event.key === "Escape") fugueNavigate("/issues"); if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "s") { event.preventDefault(); $("save").click(); } else if (!editing && event.key.toLowerCase() === "c") { event.preventDefault(); $("commentEditor")?.focus(); } });
  load();
</script>
</body>
</html>"#;
fn meta_console_decision_state() -> Result<meerkat_meta::RuntimeDecisionState, String> {
    // Authentication is enforced by Fugue's outer HTTP middleware. Do not mint
    // a fictitious OIDC issuer or signing key for a locally embedded console.
    Ok(meerkat_meta::RuntimeDecisionState::local_console(
        ConsolePolicy {
            require_app_auth: false,
            ..ConsolePolicy::default()
        },
        None,
    ))
}

fn meta_console_enabled(config: &ServiceConfig) -> bool {
    config
        .server
        .as_ref()
        .is_none_or(|server| server.meta_console.enabled)
}

#[derive(Debug, Serialize)]
struct IssueBoardSnapshot {
    generated_at: chrono::DateTime<Utc>,
    lanes: Vec<IssueBoardLane>,
    totals: IssueBoardTotals,
}

#[derive(Debug, Serialize)]
struct IssueBoardTotals {
    issues: usize,
    running: usize,
    retrying: usize,
}

#[derive(Debug, Serialize)]
struct IssueBoardLane {
    name: String,
    kind: IssueBoardLaneKind,
    action: Option<LaneAction>,
    issues: Vec<IssueBoardIssue>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum IssueBoardLaneKind {
    Parked,
    Intake,
    Active,
    Attention,
    Terminal,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LaneAction {
    Review,
    Merge,
    Deploy,
}

fn lane_action(config: &ServiceConfig, lane: &str) -> Option<LaneAction> {
    if config.tracker.kind == "jira" {
        // Only configured action statuses trigger a procedure. A Jira status
        // named "Deploy" or "Review" is not itself authorization to run one.
        if config.deploy.enabled && lane.eq_ignore_ascii_case(&config.deploy.lane) {
            return Some(if config.deploy.procedure == "merge-only" {
                LaneAction::Merge
            } else {
                LaneAction::Deploy
            });
        }
        if (config.final_review.enabled || config.independent_review.enabled)
            && config
                .linear
                .lanes
                .do_review
                .as_ref()
                .or(config.linear.lanes.review_ready.as_ref())
                .is_some_and(|state| state.eq_ignore_ascii_case(lane))
        {
            return Some(LaneAction::Review);
        }
        return None;
    }
    match lane {
        "Review" if config.final_review.enabled || config.independent_review.enabled => {
            Some(LaneAction::Review)
        }
        "Deploy" if config.deploy.enabled && config.deploy.procedure == "merge-only" => {
            Some(LaneAction::Merge)
        }
        "Deploy" if config.deploy.enabled => Some(LaneAction::Deploy),
        _ => None,
    }
}

#[derive(Debug, Serialize)]
struct IssueBoardIssue {
    issue_id: String,
    identifier: String,
    title: String,
    lane: String,
    priority: Option<i64>,
    url: Option<String>,
    branch_name: Option<String>,
    labels: Vec<String>,
    blocked_by: Vec<IssueBoardBlocker>,
    updated_at: Option<chrono::DateTime<Utc>>,
    runtime: Option<IssueBoardRuntimeStatus>,
}

#[derive(Debug, Serialize)]
struct IssueBoardBlocker {
    identifier: Option<String>,
    state: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum IssueBoardRuntimeStatus {
    Running {
        backend: String,
        policy_mode: String,
        turn_count: u64,
        last_event: Option<String>,
        last_message: Option<String>,
        token_total: u64,
    },
    Retrying {
        policy_mode: String,
        attempt: u32,
        due_at: chrono::DateTime<Utc>,
        error: Option<String>,
    },
}

#[derive(Debug, Serialize)]
struct IssueDetailSnapshot {
    issue: crate::tools::LinearIssueSnapshot,
    workpad: Option<crate::tools::LinearWorkpadSnapshot>,
    comments: Vec<LinearIssueCommentSnapshot>,
    #[serde(default)]
    attachments: Vec<crate::tools::LinearFileAttachmentSnapshot>,
    runtime: Option<IssueBoardRuntimeStatus>,
    lanes: Vec<String>,
    lane_actions: BTreeMap<String, LaneAction>,
}

#[derive(Debug, Deserialize)]
struct IssueCreateRequest {
    title: String,
    #[serde(default)]
    description: Option<String>,
    lane: String,
    #[serde(default)]
    attachments: Vec<IssueCreateAttachmentRequest>,
}

#[derive(Debug, Deserialize)]
struct IssueCreateAttachmentRequest {
    name: String,
    size: u64,
    #[serde(default)]
    content_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IssueMoveRequest {
    state: String,
    #[serde(default)]
    expected_updated_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct IssueWorkpadRequest {
    body_markdown: String,
    #[serde(default)]
    heading: Option<String>,
    #[serde(default)]
    create_if_missing: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct IssueCommentRequest {
    body_markdown: String,
    #[serde(default)]
    attachments: Vec<LinearCommentAttachmentInput>,
}

struct ParsedIssueCommentRequest {
    body_markdown: String,
    attachments: Vec<LinearCommentAttachmentInput>,
    uploads: Vec<IssueUploadedAttachment>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum IssueCreateResponse {
    Created {
        issue: Box<IssueBoardIssue>,
    },
    CreatedWithWarning {
        created_issue: crate::domain::CreatedIssueReceipt,
        warning: IssueCreationWarning,
    },
}

#[derive(Serialize)]
struct IssueCreationWarning {
    code: &'static str,
    message: String,
}

async fn api_issue_create(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IssueCreateRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    if let Err(response) = validate_browser_tracker_context(&app.config, &headers) {
        return response.into_response();
    }
    let (title, lane) =
        match validate_issue_create_request(&app.config, &request.title, &request.lane) {
            Ok(valid) => valid,
            Err((code, message)) => return bad_request(code, &message),
        };
    if !request.attachments.is_empty()
        && let Err((code, message)) = validate_issue_create_attachments(&request.attachments)
    {
        return bad_request(code, &message);
    }
    let description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let adapter = match IssueAdapter::from_config(&app.config.tracker) {
        Ok(adapter) => adapter,
        Err(error) => return linear_config_error_response(error),
    };
    match adapter
        .create_issue(
            &app.config.tracker,
            title,
            description,
            &tracker_state_for_canonical_lane(&app.config, &lane),
        )
        .await
    {
        Ok(issue) => {
            let lane = canonical_lane_for_state(&app.config, &issue.state);
            (
                StatusCode::CREATED,
                Json(IssueCreateResponse::Created {
                    issue: Box::new(issue_board_issue(
                        issue,
                        &lane,
                        &HashMap::new(),
                        &HashMap::new(),
                    )),
                }),
            )
                .into_response()
        }
        Err(crate::error::FugueError::IssueCreatedNeedsAttention { created, source }) => {
            tracing::warn!(issue = %created.identifier, error = %source, "created issue needs follow-up");
            created_issue_attention_response(created)
        }
        Err(error) => issue_adapter_error_response(error),
    }
}

fn created_issue_attention_response(
    created: crate::domain::CreatedIssueReceipt,
) -> axum::response::Response {
    let message = format!(
        "Created {}. Fugue could not confirm its initial status. Open the existing issue to check it.",
        created.identifier,
    );
    (
        StatusCode::CREATED,
        Json(IssueCreateResponse::CreatedWithWarning {
            created_issue: created,
            warning: IssueCreationWarning {
                code: "issue_created_followup_failed",
                message,
            },
        }),
    )
        .into_response()
}

async fn api_issue_board(
    State(app): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    match issue_board_snapshot(&app).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": {
                    "code": "issue_board_unavailable",
                    "message": error.to_string()
                }
            })),
        )
            .into_response(),
    }
}

async fn api_issue_detail(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    match issue_detail_snapshot(&app, &issue_identifier).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(response) => response,
    }
}

async fn api_issue_move(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
    Json(request): Json<IssueMoveRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let state = request.state.trim();
    if state.is_empty() {
        return bad_request("missing_state", "state must be non-empty");
    }
    let adapter = match IssueAdapter::from_config(&app.config.tracker) {
        Ok(adapter) => adapter,
        Err(error) => return linear_config_error_response(error),
    };
    match adapter
        .move_issue_as_operator(MoveIssueInput {
            issue: issue_ref_for_identifier(&issue_identifier),
            state: LinearStateRef::Name {
                name: tracker_state_for_canonical_lane(&app.config, state),
            },
            expected_updated_at: request.expected_updated_at,
            idempotency_key: generated_idempotency_key("issue-board-move", &issue_identifier),
        })
        .await
    {
        Ok(output) => Json(json!({ "result": output })).into_response(),
        Err(error) => issue_adapter_error_response(error),
    }
}

async fn api_issue_upsert_workpad(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
    Json(request): Json<IssueWorkpadRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let adapter = match IssueAdapter::from_config(&app.config.tracker) {
        Ok(adapter) => adapter,
        Err(error) => return linear_config_error_response(error),
    };
    match adapter
        .upsert_workpad(UpsertWorkpadInput {
            issue: issue_ref_for_identifier(&issue_identifier),
            heading: request
                .heading
                .unwrap_or_else(|| "## Fugue Workpad".to_string()),
            body_markdown: request.body_markdown,
            create_if_missing: request.create_if_missing.unwrap_or(true),
            idempotency_key: generated_idempotency_key("issue-board-workpad", &issue_identifier),
        })
        .await
    {
        Ok(output) => Json(json!({ "result": output })).into_response(),
        Err(error) => issue_adapter_error_response(error),
    }
}

#[derive(Debug, Serialize)]
struct IssueAttachmentUploadResponse {
    attachments: Vec<IssueUploadedAttachment>,
    comment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<IssueUploadWarning>,
}

#[derive(Debug, Serialize)]
struct IssueUploadWarning {
    code: &'static str,
    message: &'static str,
}

#[derive(Debug, Serialize)]
struct IssueUploadedAttachment {
    name: String,
    size: u64,
    content_type: Option<String>,
    url: String,
    #[serde(skip_serializing)]
    path: Option<String>,
    #[serde(skip_serializing)]
    bytes: Vec<u8>,
}

impl From<LinearCommentAttachmentInput> for IssueUploadedAttachment {
    fn from(attachment: LinearCommentAttachmentInput) -> Self {
        Self {
            name: attachment.title.unwrap_or_else(|| "attachment".to_string()),
            size: attachment.byte_len,
            content_type: attachment.content_type,
            url: attachment.url,
            path: None,
            bytes: Vec::new(),
        }
    }
}

async fn publish_jira_uploads(
    client: &crate::jira::JiraClient,
    issue: &LinearIssueRef,
    uploaded: Vec<IssueUploadedAttachment>,
) -> crate::error::Result<crate::jira::JiraUploadReceipt> {
    client
        .upload_browser_files(
            issue,
            uploaded
                .into_iter()
                .map(|file| crate::jira::JiraUploadFile {
                    name: file.name,
                    content_type: file.content_type,
                    bytes: file.bytes,
                })
                .collect(),
        )
        .await
}

fn jira_upload_attention_response(
    attachments: Vec<LinearCommentAttachmentInput>,
    comment_attempted: bool,
) -> Response {
    Json(IssueAttachmentUploadResponse {
        attachments: attachments.into_iter().map(Into::into).collect(),
        comment_id: None,
        warning: Some(if comment_attempted {
            IssueUploadWarning { code: "comment_not_confirmed", message:
                "Jira accepted the files, but the comment was not confirmed. Check the existing issue before posting the comment again. Do not upload these files again." }
        } else {
            IssueUploadWarning { code: "upload_not_confirmed", message:
                "Jira did not confirm the complete upload. Some files may already be attached. Check the existing issue before uploading any files again." }
        }),
    }).into_response()
}

async fn api_issue_upload_attachments(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
    mut multipart: Multipart,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    if let Err(response) = validate_browser_tracker_context(&app.config, &headers) {
        return response.into_response();
    }
    let (adapter, issue_identifier) =
        match attachment_issue_access(&app.config, &issue_identifier).await {
            Ok(access) => access,
            Err(response) => return response,
        };
    let issue_identifier = issue_identifier.0;
    let mut uploaded = Vec::new();
    let upload_dir = issue_attachment_dir(&app.config, &issue_identifier);
    if let Err(error) = tokio::fs::create_dir_all(&upload_dir).await {
        return server_error(
            "attachment_upload_failed",
            &format!("failed to prepare attachment storage: {error}"),
        );
    }
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(error) => {
                return bad_request(
                    "invalid_attachment_upload",
                    &format!("failed to read multipart upload: {error}"),
                );
            }
        };
        if field.name() != Some("attachments") {
            continue;
        }
        let original_name = field
            .file_name()
            .map(ToString::to_string)
            .unwrap_or_else(|| "attachment".to_string());
        let content_type = field.content_type().map(ToString::to_string);
        let bytes = match field.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                return bad_request(
                    "invalid_attachment_upload",
                    &format!("failed to read uploaded attachment: {error}"),
                );
            }
        };
        let request = IssueCreateAttachmentRequest {
            name: original_name.clone(),
            size: bytes.len() as u64,
            content_type: content_type.clone(),
        };
        let mut existing: Vec<IssueCreateAttachmentRequest> = uploaded
            .iter()
            .map(
                |attachment: &IssueUploadedAttachment| IssueCreateAttachmentRequest {
                    name: attachment.name.clone(),
                    size: attachment.size,
                    content_type: attachment.content_type.clone(),
                },
            )
            .collect();
        existing.push(request);
        if let Err((code, message)) = validate_issue_create_attachments(&existing) {
            return bad_request(code, &message);
        }
        let stored_name = stored_attachment_file_name(uploaded.len(), &original_name, &bytes);
        let path = upload_dir.join(&stored_name);
        if let Err(error) = tokio::fs::write(&path, &bytes).await {
            return server_error(
                "attachment_upload_failed",
                &format!("failed to persist attachment: {error}"),
            );
        }
        uploaded.push(IssueUploadedAttachment {
            name: original_name,
            size: bytes.len() as u64,
            content_type,
            url: format!(
                "/api/v1/issues/{}/attachments/{}",
                percent_encode_path_segment(&issue_identifier),
                percent_encode_path_segment(&stored_name)
            ),
            path: Some(path.display().to_string()),
            bytes: bytes.to_vec(),
        });
    }
    if uploaded.is_empty() {
        return bad_request(
            "missing_attachment",
            "at least one attachment file is required",
        );
    }
    if let IssueAdapter::Jira(client) = &adapter {
        return match publish_jira_uploads(
            client,
            &issue_ref_for_identifier(&issue_identifier),
            uploaded,
        )
        .await
        {
            Ok(receipt) if receipt.complete => Json(IssueAttachmentUploadResponse {
                attachments: receipt.attachments.into_iter().map(Into::into).collect(),
                comment_id: None,
                warning: None,
            })
            .into_response(),
            Ok(receipt) => jira_upload_attention_response(receipt.attachments, false),
            Err(error) => issue_adapter_error_response(error),
        };
    }
    let attachments = uploaded
        .iter()
        .map(|attachment| LinearCommentAttachmentInput {
            title: Some(attachment.name.clone()),
            url: attachment.url.clone(),
            path: attachment.path.clone(),
            content_type: attachment.content_type.clone(),
            byte_len: attachment.size,
        })
        .collect();
    let comment_id = match adapter
        .add_comment(AddCommentInput {
            issue: issue_ref_for_identifier(&issue_identifier),
            body_markdown: String::new(),
            attachments,
        })
        .await
    {
        Ok(output) => Some(output.comment_id.0),
        Err(error) => return issue_adapter_error_response(error),
    };
    Json(IssueAttachmentUploadResponse {
        attachments: uploaded,
        comment_id,
        warning: None,
    })
    .into_response()
}

async fn api_issue_attachment_download(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath((issue_identifier, file_name)): AxumPath<(String, String)>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    if file_name != sanitize_attachment_component(&file_name) {
        return bad_request("invalid_attachment_path", "attachment file name is invalid");
    }
    let (_, issue_identifier) = match attachment_issue_access(&app.config, &issue_identifier).await
    {
        Ok(access) => access,
        Err(response) => return response,
    };
    let path = issue_attachment_dir(&app.config, &issue_identifier.0).join(&file_name);
    match tokio::fs::read(&path).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            )],
            bytes,
        )
            .into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":{"code":"attachment_not_found","message":"attachment not found"}})),
        )
            .into_response(),
        Err(error) => server_error(
            "attachment_read_failed",
            &format!("failed to read attachment: {error}"),
        ),
    }
}

#[derive(Debug)]
struct TrackerContextChanged;

impl IntoResponse for TrackerContextChanged {
    fn into_response(self) -> Response {
        (StatusCode::CONFLICT, Json(json!({"error": {
            "code": "tracker_context_changed",
            "message": "The repository's issue tracker changed. Reload the page and check the original tracker before retrying."
        }}))).into_response()
    }
}

fn validate_browser_tracker_context(
    config: &ServiceConfig,
    headers: &HeaderMap,
) -> std::result::Result<(), TrackerContextChanged> {
    let scope = headers.get("x-fugue-tracker-scope");
    let browser_request = headers.contains_key("origin")
        || headers
            .keys()
            .any(|key| key.as_str().starts_with("sec-fetch-"));
    // Non-browser API clients retain the existing optional-header contract.
    // Browsers cannot suppress Origin/Fetch Metadata on these mutations, so
    // an already-open legacy page must refresh after tracker reconfiguration.
    if (browser_request && scope.is_none())
        || scope
            .is_some_and(|scope| scope.to_str().ok() != Some(config.tracker_context_id().as_str()))
    {
        return Err(TrackerContextChanged);
    }
    Ok(())
}

async fn attachment_issue_access(
    config: &ServiceConfig,
    identifier: &str,
) -> std::result::Result<(IssueAdapter, LinearIssueIdentifier), Response> {
    let adapter =
        IssueAdapter::from_config(&config.tracker).map_err(linear_config_error_response)?;
    let identifier = adapter
        .resolve_issue_identifier(&issue_ref_for_identifier(identifier))
        .await
        .map_err(issue_adapter_error_response)?;
    Ok((adapter, identifier))
}

fn issue_attachment_dir(config: &ServiceConfig, issue_identifier: &str) -> PathBuf {
    config
        .attachment_storage_root()
        .join("issue-attachments")
        .join(sanitize_attachment_component(issue_identifier))
}

fn stored_attachment_file_name(index: usize, original_name: &str, bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!(
        "{:03}-{}-{}",
        index + 1,
        hex_prefix(&digest, 12),
        sanitize_attachment_component(original_name)
    )
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    bytes
        .iter()
        .flat_map(|byte| {
            [
                b"0123456789abcdef"[(byte >> 4) as usize] as char,
                b"0123456789abcdef"[(byte & 0x0f) as usize] as char,
            ]
        })
        .take(len)
        .collect()
}

#[cfg(test)]
fn attachment_comment_markdown(attachments: &[IssueUploadedAttachment]) -> String {
    let mut body = String::from("Attached files:\n");
    for attachment in attachments {
        body.push_str(&format!(
            "- [{}]({}) ({} bytes)\n",
            attachment.name.replace('[', "\\[").replace(']', "\\]"),
            attachment.url,
            attachment.size
        ));
    }
    body
}

#[cfg(test)]
fn comment_body_with_attachments(
    body_markdown: &str,
    attachments: &[IssueUploadedAttachment],
) -> String {
    let body = body_markdown.trim();
    if attachments.is_empty() {
        return body.to_string();
    }
    let attachment_markdown = attachment_comment_markdown(attachments);
    if body.is_empty() {
        attachment_markdown
    } else {
        format!("{body}\n\n{attachment_markdown}")
    }
}

async fn parse_issue_comment_request(
    app: &AppState,
    issue_identifier: &str,
    headers: &HeaderMap,
    request: Request,
) -> std::result::Result<ParsedIssueCommentRequest, axum::response::Response> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if content_type.starts_with("multipart/form-data") {
        parse_multipart_issue_comment_request(app, issue_identifier, request).await
    } else {
        parse_json_issue_comment_request(request).await
    }
}

async fn parse_json_issue_comment_request(
    request: Request,
) -> std::result::Result<ParsedIssueCommentRequest, axum::response::Response> {
    let bytes = match to_bytes(request.into_body(), 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(bad_request(
                "invalid_comment",
                &format!("failed to read comment request body: {error}"),
            ));
        }
    };
    let request: IssueCommentRequest = match serde_json::from_slice(&bytes) {
        Ok(request) => request,
        Err(error) => {
            return Err(bad_request(
                "invalid_comment",
                &format!("failed to parse comment request JSON: {error}"),
            ));
        }
    };
    Ok(ParsedIssueCommentRequest {
        body_markdown: request.body_markdown,
        attachments: request.attachments,
        uploads: Vec::new(),
    })
}

async fn parse_multipart_issue_comment_request(
    app: &AppState,
    issue_identifier: &str,
    request: Request,
) -> std::result::Result<ParsedIssueCommentRequest, axum::response::Response> {
    let mut multipart = Multipart::from_request(request, app)
        .await
        .map_err(|error| {
            bad_request(
                "invalid_comment_multipart",
                &format!("failed to parse multipart comment request: {error}"),
            )
        })?;
    let mut body_markdown = String::new();
    let mut uploaded = Vec::new();
    let upload_dir = issue_attachment_dir(&app.config, issue_identifier);
    while let Some(field) = multipart.next_field().await.map_err(|error| {
        bad_request(
            "invalid_comment_multipart",
            &format!("failed to read multipart comment: {error}"),
        )
    })? {
        match field.name() {
            Some("body_markdown") => {
                body_markdown = field.text().await.map_err(|error| {
                    bad_request(
                        "invalid_comment",
                        &format!("failed to read comment body_markdown field: {error}"),
                    )
                })?;
            }
            Some("attachments") => {
                persist_issue_comment_attachment(
                    issue_identifier,
                    &upload_dir,
                    &mut uploaded,
                    field,
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(ParsedIssueCommentRequest {
        body_markdown,
        attachments: Vec::new(),
        uploads: uploaded,
    })
}

async fn persist_issue_comment_attachment(
    issue_identifier: &str,
    upload_dir: &Path,
    uploaded: &mut Vec<IssueUploadedAttachment>,
    field: axum::extract::multipart::Field<'_>,
) -> std::result::Result<(), axum::response::Response> {
    let original_name = field
        .file_name()
        .map(ToString::to_string)
        .unwrap_or_else(|| "attachment".to_string());
    let content_type = field.content_type().map(ToString::to_string);
    let bytes = field.bytes().await.map_err(|error| {
        bad_request(
            "invalid_attachment_upload",
            &format!("failed to read uploaded attachment: {error}"),
        )
    })?;
    let request = IssueCreateAttachmentRequest {
        name: original_name.clone(),
        size: bytes.len() as u64,
        content_type: content_type.clone(),
    };
    let mut existing: Vec<IssueCreateAttachmentRequest> = uploaded
        .iter()
        .map(|attachment| IssueCreateAttachmentRequest {
            name: attachment.name.clone(),
            size: attachment.size,
            content_type: attachment.content_type.clone(),
        })
        .collect();
    existing.push(request);
    if let Err((code, message)) = validate_issue_create_attachments(&existing) {
        return Err(bad_request(code, &message));
    }
    if let Err(error) = tokio::fs::create_dir_all(upload_dir).await {
        return Err(server_error(
            "attachment_upload_failed",
            &format!("failed to prepare attachment storage: {error}"),
        ));
    }
    let stored_name = stored_attachment_file_name(uploaded.len(), &original_name, &bytes);
    let path = upload_dir.join(&stored_name);
    if let Err(error) = tokio::fs::write(&path, &bytes).await {
        return Err(server_error(
            "attachment_upload_failed",
            &format!("failed to persist attachment: {error}"),
        ));
    }
    uploaded.push(IssueUploadedAttachment {
        name: original_name,
        size: bytes.len() as u64,
        content_type,
        url: format!(
            "/api/v1/issues/{}/attachments/{}",
            percent_encode_path_segment(issue_identifier),
            percent_encode_path_segment(&stored_name)
        ),
        path: Some(path.display().to_string()),
        bytes: bytes.to_vec(),
    });
    Ok(())
}

fn sanitize_attachment_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches(['.', '-']).to_string();
    if trimmed.is_empty() {
        "attachment".to_string()
    } else {
        trimmed
    }
}

fn percent_encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
                vec![byte as char]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

fn server_error(code: &str, message: &str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response()
}

async fn api_issue_add_comment(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
    request: Request,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    if let Err(response) = validate_browser_tracker_context(&app.config, &headers) {
        return response.into_response();
    }
    let (adapter, issue_identifier) =
        match attachment_issue_access(&app.config, &issue_identifier).await {
            Ok(access) => access,
            Err(response) => return response,
        };
    let issue_identifier = issue_identifier.0;
    let mut parsed =
        match parse_issue_comment_request(&app, &issue_identifier, &headers, request).await {
            Ok(parsed) => parsed,
            Err(response) => return response,
        };
    if parsed.body_markdown.trim().is_empty()
        && parsed.attachments.is_empty()
        && parsed.uploads.is_empty()
    {
        return bad_request(
            "missing_comment",
            "body_markdown or at least one attachment must be provided",
        );
    }
    let mut native_uploads = Vec::new();
    if !parsed.uploads.is_empty() {
        if let IssueAdapter::Jira(client) = &adapter {
            match publish_jira_uploads(
                client,
                &issue_ref_for_identifier(&issue_identifier),
                parsed.uploads,
            )
            .await
            {
                Ok(receipt) if receipt.complete => native_uploads = receipt.attachments,
                Ok(receipt) => return jira_upload_attention_response(receipt.attachments, false),
                Err(error) => return issue_adapter_error_response(error),
            }
            parsed.attachments.extend(native_uploads.clone());
        } else {
            parsed
                .attachments
                .extend(parsed.uploads.into_iter().map(|attachment| {
                    LinearCommentAttachmentInput {
                        title: Some(attachment.name),
                        url: attachment.url,
                        path: attachment.path,
                        content_type: attachment.content_type,
                        byte_len: attachment.size,
                    }
                }));
        }
    }
    match adapter
        .add_comment(AddCommentInput {
            issue: issue_ref_for_identifier(&issue_identifier),
            body_markdown: parsed.body_markdown,
            attachments: parsed.attachments.clone(),
        })
        .await
    {
        Ok(output) => {
            Json(json!({ "result": output, "attachments": parsed.attachments })).into_response()
        }
        Err(error) if !native_uploads.is_empty() => {
            tracing::warn!(error = %error, "Jira accepted files but comment was not confirmed");
            jira_upload_attention_response(native_uploads, true)
        }
        Err(error) => issue_adapter_error_response(error),
    }
}

async fn issue_detail_snapshot(
    app: &AppState,
    issue_identifier: &str,
) -> std::result::Result<IssueDetailSnapshot, axum::response::Response> {
    let adapter =
        IssueAdapter::from_config(&app.config.tracker).map_err(linear_config_error_response)?;
    let issue_ref = issue_ref_for_identifier(issue_identifier);
    let mut inspected = adapter
        .inspect_issue(InspectIssueInput {
            issue: issue_ref.clone(),
        })
        .await
        .map_err(issue_adapter_error_response)?;
    // Jira statuses may combine multiple roles, so preserve their real names.
    // Other trackers retain the existing canonical board projection.
    let canonical_lane = canonical_lane_for_state(&app.config, &inspected.issue.state.name);
    if let Some(phase) = phase_label_for_state(&inspected.issue.state.name)
        && !inspected
            .issue
            .labels
            .iter()
            .any(|label| label.eq_ignore_ascii_case(phase))
    {
        inspected.issue.labels.insert(0, phase.to_string());
    }
    inspected.issue.state.name = canonical_lane;
    let comments = adapter
        .issue_comments(&issue_ref)
        .await
        .map_err(issue_adapter_error_response)?;
    let runtime = runtime_status_for_issue(
        &app.state,
        &app.config,
        &inspected.issue.id.0,
        &inspected.issue.identifier.0,
    )
    .await;
    Ok(IssueDetailSnapshot {
        issue: inspected.issue,
        workpad: inspected.workpad,
        attachments: inspected.attachments,
        comments,
        runtime,
        lane_actions: issue_board_lanes(&app.config)
            .into_iter()
            .filter_map(|name| lane_action(&app.config, &name).map(|action| (name, action)))
            .collect(),
        lanes: issue_board_lanes(&app.config),
    })
}

async fn runtime_status_for_issue(
    state: &SharedState,
    config: &ServiceConfig,
    issue_id: &str,
    issue_identifier: &str,
) -> Option<IssueBoardRuntimeStatus> {
    let runtime = snapshot(state, config).await;
    runtime
        .running
        .iter()
        .find(|row| row.issue_id == issue_id || row.issue_identifier == issue_identifier)
        .map(running_issue_board_status)
        .or_else(|| {
            runtime
                .retrying
                .iter()
                .find(|row| row.issue_id == issue_id || row.issue_identifier == issue_identifier)
                .map(retrying_issue_board_status)
        })
}

async fn issue_board_snapshot(app: &AppState) -> anyhow::Result<IssueBoardSnapshot> {
    let fetch_lanes = tracker_fetch_lanes(&app.config);
    let adapter = IssueAdapter::from_config(&app.config.tracker)?;
    let issues = adapter
        .fetch_issue_board_issues_by_states(&fetch_lanes)
        .await?;
    let runtime = snapshot(&app.state, &app.config).await;
    Ok(issue_board_snapshot_from_rows(
        &app.config,
        issues,
        &runtime.running,
        &runtime.retrying,
        runtime.counts.running,
        runtime.counts.retrying,
    ))
}

fn issue_board_snapshot_from_rows(
    config: &ServiceConfig,
    issues: Vec<Issue>,
    running: &[RunningRow],
    retrying: &[RetryRow],
    running_count: usize,
    retrying_count: usize,
) -> IssueBoardSnapshot {
    let mut runtime_by_issue_id = HashMap::new();
    let mut runtime_by_identifier = HashMap::new();
    for row in running {
        let status = running_issue_board_status(row);
        runtime_by_issue_id.insert(row.issue_id.clone(), status.clone());
        runtime_by_identifier.insert(row.issue_identifier.clone(), status);
    }
    for row in retrying {
        let status = retrying_issue_board_status(row);
        runtime_by_issue_id.insert(row.issue_id.clone(), status.clone());
        runtime_by_identifier.insert(row.issue_identifier.clone(), status);
    }

    let lanes = issue_board_lanes(config);
    let mut grouped = lanes
        .iter()
        .map(|lane| (lane.clone(), Vec::new()))
        .collect::<IndexMap<_, _>>();
    let mut seen_issue_ids = BTreeSet::new();
    for issue in issues {
        seen_issue_ids.insert(issue.id.clone());
        let lane = canonical_lane_for_state(config, &issue.state);
        grouped
            .entry(lane.clone())
            .or_default()
            .push(issue_board_issue(
                issue,
                &lane,
                &runtime_by_issue_id,
                &runtime_by_identifier,
            ));
    }
    add_missing_runtime_rows(
        config,
        &mut grouped,
        &seen_issue_ids,
        running,
        retrying,
        &runtime_by_issue_id,
        &runtime_by_identifier,
    );

    let lanes = grouped
        .into_iter()
        .map(|(name, mut issues)| {
            sort_issue_board_issues(&mut issues);
            IssueBoardLane {
                kind: if config.tracker.kind == "jira" {
                    canonical_lane_kind(&canonical_role_for_state(config, &name))
                } else {
                    canonical_lane_kind(&name)
                },
                action: lane_action(config, &name),
                name,
                issues,
            }
        })
        .collect::<Vec<_>>();
    // Keep empty lanes visible so operators can move tickets into them.
    let issue_count = lanes.iter().map(|lane| lane.issues.len()).sum();
    IssueBoardSnapshot {
        generated_at: Utc::now(),
        lanes,
        totals: IssueBoardTotals {
            issues: issue_count,
            running: running_count,
            retrying: retrying_count,
        },
    }
}

fn validate_issue_create_request<'a>(
    config: &ServiceConfig,
    title: &'a str,
    lane: &str,
) -> std::result::Result<(&'a str, String), (&'static str, String)> {
    let title = title.trim();
    if title.is_empty() {
        return Err(("missing_title", "title must be non-empty".to_string()));
    }
    let lane = issue_creation_lane(config, lane).ok_or_else(|| {
        (
            "invalid_lane",
            format!(
                "lane must be one of: {}",
                issue_creation_lanes(config).join(", ")
            ),
        )
    })?;
    Ok((title, lane))
}

fn validate_issue_create_attachments(
    attachments: &[IssueCreateAttachmentRequest],
) -> std::result::Result<(), (&'static str, String)> {
    const MAX_FILES: usize = 20;
    const MAX_FILE_BYTES: u64 = 25 * 1024 * 1024;
    const MAX_TOTAL_BYTES: u64 = 100 * 1024 * 1024;

    if attachments.len() > MAX_FILES {
        return Err((
            "too_many_attachments",
            format!("at most {MAX_FILES} attachments can be added when creating an issue"),
        ));
    }
    let mut total = 0_u64;
    for attachment in attachments {
        if attachment.name.trim().is_empty() {
            return Err((
                "invalid_attachment",
                "attachment name must be non-empty".to_string(),
            ));
        }
        if attachment.size > MAX_FILE_BYTES {
            return Err((
                "attachment_too_large",
                format!(
                    "attachment {} exceeds the 25 MiB per-file limit",
                    attachment.name
                ),
            ));
        }
        total = total.saturating_add(attachment.size);
        if total > MAX_TOTAL_BYTES {
            return Err((
                "attachments_too_large",
                "attachments exceed the 100 MiB total limit".to_string(),
            ));
        }
        if let Some(content_type) = &attachment.content_type
            && content_type.len() > 255
        {
            return Err((
                "invalid_attachment",
                format!("attachment {} content type is too long", attachment.name),
            ));
        }
    }
    Ok(())
}

fn issue_creation_lanes(config: &ServiceConfig) -> Vec<String> {
    if config.tracker.kind == "jira" {
        let mut lanes = vec![config.linear.lanes.parked.clone()];
        push_unique_lane(&mut lanes, config.linear.lanes.ready.clone());
        return lanes;
    }
    vec!["Backlog".to_string(), "Queue".to_string()]
}

fn issue_creation_lane(config: &ServiceConfig, lane: &str) -> Option<String> {
    let requested = lane.trim();
    issue_creation_lanes(config)
        .into_iter()
        .find(|configured| configured.eq_ignore_ascii_case(requested))
}

fn issue_ref_for_identifier(issue_identifier: &str) -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier(issue_identifier.to_string()),
    }
}

fn generated_idempotency_key(prefix: &str, issue_identifier: &str) -> IdempotencyKey {
    IdempotencyKey(format!(
        "{prefix}:{issue_identifier}:{}:{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    ))
}

fn linear_config_error_response(error: crate::error::FugueError) -> axum::response::Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": {
                "code": "issue_adapter_unavailable",
                "message": error.to_string()
            }
        })),
    )
        .into_response()
}

fn issue_adapter_error_response(error: crate::error::FugueError) -> axum::response::Response {
    if let crate::error::FugueError::JiraApiStatus { status } = &error
        && matches!(
            status,
            400 | 401 | 403 | 404 | 405 | 409 | 413 | 415 | 422 | 429
        )
        && let Ok(status) = StatusCode::from_u16(*status)
    {
        return (
            status,
            Json(json!({"error": {
                "code": "jira_request_rejected", "message": error.to_string()
            }})),
        )
            .into_response();
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": {
                "code": "issue_adapter_unavailable",
                "message": error.to_string()
            }
        })),
    )
        .into_response()
}

fn bad_request(code: &str, message: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": {
                "code": code,
                "message": message
            }
        })),
    )
        .into_response()
}

fn running_issue_board_status(row: &RunningRow) -> IssueBoardRuntimeStatus {
    IssueBoardRuntimeStatus::Running {
        backend: format!("{:?}", row.backend),
        policy_mode: format!("{:?}", row.policy_mode),
        turn_count: row.turn_count,
        last_event: row.last_event.clone(),
        last_message: row.last_message.clone(),
        token_total: row.tokens.total_tokens,
    }
}

fn retrying_issue_board_status(row: &RetryRow) -> IssueBoardRuntimeStatus {
    IssueBoardRuntimeStatus::Retrying {
        policy_mode: format!("{:?}", row.policy_mode),
        attempt: row.attempt,
        due_at: row.due_at,
        error: row.error.clone(),
    }
}

fn issue_board_issue(
    issue: Issue,
    canonical_lane: &str,
    runtime_by_issue_id: &HashMap<String, IssueBoardRuntimeStatus>,
    runtime_by_identifier: &HashMap<String, IssueBoardRuntimeStatus>,
) -> IssueBoardIssue {
    let runtime = runtime_by_issue_id
        .get(&issue.id)
        .or_else(|| runtime_by_identifier.get(&issue.identifier))
        .cloned();
    IssueBoardIssue {
        issue_id: issue.id,
        identifier: issue.identifier,
        title: issue.title,
        lane: canonical_lane.to_string(),
        priority: issue.priority,
        url: issue.url,
        branch_name: issue.branch_name,
        labels: issue_labels_with_phase(issue.labels, &issue.state),
        blocked_by: issue
            .blocked_by
            .into_iter()
            .map(|blocker| IssueBoardBlocker {
                identifier: blocker.identifier,
                state: blocker.state,
            })
            .collect(),
        updated_at: issue.updated_at,
        runtime,
    }
}

fn issue_labels_with_phase(mut labels: Vec<String>, state: &str) -> Vec<String> {
    if let Some(phase) = phase_label_for_state(state)
        && !labels.iter().any(|label| label.eq_ignore_ascii_case(phase))
    {
        labels.insert(0, phase.to_string());
    }
    labels
}

fn add_missing_runtime_rows(
    config: &ServiceConfig,
    grouped: &mut IndexMap<String, Vec<IssueBoardIssue>>,
    seen_issue_ids: &BTreeSet<String>,
    running: &[RunningRow],
    retrying: &[RetryRow],
    runtime_by_issue_id: &HashMap<String, IssueBoardRuntimeStatus>,
    runtime_by_identifier: &HashMap<String, IssueBoardRuntimeStatus>,
) {
    for row in running {
        if seen_issue_ids.contains(&row.issue_id) {
            continue;
        }
        let issue = synthetic_issue_board_issue(
            &row.issue_id,
            &row.issue_identifier,
            &row.issue_identifier,
            &canonical_lane_for_state(config, &row.state),
            runtime_by_issue_id,
            runtime_by_identifier,
        );
        let lane = canonical_lane_for_state(config, &row.state);
        grouped.entry(lane).or_default().push(issue);
    }
    for row in retrying {
        if seen_issue_ids.contains(&row.issue_id) {
            continue;
        }
        let lane = if config.tracker.kind == "jira" {
            config
                .tracker
                .in_flight_states
                .first()
                .cloned()
                .unwrap_or_else(|| config.linear.lanes.ready.clone())
        } else {
            "Active".to_string()
        };
        let issue = synthetic_issue_board_issue(
            &row.issue_id,
            &row.issue_identifier,
            &row.issue_identifier,
            &lane,
            runtime_by_issue_id,
            runtime_by_identifier,
        );
        grouped.entry(lane).or_default().push(issue);
    }
}

fn synthetic_issue_board_issue(
    issue_id: &str,
    identifier: &str,
    title: &str,
    lane: &str,
    runtime_by_issue_id: &HashMap<String, IssueBoardRuntimeStatus>,
    runtime_by_identifier: &HashMap<String, IssueBoardRuntimeStatus>,
) -> IssueBoardIssue {
    IssueBoardIssue {
        issue_id: issue_id.to_string(),
        identifier: identifier.to_string(),
        title: title.to_string(),
        lane: lane.to_string(),
        priority: None,
        url: None,
        branch_name: None,
        labels: Vec::new(),
        blocked_by: Vec::new(),
        updated_at: None,
        runtime: runtime_by_issue_id
            .get(issue_id)
            .or_else(|| runtime_by_identifier.get(identifier))
            .cloned(),
    }
}

const FUGUE_CANONICAL_LANES: [&str; 8] = [
    "Backlog", "Queue", "Active", "Review", "Deploy", "Blocked", "Done", "Canceled",
];

fn canonical_issue_board_lanes() -> Vec<String> {
    FUGUE_CANONICAL_LANES
        .iter()
        .map(|lane| (*lane).to_string())
        .collect()
}

fn issue_board_lanes(config: &ServiceConfig) -> Vec<String> {
    if config.tracker.kind == "jira" {
        tracker_fetch_lanes(config)
    } else {
        canonical_issue_board_lanes()
    }
}

fn tracker_fetch_lanes(config: &ServiceConfig) -> Vec<String> {
    let mut lanes = Vec::new();
    push_unique_lane(&mut lanes, config.linear.lanes.parked.clone());
    for state in &config.tracker.intake_states {
        push_unique_lane(&mut lanes, state.clone());
    }
    for state in &config.tracker.in_flight_states {
        push_unique_lane(&mut lanes, state.clone());
    }
    for state in &config.tracker.active_states {
        push_unique_lane(&mut lanes, state.clone());
    }
    if let Some(review_ready) = &config.linear.lanes.review_ready {
        push_unique_lane(&mut lanes, review_ready.clone());
    }
    if let Some(blocked) = &config.linear.lanes.blocked {
        push_unique_lane(&mut lanes, blocked.clone());
    }
    if let Some(deploy) = &config.linear.lanes.deploy {
        push_unique_lane(&mut lanes, deploy.clone());
    }
    if config.deploy.enabled {
        push_unique_lane(&mut lanes, config.deploy.lane.clone());
    }
    for state in &config.tracker.terminal_states {
        push_unique_lane(&mut lanes, state.clone());
    }
    push_unique_lane(&mut lanes, config.linear.lanes.success.clone());
    push_unique_lane(&mut lanes, config.linear.lanes.canceled.clone());
    push_unique_lane(&mut lanes, config.linear.lanes.duplicate.clone());
    if lanes.is_empty() {
        for state in [
            "Backlog",
            "Todo",
            "In Progress",
            "In Review",
            "Ready for Deploy",
            "Blocked",
            "Done",
            "Canceled",
        ] {
            push_unique_lane(&mut lanes, state.to_string());
        }
    }
    lanes
}

fn canonical_lane_for_state(config: &ServiceConfig, state: &str) -> String {
    if config.tracker.kind == "jira" {
        state.to_string()
    } else {
        canonical_role_for_state(config, state)
    }
}

fn canonical_role_for_state(config: &ServiceConfig, state: &str) -> String {
    let normalized = state.trim().to_ascii_lowercase();
    if config.tracker.is_intake_state(state) {
        return "Queue".to_string();
    }
    if config.is_parked_state(state) || normalized == "backlog" || normalized == "icebox" {
        return "Backlog".to_string();
    }
    if config.tracker.is_intake_state(state)
        || normalized == "todo"
        || normalized == "queue"
        || normalized == "ready queue"
    {
        return "Queue".to_string();
    }
    if config
        .linear
        .lanes
        .blocked
        .as_ref()
        .is_some_and(|blocked| blocked.eq_ignore_ascii_case(state))
        || normalized == "blocked"
    {
        return "Blocked".to_string();
    }
    if config.tracker.is_terminal_state(state)
        || matches!(
            normalized.as_str(),
            "done" | "canceled" | "cancelled" | "duplicate"
        )
    {
        return if state.eq_ignore_ascii_case(&config.linear.lanes.success)
            || (config.deploy.enabled && state.eq_ignore_ascii_case(&config.deploy.success_lane))
            || normalized == "done"
        {
            "Done"
        } else {
            "Canceled"
        }
        .to_string();
    }
    if config
        .linear
        .lanes
        .deploy
        .as_ref()
        .is_some_and(|deploy| deploy.eq_ignore_ascii_case(state))
        || (config.deploy.enabled && state.eq_ignore_ascii_case(&config.deploy.lane))
        || matches!(normalized.as_str(), "deploy" | "ready for deploy")
    {
        return "Deploy".to_string();
    }
    if config
        .linear
        .lanes
        .review_ready
        .as_ref()
        .is_some_and(|review_ready| review_ready.eq_ignore_ascii_case(state))
        || matches!(
            normalized.as_str(),
            "review" | "in review" | "ready for review" | "do review" | "human review"
        )
    {
        return "Review".to_string();
    }
    "Active".to_string()
}

fn phase_label_for_state(state: &str) -> Option<&'static str> {
    match state.trim().to_ascii_lowercase().as_str() {
        "planning" => Some("fugue:phase:planning"),
        "implementing" | "in progress" | "rework" => Some("fugue:phase:implementing"),
        "reviewing" => Some("fugue:phase:reviewing"),
        "finalizing" => Some("fugue:phase:finalizing"),
        _ => None,
    }
}

fn canonical_lane_kind(lane: &str) -> IssueBoardLaneKind {
    match lane {
        "Backlog" => IssueBoardLaneKind::Parked,
        "Queue" => IssueBoardLaneKind::Intake,
        "Review" | "Deploy" | "Blocked" => IssueBoardLaneKind::Attention,
        "Done" | "Canceled" => IssueBoardLaneKind::Terminal,
        "Active" => IssueBoardLaneKind::Active,
        _ => IssueBoardLaneKind::Other,
    }
}

fn tracker_state_for_canonical_lane(config: &ServiceConfig, lane: &str) -> String {
    if config.tracker.kind == "jira"
        && let Some(state) = tracker_fetch_lanes(config)
            .into_iter()
            .find(|state| state.eq_ignore_ascii_case(lane.trim()))
    {
        return state;
    }
    match lane.trim().to_ascii_lowercase().as_str() {
        "backlog" => config.linear.lanes.parked.clone(),
        "queue" => config
            .tracker
            .intake_states
            .first()
            .cloned()
            .unwrap_or_else(|| config.linear.lanes.ready.clone()),
        "active" => config
            .tracker
            .in_flight_states
            .first()
            .cloned()
            .or_else(|| config.tracker.active_states.first().cloned())
            .unwrap_or_else(|| "In Progress".to_string()),
        "review" => config
            .linear
            .lanes
            .do_review
            .clone()
            .or_else(|| config.linear.lanes.review_ready.clone())
            .unwrap_or_else(|| "In Review".to_string()),
        "deploy" => config
            .linear
            .lanes
            .deploy
            .clone()
            .or_else(|| config.deploy.enabled.then(|| config.deploy.lane.clone()))
            .unwrap_or_else(|| "Ready for Deploy".to_string()),
        "blocked" => config
            .linear
            .lanes
            .blocked
            .clone()
            .unwrap_or_else(|| "Blocked".to_string()),
        "done" => config.linear.lanes.success.clone(),
        "canceled" | "cancelled" => config.linear.lanes.canceled.clone(),
        _ => lane.trim().to_string(),
    }
}

fn push_unique_lane(lanes: &mut Vec<String>, lane: String) {
    let trimmed = lane.trim();
    if trimmed.is_empty() {
        return;
    }
    if lanes
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(trimmed))
    {
        return;
    }
    lanes.push(trimmed.to_string());
}

fn sort_issue_board_issues(issues: &mut [IssueBoardIssue]) {
    issues.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.identifier.cmp(&right.identifier))
            .then_with(|| left.issue_id.cmp(&right.issue_id))
    });
}

async fn api_state(State(app): State<AppState>, headers: HeaderMap) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Json(snapshot(&app.state, &app.config).await).into_response()
}

async fn api_issue(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(issue_identifier): AxumPath<String>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    match issue_snapshot(&app.state, &issue_identifier).await {
        Some(snapshot) => (StatusCode::OK, Json(json!(snapshot))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "code": "issue_not_found",
                    "message": format!("issue not found in current runtime state: {issue_identifier}")
                }
            })),
        )
            .into_response(),
    }
}

async fn api_refresh(State(app): State<AppState>, headers: HeaderMap) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let requested_at = Utc::now();
    let queued = app.refresh_tx.try_send(()).is_ok();
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "queued": queued,
            "coalesced": !queued,
            "requested_at": requested_at,
            "operations": ["poll", "reconcile"]
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct KapellmeisterStartIssueRequest {
    issue_identifier: String,
}

async fn api_kapellmeister_start_issue(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<KapellmeisterStartIssueRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let issue_identifier = request.issue_identifier.trim().to_string();
    if issue_identifier.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "missing_issue_identifier",
                    "message": "issue_identifier must be non-empty"
                }
            })),
        )
            .into_response();
    }
    let (response_tx, response_rx) = oneshot::channel();
    if app
        .internal_tx
        .send(InternalMessage::KapellmeisterStartIssue {
            issue_identifier,
            response: response_tx,
        })
        .await
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_unavailable",
                    "message": "orchestrator command channel is closed"
                }
            })),
        )
            .into_response();
    }
    match timeout(Duration::from_secs(45), response_rx).await {
        Ok(Ok(result)) => {
            let status = if result.accepted {
                StatusCode::ACCEPTED
            } else {
                StatusCode::CONFLICT
            };
            (status, Json(json!({ "result": result }))).into_response()
        }
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_response_dropped",
                    "message": "orchestrator dropped the start issue response"
                }
            })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {
                    "issue_identifier": null,
                    "accepted": true,
                    "message": "Kapellmeister issue start command is still being processed"
                }
            })),
        )
            .into_response(),
    }
}

async fn api_kapellmeister_start_final_pr_gate(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FinalPrGateStartRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let (response_tx, response_rx) = oneshot::channel();
    if app
        .internal_tx
        .send(InternalMessage::KapellmeisterStartFinalPrGate {
            request,
            response: response_tx,
        })
        .await
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_unavailable",
                    "message": "orchestrator command channel is closed"
                }
            })),
        )
            .into_response();
    }
    match timeout(Duration::from_secs(45), response_rx).await {
        Ok(Ok(result)) => {
            let status = if result.accepted {
                StatusCode::ACCEPTED
            } else {
                StatusCode::CONFLICT
            };
            (status, Json(json!({ "result": result }))).into_response()
        }
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_response_dropped",
                    "message": "orchestrator dropped the final PR gate start response"
                }
            })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {
                    "issue_identifier": null,
                    "accepted": true,
                    "message": "Kapellmeister final PR gate start command is still being processed"
                }
            })),
        )
            .into_response(),
    }
}

async fn api_kapellmeister_start_independent_review(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndependentReviewStartRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let (response_tx, response_rx) = oneshot::channel();
    if app
        .internal_tx
        .send(InternalMessage::KapellmeisterStartIndependentReview {
            request,
            response: response_tx,
        })
        .await
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_unavailable",
                    "message": "orchestrator command channel is closed"
                }
            })),
        )
            .into_response();
    }
    match timeout(Duration::from_secs(45), response_rx).await {
        Ok(Ok(result)) => {
            let status = if result.accepted {
                StatusCode::ACCEPTED
            } else {
                StatusCode::CONFLICT
            };
            (status, Json(json!({ "result": result }))).into_response()
        }
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "orchestrator_response_dropped",
                    "message": "orchestrator dropped the independent review start response"
                }
            })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {
                    "issue_identifier": null,
                    "accepted": true,
                    "message": "Kapellmeister independent review start command is still being processed"
                }
            })),
        )
            .into_response(),
    }
}

async fn api_kapellmeister_admit_independent_review(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<crate::independent_review::IndependentReviewAdmissionRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    if !app.config.independent_review.enabled && !app.config.final_review.enabled {
        return bad_request(
            "independent_review_disabled",
            "Independent review is disabled for this repository.",
        );
    }
    let (candidate, _) = match crate::native_review::resolve_candidate(
        &app.config,
        request.issue,
        &request.repo,
        crate::tools::GitHubPullRequestRef::Number {
            number: request.pull_request,
        },
    )
    .await
    {
        Ok(candidate) => candidate,
        Err(error) => return bad_request("review_candidate_unavailable", &error.to_string()),
    };
    if candidate.head_sha != request.head_sha {
        return bad_request(
            "review_candidate_changed",
            "The PR head changed. Inspect it before requesting review.",
        );
    }
    let draft = crate::review_requests::ReviewRequestDraft {
        candidate,
        reviewers: request.reviewers,
        task: request.task,
        idempotency_key: request.idempotency_key,
        supersedes: request.supersedes,
    };
    match app.meta_runtime_registry.admit_native_review(draft).await {
        Ok((intent, native_review)) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {"accepted":true, "intent":intent, "native_review":native_review}
            })),
        )
            .into_response(),
        Err(error) => {
            use crate::review_requests::ReviewRequestError;
            let current = match &error {
                ReviewRequestError::CurrentChanged { current } => current.as_deref(),
                _ => None,
            };
            let message = match &error {
                ReviewRequestError::CurrentChanged { .. } => {
                    "A review request already exists. Explicitly supersede its current request ID."
                }
                ReviewRequestError::KeyConflict => {
                    "This idempotency key belongs to another assignment. Retry unchanged or supersede explicitly with a new key."
                }
                ReviewRequestError::Superseded => {
                    "This request has been superseded and cannot be restored by retrying."
                }
                ReviewRequestError::IneligibleReviewer => {
                    "Select existing repository specialists outside this issue's execution team, using their actual native profiles."
                }
                _ => {
                    "Review admission could not be confirmed. Retry the unchanged request and key; inspect native runs if it remains unavailable."
                }
            };
            tracing::warn!(%error, "native review admission rejected");
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "result": {"accepted":false, "message":message, "current_request":current}
                })),
            )
                .into_response()
        }
    }
}

async fn api_kapellmeister_complete_independent_review(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndependentReviewCompletionRequest>,
) -> axum::response::Response {
    complete_native_review_response(
        app,
        headers,
        request,
        crate::review_completion::ReviewCompletionKind::Independent,
    )
    .await
}

async fn api_kapellmeister_complete_final_pr_gate(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndependentReviewCompletionRequest>,
) -> axum::response::Response {
    complete_native_review_response(
        app,
        headers,
        request,
        crate::review_completion::ReviewCompletionKind::Final,
    )
    .await
}

async fn complete_native_review_response(
    app: AppState,
    headers: HeaderMap,
    request: IndependentReviewCompletionRequest,
    kind: crate::review_completion::ReviewCompletionKind,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    // Finish an accepted operation even if the browser disconnects. An unchanged
    // final-review retry also accepts an already-confirmed merge for projection.
    let completion = tokio::spawn(async move {
        let host = crate::github_tools::GitHubToolHost::with_connections(
            app.config.meerkat.runtime_connections.clone(),
        );
        crate::review_completion::complete(
            &app.config,
            &app.meta_runtime_registry,
            request,
            kind,
            host,
        )
        .await
    })
    .await;
    match completion {
        Ok(Ok(result)) => (StatusCode::ACCEPTED, Json(json!({"result":result}))).into_response(),
        Ok(Err(error)) => {
            let status = if error.code == "review_projection_failed" { StatusCode::BAD_GATEWAY } else { StatusCode::CONFLICT };
            (status, Json(json!({"result":{"accepted":false,"message":error.message}, "error":{"code":error.code}}))).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"result":{"accepted":false,"message":"Review completion was interrupted. Inspect current review and remote PR state before retrying."}}))).into_response(),
    }
}

async fn api_kapellmeister_deploy(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DeployStartRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let deploy_task = tokio::spawn(async move { execute_kapellmeister_deploy(app, request).await });
    match deploy_task.await {
        Ok(response) => response,
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "result": {
                    "accepted": false,
                    "issue_identifier": "unknown",
                    "message": format!("deploy task failed to complete: {error}"),
                    "failure_category": DeployFailureCategory::Unknown
                }
            })),
        )
            .into_response(),
    }
}

async fn execute_kapellmeister_deploy(
    app: AppState,
    request: DeployStartRequest,
) -> axum::response::Response {
    let result = execute_deploy_transaction(app.config, app.meta_runtime_registry, request).await;
    let status = if result.accepted {
        StatusCode::ACCEPTED
    } else {
        match result.failure_category {
            Some(DeployFailureCategory::MissingPrEvidence)
            | Some(DeployFailureCategory::WrongRepository)
            | Some(DeployFailureCategory::StaleHead)
            | Some(DeployFailureCategory::CiNotGreen)
            | Some(DeployFailureCategory::UnresolvedWorkGraph)
            | Some(DeployFailureCategory::MergeFailed)
            | Some(DeployFailureCategory::UnsafeDeploy) => StatusCode::CONFLICT,
            Some(DeployFailureCategory::OtaFailed)
            | Some(DeployFailureCategory::MissingAuth)
            | Some(DeployFailureCategory::Unknown)
            | None => StatusCode::BAD_GATEWAY,
        }
    };
    (status, Json(json!({ "result": result }))).into_response()
}

pub async fn execute_deploy_transaction(
    config: ServiceConfig,
    registry: MetaRuntimeRegistry,
    request: DeployStartRequest,
) -> DeployTransactionResult {
    let issue_identifier =
        issue_identifier_from_ref_http(&request.issue).unwrap_or_else(|| "unknown".to_string());
    if !config.deploy.enabled {
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: "deploy lane is disabled by configuration".to_string(),
            failure_category: Some(DeployFailureCategory::UnsafeDeploy),
            plan: None,
            outcome: None,
            ota: None,
        };
    }
    let issue_identifier =
        match crate::workgraph_gate::canonical_issue_identifier(&config.tracker, &request.issue)
            .await
        {
            Ok(identifier) => identifier,
            Err(error) => {
                return DeployTransactionResult {
                    accepted: false,
                    issue_identifier,
                    message: format!("Cannot resolve the deployment issue: {error}"),
                    failure_category: Some(DeployFailureCategory::UnsafeDeploy),
                    plan: None,
                    outcome: None,
                    ota: None,
                };
            }
        };
    let ota_update = config.ota_update.clone();
    let ota_update_enabled = ota_update.as_ref().is_some_and(|config| config.enabled);
    let plan = plan_deploy_gate(DeployGateInput {
        config: config.deploy.clone(),
        issue_identifier: issue_identifier.clone(),
        pull_request: Some(request.pull_request.clone()),
        recorded_head_sha: request.recorded_head_sha.clone(),
        ci_status: request.ci_status,
        unresolved_workgraph_blockers: request.unresolved_workgraph_blockers,
        ota_update_enabled,
        deploy_auth_available: ota_update.is_some(),
        idempotency_key: request.idempotency_key.clone(),
    });
    let Some(merge_input) = plan.merge.clone() else {
        let category = plan
            .failure_category
            .unwrap_or(DeployFailureCategory::Unknown);
        let outcome = deploy_failure_outcome(
            issue_identifier.clone(),
            &request,
            category,
            Utc::now(),
            Utc::now(),
        );
        if let Err(error) =
            record_failed_deploy_projection(&config, &request, &outcome, &plan.target_lane).await
        {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: None,
            };
        }
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: plan.reasons.join("; "),
            failure_category: outcome.failure_category,
            plan: Some(plan),
            outcome: Some(outcome),
            ota: None,
        };
    };
    let merge_result = crate::merge_gate::merge(
        &config,
        &registry,
        request.issue.clone(),
        merge_input,
        crate::merge_gate::MergePurpose::Deploy,
    )
    .await;
    let merge_output = match merge_result {
        crate::tools::FugueToolResult::Ok {
            output: FugueToolOutput::GitHubMergePr(output),
            ..
        } => output,
        crate::tools::FugueToolResult::Ok { output, .. } => {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: format!("github_merge_pr returned unexpected output {output:?}"),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: Some(plan),
                outcome: None,
                ota: None,
            };
        }
        crate::tools::FugueToolResult::Err { error, .. } => {
            let outcome = deploy_failure_outcome(
                issue_identifier.clone(),
                &request,
                DeployFailureCategory::MergeFailed,
                Utc::now(),
                Utc::now(),
            );
            if let Err(projection_error) = record_failed_deploy_projection(
                &config,
                &request,
                &outcome,
                &config.deploy.rework_lane,
            )
            .await
            {
                return DeployTransactionResult {
                    accepted: false,
                    issue_identifier,
                    message: projection_error.to_string(),
                    failure_category: Some(DeployFailureCategory::Unknown),
                    plan: Some(plan),
                    outcome: Some(outcome),
                    ota: None,
                };
            }
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.message,
                failure_category: Some(DeployFailureCategory::MergeFailed),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: None,
            };
        }
    };
    let merged_sha = match merge_output.merge_commit_sha.clone() {
        Some(sha) => sha,
        None => {
            let outcome = deploy_failure_outcome(
                issue_identifier.clone(),
                &request,
                DeployFailureCategory::UnsafeDeploy,
                Utc::now(),
                Utc::now(),
            );
            if let Err(error) = record_failed_deploy_projection(
                &config,
                &request,
                &outcome,
                &config.deploy.blocked_lane,
            )
            .await
            {
                return DeployTransactionResult {
                    accepted: false,
                    issue_identifier,
                    message: error.to_string(),
                    failure_category: Some(DeployFailureCategory::Unknown),
                    plan: Some(plan),
                    outcome: Some(outcome),
                    ota: None,
                };
            }
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: "github merge completed but did not return a merge commit sha; refusing OTA deploy".to_string(),
                failure_category: Some(DeployFailureCategory::UnsafeDeploy),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: None,
            };
        }
    };
    if plan.decision == crate::deploy_gate::DeployGateDecision::MergeOnly {
        let outcome = merge_only_success_outcome(
            issue_identifier.clone(),
            &request,
            merged_sha,
            config.deploy.merge_method,
        );
        if let Err(error) = record_successful_deploy_projection(&config, &request, &outcome).await {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: None,
            };
        }
        return DeployTransactionResult {
            accepted: true,
            issue_identifier,
            message: "PR merged; no deployment requested".to_string(),
            failure_category: None,
            plan: Some(plan),
            outcome: Some(outcome),
            ota: None,
        };
    }
    let Some(ota_config) = ota_update else {
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: "ota_update is not configured".to_string(),
            failure_category: Some(DeployFailureCategory::UnsafeDeploy),
            plan: Some(plan),
            outcome: None,
            ota: None,
        };
    };
    // The receipt retains deployment ownership through every projection below.
    let receipt = match crate::deployment_receipts::run(&config, &request, &merged_sha).await {
        Ok(receipt) => receipt,
        Err(error) => {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::UnsafeDeploy),
                plan: Some(plan),
                outcome: None,
                ota: None,
            };
        }
    };
    let ota_report = match &receipt.outcome {
        Ok(report) => report.clone(),
        Err(error) => {
            let mut outcome = deploy_failure_outcome(
                issue_identifier.clone(),
                &request,
                DeployFailureCategory::OtaFailed,
                Utc::now(),
                Utc::now(),
            );
            outcome.merge = Some(crate::deploy_gate::DeployMergeEvidence {
                method: config.deploy.merge_method,
                merged_sha: merged_sha.clone(),
            });
            if let Err(projection_error) = record_failed_deploy_projection(
                &config,
                &request,
                &outcome,
                &config.deploy.blocked_lane,
            )
            .await
            {
                return DeployTransactionResult {
                    accepted: false,
                    issue_identifier,
                    message: projection_error.to_string(),
                    failure_category: Some(DeployFailureCategory::Unknown),
                    plan: Some(plan),
                    outcome: Some(outcome),
                    ota: None,
                };
            }
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::OtaFailed),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: None,
            };
        }
    };
    if ota_report.status != OtaUpdateStatus::Succeeded {
        let outcome = deploy_ota_failure_outcome(
            issue_identifier.clone(),
            &request,
            &merge_output,
            &ota_report,
            config.deploy.merge_method,
        );
        if let Err(error) = record_failed_deploy_projection(
            &config,
            &request,
            &outcome,
            &config.deploy.blocked_lane,
        )
        .await
        {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: Some(ota_report),
            };
        }
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: ota_report.message.clone(),
            failure_category: Some(DeployFailureCategory::OtaFailed),
            plan: Some(plan),
            outcome: Some(outcome),
            ota: Some(ota_report),
        };
    }
    if let Err(error) = validate_ota_deployed_merge_sha(&merged_sha, &ota_report) {
        let outcome = deploy_post_merge_ota_failure_outcome(
            issue_identifier.clone(),
            &request,
            merged_sha,
            &ota_report,
            Some(&ota_config),
            config.deploy.merge_method,
            DeployFailureCategory::UnsafeDeploy,
            error.to_string(),
        );
        if let Err(projection_error) = record_failed_deploy_projection(
            &config,
            &request,
            &outcome,
            &config.deploy.blocked_lane,
        )
        .await
        {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: projection_error.to_string(),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: Some(plan),
                outcome: Some(outcome),
                ota: Some(ota_report),
            };
        }
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: error.to_string(),
            failure_category: Some(DeployFailureCategory::UnsafeDeploy),
            plan: Some(plan),
            outcome: Some(outcome),
            ota: Some(ota_report),
        };
    }
    let outcome = deploy_success_outcome(
        issue_identifier.clone(),
        &request,
        merged_sha,
        &ota_report,
        Some(&ota_config),
        config.deploy.merge_method,
    );
    if let Err(error) = record_successful_deploy_projection(&config, &request, &outcome).await {
        return DeployTransactionResult {
            accepted: false,
            issue_identifier,
            message: error.to_string(),
            failure_category: Some(DeployFailureCategory::Unknown),
            plan: Some(plan),
            outcome: Some(outcome),
            ota: Some(ota_report),
        };
    }
    DeployTransactionResult {
        accepted: true,
        issue_identifier,
        message: "merge and OTA deploy completed".to_string(),
        failure_category: None,
        plan: Some(plan),
        outcome: Some(outcome),
        ota: Some(ota_report),
    }
}

async fn record_successful_deploy_projection(
    config: &ServiceConfig,
    request: &DeployStartRequest,
    outcome: &DeployOutcome,
) -> anyhow::Result<()> {
    record_deploy_projection(
        config,
        request,
        outcome,
        &config.deploy.success_lane,
        "deploy-success",
    )
    .await
}

async fn record_failed_deploy_projection(
    config: &ServiceConfig,
    request: &DeployStartRequest,
    outcome: &DeployOutcome,
    target_lane: &str,
) -> anyhow::Result<()> {
    record_deploy_projection(config, request, outcome, target_lane, "deploy-failure").await
}

async fn record_deploy_projection(
    config: &ServiceConfig,
    request: &DeployStartRequest,
    outcome: &DeployOutcome,
    target_lane: &str,
    move_key_prefix: &str,
) -> anyhow::Result<()> {
    outcome
        .validate()
        .map_err(|error| anyhow::anyhow!("invalid deploy outcome: {error:?}"))?;
    let adapter = IssueAdapter::from_config(&config.tracker)?;
    let evidence = serde_json::to_string_pretty(outcome)?;
    let inspected = adapter
        .inspect_issue(InspectIssueInput {
            issue: request.issue.clone(),
        })
        .await?;
    let deployment = format!(
        "### Deployment\n\nIdempotency-Key: `{}`\n\n```json\n{evidence}\n```",
        request.idempotency_key.0
    );
    let body_markdown = crate::orchestrator::upsert_markdown_section(
        inspected
            .workpad
            .as_ref()
            .map_or("", |workpad| workpad.body_markdown.as_str()),
        "### Deployment",
        &deployment,
    );
    adapter
        .upsert_workpad(UpsertWorkpadInput {
            issue: request.issue.clone(),
            heading: "## Fugue Workpad".to_string(),
            body_markdown,
            create_if_missing: true,
            idempotency_key: IdempotencyKey(format!(
                "deploy-workpad:{}:{}",
                outcome.issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;
    let movement = MoveIssueInput {
        issue: request.issue.clone(),
        state: LinearStateRef::Name {
            name: target_lane.to_string(),
        },
        expected_updated_at: None,
        idempotency_key: IdempotencyKey(format!(
            "{}:{}:{}",
            move_key_prefix, outcome.issue_identifier, request.idempotency_key.0
        )),
    };
    let mut graph = crate::workgraph_gate::WorkGraphTerminalGate::for_service(config);
    graph
        .terminal_states
        .push(config.deploy.success_lane.clone());
    graph
        .allowed_terminal_states
        .push(config.deploy.blocked_lane.clone());
    if outcome.status == DeployOutcomeStatus::Succeeded && !graph.state_path.is_file() {
        anyhow::bail!(
            "The repository's native WorkGraph database is unavailable before completion"
        );
    }
    graph
        .guard_terminal_move(&config.tracker, &movement)
        .await
        .map_err(anyhow::Error::msg)?;
    adapter.move_issue(movement).await?;
    Ok(())
}

#[cfg(test)]
async fn deploy_idempotency_already_recorded(
    config: &ServiceConfig,
    issue: &LinearIssueRef,
    idempotency_key: &IdempotencyKey,
) -> anyhow::Result<bool> {
    let adapter = IssueAdapter::from_config(&config.tracker)?;
    let inspected = adapter
        .inspect_issue(InspectIssueInput {
            issue: issue.clone(),
        })
        .await?;
    Ok(inspected.workpad.as_ref().is_some_and(|workpad| {
        deploy_success_idempotency_recorded_in_workpad(&workpad.body_markdown, idempotency_key)
    }))
}

#[cfg(test)]
fn deploy_success_idempotency_recorded_in_workpad(
    body_markdown: &str,
    idempotency_key: &IdempotencyKey,
) -> bool {
    let marker = format!("Idempotency-Key: `{}`", idempotency_key.0);
    let Some(marker_index) = body_markdown.find(&marker) else {
        return false;
    };
    let body_after_marker = &body_markdown[marker_index..];
    let Some(fence_start) = body_after_marker.find("```json") else {
        return false;
    };
    let json_after_fence = &body_after_marker[fence_start + "```json".len()..];
    let Some(fence_end) = json_after_fence.find("```") else {
        return false;
    };
    let json_body = json_after_fence[..fence_end].trim();
    match serde_json::from_str::<DeployOutcome>(json_body) {
        Ok(outcome) => {
            matches!(
                outcome.status,
                DeployOutcomeStatus::Succeeded | DeployOutcomeStatus::Merged
            ) && outcome.validate().is_ok()
        }
        Err(_) => false,
    }
}

fn deploy_failure_outcome(
    issue_identifier: String,
    request: &DeployStartRequest,
    failure_category: DeployFailureCategory,
    started_at: chrono::DateTime<Utc>,
    completed_at: chrono::DateTime<Utc>,
) -> DeployOutcome {
    DeployOutcome {
        schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
        issue_identifier,
        status: DeployOutcomeStatus::Failed,
        failure_category: Some(failure_category),
        pull_request: DeployPullRequestEvidence {
            repo: request.pull_request.repo.clone(),
            number: request.pull_request.number,
            url: request.pull_request.url.clone(),
            base_branch: request.pull_request.base_branch.clone(),
            head_branch: request.pull_request.head_branch.clone(),
            head_sha: request.pull_request.head_sha.clone(),
        },
        merge: None,
        ota: None,
        started_at,
        completed_at,
    }
}

fn deploy_ota_failure_outcome(
    issue_identifier: String,
    request: &DeployStartRequest,
    merge: &MergePrOutput,
    ota: &OtaUpdateReport,
    merge_method: crate::tools::MergeMethod,
) -> DeployOutcome {
    let merged_sha = merge
        .merge_commit_sha
        .clone()
        .unwrap_or_else(|| request.pull_request.head_sha.clone());
    let attempted_sha = ota
        .fetched_sha
        .clone()
        .or_else(|| ota.previous_sha.clone())
        .map(GitSha)
        .unwrap_or_else(|| merged_sha.clone());
    DeployOutcome {
        schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
        issue_identifier,
        status: DeployOutcomeStatus::Failed,
        failure_category: Some(DeployFailureCategory::OtaFailed),
        pull_request: DeployPullRequestEvidence {
            repo: request.pull_request.repo.clone(),
            number: request.pull_request.number,
            url: request.pull_request.url.clone(),
            base_branch: request.pull_request.base_branch.clone(),
            head_branch: request.pull_request.head_branch.clone(),
            head_sha: request.pull_request.head_sha.clone(),
        },
        merge: Some(crate::deploy_gate::DeployMergeEvidence {
            method: merge_method,
            merged_sha,
        }),
        ota: Some(DeployOtaEvidence {
            service_name: "meerkat-fugue".to_string(),
            deployed_checkout_sha: attempted_sha,
            installed_binary_hashes: deployed_binary_hashes_from_deploy_command(""),
            health_url: ota_health_evidence(ota, None),
            health_result: ota.message.clone(),
        }),
        started_at: ota.started_at,
        completed_at: ota.completed_at,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit host composition boundary"
)]
fn deploy_post_merge_ota_failure_outcome(
    issue_identifier: String,
    request: &DeployStartRequest,
    merged_sha: GitSha,
    ota: &OtaUpdateReport,
    ota_config: Option<&crate::config::OtaUpdateConfig>,
    merge_method: crate::tools::MergeMethod,
    failure_category: DeployFailureCategory,
    health_result: String,
) -> DeployOutcome {
    let attempted_sha = ota
        .fetched_sha
        .clone()
        .or_else(|| ota.previous_sha.clone())
        .map(GitSha)
        .unwrap_or_else(|| merged_sha.clone());
    DeployOutcome {
        schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
        issue_identifier,
        status: DeployOutcomeStatus::Failed,
        failure_category: Some(failure_category),
        pull_request: DeployPullRequestEvidence {
            repo: request.pull_request.repo.clone(),
            number: request.pull_request.number,
            url: request.pull_request.url.clone(),
            base_branch: request.pull_request.base_branch.clone(),
            head_branch: request.pull_request.head_branch.clone(),
            head_sha: request.pull_request.head_sha.clone(),
        },
        merge: Some(crate::deploy_gate::DeployMergeEvidence {
            method: merge_method,
            merged_sha,
        }),
        ota: Some(DeployOtaEvidence {
            service_name: "meerkat-fugue".to_string(),
            deployed_checkout_sha: attempted_sha,
            installed_binary_hashes: ota_config
                .map(|config| deployed_binary_hashes_from_deploy_command(&config.deploy_command))
                .unwrap_or_default(),
            health_url: ota_health_evidence(ota, ota_config),
            health_result,
        }),
        started_at: ota.started_at,
        completed_at: ota.completed_at,
    }
}

#[cfg(test)]
async fn record_independent_review_projection(
    config: &ServiceConfig,
    request: &IndependentReviewCompletionRequest,
    issue_identifier: &str,
    plan: &crate::independent_review::IndependentReviewAggregationPlan,
    verdicts: &[crate::independent_review::IndependentReviewVerdict],
) -> anyhow::Result<crate::review_completion::IndependentReviewProjectionResult> {
    crate::review_completion::record_review_projection(
        config,
        request,
        issue_identifier,
        plan,
        verdicts,
        None,
        crate::review_completion::ReviewCompletionKind::Independent,
    )
    .await
}

async fn api_ota_update(State(app): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let Some(config) = app.config.ota_update.clone() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "code": "ota_update_not_configured",
                    "message": "ota_update is not configured for this Fugue server"
                }
            })),
        )
            .into_response();
    };
    if let Err(response) = authorize_ota_update(&config, &headers) {
        return response;
    }
    match run_ota_update(&config).await {
        Ok(report) => {
            let status = match report.status {
                OtaUpdateStatus::Succeeded | OtaUpdateStatus::Disabled => StatusCode::OK,
                OtaUpdateStatus::Failed => StatusCode::CONFLICT,
            };
            (status, Json(json!(report))).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": {
                    "code": "ota_update_failed",
                    "message": err.to_string()
                }
            })),
        )
            .into_response(),
    }
}

fn merge_only_success_outcome(
    issue_identifier: String,
    request: &DeployStartRequest,
    merged_sha: GitSha,
    merge_method: crate::tools::MergeMethod,
) -> DeployOutcome {
    let now = Utc::now();
    DeployOutcome {
        schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
        issue_identifier,
        status: DeployOutcomeStatus::Merged,
        failure_category: None,
        pull_request: DeployPullRequestEvidence {
            repo: request.pull_request.repo.clone(),
            number: request.pull_request.number,
            url: request.pull_request.url.clone(),
            base_branch: request.pull_request.base_branch.clone(),
            head_branch: request.pull_request.head_branch.clone(),
            head_sha: request.pull_request.head_sha.clone(),
        },
        merge: Some(crate::deploy_gate::DeployMergeEvidence {
            method: merge_method,
            merged_sha,
        }),
        ota: None,
        started_at: now,
        completed_at: now,
    }
}

fn deploy_success_outcome(
    issue_identifier: String,
    request: &DeployStartRequest,
    merged_sha: GitSha,
    ota: &OtaUpdateReport,
    ota_config: Option<&crate::config::OtaUpdateConfig>,
    merge_method: crate::tools::MergeMethod,
) -> DeployOutcome {
    let deployed_sha = ota
        .fetched_sha
        .clone()
        .map(GitSha)
        .unwrap_or_else(|| merged_sha.clone());
    DeployOutcome {
        schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
        issue_identifier,
        status: DeployOutcomeStatus::Succeeded,
        failure_category: None,
        pull_request: DeployPullRequestEvidence {
            repo: request.pull_request.repo.clone(),
            number: request.pull_request.number,
            url: request.pull_request.url.clone(),
            base_branch: request.pull_request.base_branch.clone(),
            head_branch: request.pull_request.head_branch.clone(),
            head_sha: request.pull_request.head_sha.clone(),
        },
        merge: Some(crate::deploy_gate::DeployMergeEvidence {
            method: merge_method,
            merged_sha,
        }),
        ota: Some(DeployOtaEvidence {
            service_name: "meerkat-fugue".to_string(),
            deployed_checkout_sha: deployed_sha,
            installed_binary_hashes: ota_config
                .map(|config| deployed_binary_hashes_from_deploy_command(&config.deploy_command))
                .unwrap_or_default(),
            health_url: ota_health_evidence(ota, ota_config),
            health_result: ota.message.clone(),
        }),
        started_at: ota.started_at,
        completed_at: ota.completed_at,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeploySuccessEvidenceError {
    MissingOtaTargetSha,
    OtaTargetMismatch { merged_sha: GitSha, ota_sha: GitSha },
}

impl std::fmt::Display for DeploySuccessEvidenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOtaTargetSha => {
                formatter.write_str("OTA report did not include fetched target sha")
            }
            Self::OtaTargetMismatch {
                merged_sha,
                ota_sha,
            } => write!(
                formatter,
                "OTA fetched sha {} does not match GitHub merge commit sha {}",
                ota_sha.0, merged_sha.0
            ),
        }
    }
}

fn validate_ota_deployed_merge_sha(
    merged_sha: &GitSha,
    ota: &OtaUpdateReport,
) -> Result<(), DeploySuccessEvidenceError> {
    let Some(ota_sha) = ota.fetched_sha.as_ref().map(|sha| GitSha(sha.clone())) else {
        return Err(DeploySuccessEvidenceError::MissingOtaTargetSha);
    };
    if &ota_sha != merged_sha {
        return Err(DeploySuccessEvidenceError::OtaTargetMismatch {
            merged_sha: merged_sha.clone(),
            ota_sha,
        });
    }
    Ok(())
}

fn ota_health_evidence(
    ota: &OtaUpdateReport,
    ota_config: Option<&crate::config::OtaUpdateConfig>,
) -> String {
    ota_config
        .and_then(|config| config.health_check_command.as_ref())
        .filter(|command| !command.trim().is_empty())
        .map(|command| format!("command:{}", command.trim()))
        .unwrap_or_else(|| format!("repo:{}", ota.repo_path))
}

fn deployed_binary_hashes_from_deploy_command(command: &str) -> Vec<DeployBinaryHash> {
    let binary_names = [
        "meerkat-fugue",
        "fugue-linear-mcp",
        "fugue-github-mcp",
        "fugue-host-mcp",
    ];
    let mut paths = BTreeSet::new();
    for token in command.split_whitespace() {
        let cleaned = token.trim_matches(|character: char| {
            matches!(
                character,
                '"' | '\'' | ';' | '&' | '|' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        });
        if !cleaned.starts_with('/') {
            continue;
        }
        let path = PathBuf::from(cleaned);
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if binary_names.contains(&file_name) && path.is_file() {
            paths.insert(path);
        }
    }
    paths
        .into_iter()
        .filter_map(|path| binary_hash(&path))
        .collect()
}

fn binary_hash(path: &Path) -> Option<DeployBinaryHash> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return None,
    };
    let mut hasher = sha2::Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = match file.read(&mut buffer) {
            Ok(read) => read,
            Err(_) => return None,
        };
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Some(DeployBinaryHash {
        path: path.display().to_string(),
        sha256: format!("{:x}", hasher.finalize()),
    })
}

fn issue_identifier_from_ref_http(issue: &LinearIssueRef) -> Option<String> {
    match issue {
        LinearIssueRef::Identifier { identifier } => Some(identifier.0.clone()),
        LinearIssueRef::Id { .. } => None,
    }
}

async fn api_meta_rpc(
    headers: HeaderMap,
    Json(input): Json<MobkitRpcRequest>,
) -> impl IntoResponse {
    if let Err(response) = authorize_meta_rpc(&headers) {
        return *response;
    }
    forward_meta_rpc(input).await
}

async fn forward_meta_rpc(input: MobkitRpcRequest) -> axum::response::Response {
    if input
        .jsonrpc
        .as_deref()
        .is_some_and(|version| version != "2.0")
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "meta_rpc_invalid_jsonrpc",
                    "message": "Meta RPC jsonrpc version must be 2.0"
                }
            })),
        )
            .into_response();
    }
    if input.method.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "meta_rpc_invalid_method",
                    "message": "Meta RPC method must be a non-empty string"
                }
            })),
        )
            .into_response();
    }
    let addr = match std::env::var(META_RPC_TCP_ADDR_ENV) {
        Ok(addr) if !addr.trim().is_empty() => addr,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": {
                        "code": "meta_rpc_tcp_not_configured",
                        "message": format!("{META_RPC_TCP_ADDR_ENV} is not configured for this Fugue process")
                    }
                })),
            )
                .into_response();
        }
    };
    match forward_meta_rpc_to_addr(addr.trim(), &input).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": {
                    "code": "meta_rpc_failed",
                    "message": error
                }
            })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct MobkitRpcRequest {
    #[serde(default)]
    id: Option<JsonRpcRequestId>,
    #[serde(default)]
    jsonrpc: Option<String>,
    method: String,
    #[serde(default, deserialize_with = "deserialize_optional_raw_value")]
    params: Option<Box<RawValue>>,
}

impl MobkitRpcRequest {
    fn request_id(&self) -> JsonRpcRequestId {
        self.id
            .clone()
            .unwrap_or_else(JsonRpcRequestId::generated_meta_rpc)
    }
}

#[derive(Debug, Serialize)]
struct MobkitRpcResponse {
    jsonrpc: String,
    id: JsonRpcRequestId,
    method: String,
    response: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum JsonRpcRequestId {
    String(String),
    Number(i64),
}

impl JsonRpcRequestId {
    fn generated_meta_rpc() -> Self {
        Self::String(format!(
            "fugue-meta-rpc-{}-{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ))
    }
}

fn deserialize_optional_raw_value<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Box<RawValue>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

async fn forward_meta_rpc_to_addr(
    addr: &str,
    input: &MobkitRpcRequest,
) -> std::result::Result<MobkitRpcResponse, String> {
    forward_meta_rpc_to_addr_with_id(addr, input, input.request_id()).await
}

async fn forward_meta_rpc_to_addr_with_id(
    addr: &str,
    input: &MobkitRpcRequest,
    id: JsonRpcRequestId,
) -> std::result::Result<MobkitRpcResponse, String> {
    let id_value = serde_json::to_value(&id).map_err(|err| err.to_string())?;
    let params = input.params.as_deref().map(|raw| raw.get()).unwrap_or("{}");
    let encoded = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":{},\"params\":{}}}\n",
        serde_json::to_string(&id_value).map_err(|err| err.to_string())?,
        serde_json::to_string(&input.method).map_err(|err| err.to_string())?,
        params,
    );
    let mut stream = timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| format!("timed out connecting to Meta RPC TCP service at {addr}"))?
        .map_err(|err| format!("failed to connect to Meta RPC TCP service at {addr}: {err}"))?;
    timeout(Duration::from_secs(5), stream.write_all(encoded.as_bytes()))
        .await
        .map_err(|_| "timed out writing Meta RPC request".to_string())?
        .map_err(|err| format!("failed to write Meta RPC request: {err}"))?;
    timeout(Duration::from_secs(5), stream.flush())
        .await
        .map_err(|_| "timed out flushing Meta RPC request".to_string())?
        .map_err(|err| format!("failed to flush Meta RPC request: {err}"))?;
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        let read = timeout(Duration::from_secs(30), reader.read_line(&mut line))
            .await
            .map_err(|_| "timed out waiting for Meta RPC response".to_string())?
            .map_err(|err| format!("failed to read Meta RPC response: {err}"))?;
        if read == 0 {
            return Err(
                "Meta RPC TCP service closed before returning a matching response".to_string(),
            );
        }
        let response: serde_json::Value = serde_json::from_str(line.trim_end())
            .map_err(|err| format!("invalid Meta RPC response JSON: {err}"))?;
        if response.get("id") != Some(&id_value) {
            continue;
        }
        return Ok(MobkitRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            method: input.method.clone(),
            response,
        });
    }
}

fn authorize_meta_rpc(
    headers: &HeaderMap,
) -> std::result::Result<(), Box<axum::response::Response>> {
    let token = match std::env::var(META_RPC_BEARER_TOKEN_ENV) {
        Ok(token) if !token.trim().is_empty() => token,
        _ => {
            return Err(Box::new((
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": {
                        "code": "meta_rpc_http_auth_not_configured",
                        "message": format!("{META_RPC_BEARER_TOKEN_ENV} must be configured to use the Meta RPC bridge")
                    }
                })),
            )
                .into_response()));
        }
    };
    let authorized = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| candidate == token);
    if authorized {
        Ok(())
    } else {
        Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": {
                        "code": "meta_rpc_http_auth_failed",
                        "message": "missing or invalid Meta RPC bearer token"
                    }
                })),
            )
                .into_response(),
        ))
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Axum response is the HTTP rejection at this boundary"
)]
fn authorize_console_browser(
    config: &ServiceConfig,
    headers: &HeaderMap,
) -> std::result::Result<(), axum::response::Response> {
    let Some(auth) = config
        .server
        .as_ref()
        .and_then(|server| server.meta_console.basic_auth.as_ref())
    else {
        return Ok(());
    };
    let username = match std::env::var(&auth.username_env) {
        Ok(username) if !username.trim().is_empty() => username,
        _ => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "console_basic_auth_username_unavailable",
                        "message": "the configured console username environment variable is missing or empty"
                    }
                })),
            )
                .into_response());
        }
    };
    let password = match std::env::var(&auth.password_env) {
        Ok(password) if !password.trim().is_empty() => password,
        _ => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "console_basic_auth_password_unavailable",
                        "message": "the configured console password environment variable is missing or empty"
                    }
                })),
            )
                .into_response());
        }
    };
    let authorized = basic_auth_credentials(headers).is_some_and(|credentials| {
        constant_time_eq(&credentials.username, &username)
            && constant_time_eq(&credentials.password, &password)
    });
    if authorized {
        Ok(())
    } else {
        Err(console_basic_auth_challenge(&auth.realm))
    }
}

struct BasicAuthCredentials {
    username: String,
    password: String,
}

fn basic_auth_credentials(headers: &HeaderMap) -> Option<BasicAuthCredentials> {
    let header = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let (scheme, encoded) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some(BasicAuthCredentials {
        username: username.to_string(),
        password: password.to_string(),
    })
}

fn console_basic_auth_challenge(realm: &str) -> axum::response::Response {
    let challenge = HeaderValue::from_str(&format!(r#"Basic realm="{realm}", charset="UTF-8""#))
        .unwrap_or_else(|_| HeaderValue::from_static(r#"Basic realm="Meerkat Fugue""#));
    let mut response = (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": {
                "code": "console_basic_auth_failed",
                "message": "missing or invalid console basic auth credentials"
            }
        })),
    )
        .into_response();
    response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
    response
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut diff = left.len() ^ right.len();
    for (left, right) in left.iter().zip(right.iter()) {
        diff |= usize::from(*left ^ *right);
    }
    diff == 0
}

#[allow(
    clippy::result_large_err,
    reason = "Axum response is the HTTP rejection at this boundary"
)]
fn authorize_ota_update(
    config: &crate::config::OtaUpdateConfig,
    headers: &HeaderMap,
) -> std::result::Result<(), axum::response::Response> {
    let Some(env_name) = config
        .trigger_token_env
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": {
                    "code": "ota_update_http_auth_not_configured",
                    "message": "ota_update.trigger_token_env must be configured to use the HTTP OTA trigger"
                }
            })),
        )
            .into_response());
    };
    let token = match std::env::var(env_name) {
        Ok(token) if !token.is_empty() => token,
        _ => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "ota_update_http_auth_token_unavailable",
                        "message": "the configured OTA trigger token environment variable is missing or empty"
                    }
                })),
            )
                .into_response());
        }
    };
    let authorized = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| candidate == token);
    if authorized {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "code": "ota_update_http_auth_failed",
                    "message": "missing or invalid OTA update bearer token"
                }
            })),
        )
            .into_response())
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkflowDefinition;
    use crate::fugue_ims::CreateFugueIssue;
    use crate::ims::{ImsIssueKey, ImsStageName};
    use crate::tools::{
        BranchName, GitHubOwner, GitHubPullRequestNumber, GitHubPullRequestSnapshot,
        GitHubRepoName, GitHubRepoRef, PullRequestState,
    };
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    async fn managed_test_router(
        root: &std::path::Path,
        extra: Option<Router>,
    ) -> (Router, crate::orchestrator::Orchestrator) {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "fugue-ims", "endpoint": root.join("ims.sqlite"), "project_slug": "test/repository"},
                "meerkat": {"repository_runtime": {
                    "repository": "test-repository", "state_path": root.join("team")
                }},
                "server": {"meta_console": {"enabled": false}}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "test".into(),
        };
        let manager = crate::config::WorkflowConfigManager::from_definition_for_test(
            root.join("fugue.yaml"),
            definition,
        )
        .unwrap();
        let (refresh, receiver) = mpsc::channel(8);
        let owner = crate::orchestrator::Orchestrator::new(manager, receiver);
        let app = router(
            owner.shared_state(),
            owner.config().effective().clone(),
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            extra,
        )
        .await
        .unwrap();
        (app, owner)
    }

    #[tokio::test]
    async fn jira_create_http_preserves_acknowledgement_when_initial_status_fails() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        use tower::ServiceExt;

        for partial in [false, true] {
            let mut responses = vec![reply(
                "POST",
                "/rest/api/3/issue",
                json!({"id":"10001","key":"TEST-1"}),
            )];
            if partial {
                responses.push(reply(
                    "GET",
                    "/rest/api/3/issue/10001",
                    issue("In Progress"),
                ));
                responses.push(reply(
                    "GET",
                    "/rest/api/3/issue/TEST-1/transitions",
                    json!({"transitions":[]}),
                ));
            } else {
                responses.push(reply("GET", "/rest/api/3/issue/10001", issue("To Do")));
                responses.push(reply("GET", "/rest/api/3/issue/10001", issue("To Do")));
            }
            let mock = mock_jira(responses).await;
            let root = tempfile::tempdir().unwrap();
            let (_, owner) = managed_test_router(root.path(), None).await;
            let mut config = owner.config().effective().clone();
            config.tracker = tracker(&mock.endpoint);
            config.linear.lanes.parked = "To Do".to_owned();
            config.linear.lanes.ready = "To Do".to_owned();
            let (refresh, _receiver) = mpsc::channel(1);
            let app = router(
                owner.shared_state(),
                config.clone(),
                refresh,
                owner.internal_sender(),
                owner.meta_runtime_registry(),
                None,
            )
            .await
            .unwrap();
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/issues")
                        .header("Host", "127.0.0.1:8788")
                        .header("Origin", "http://127.0.0.1:8788")
                        .header(CONTENT_TYPE, "application/json")
                        .header("x-fugue-tracker-scope", config.tracker_context_id())
                        .body(Body::from(r#"{"title":"One issue","lane":"To Do"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if partial {
                assert_eq!(payload["outcome"], "created_with_warning");
                assert_eq!(
                    payload["created_issue"],
                    json!({
                        "id":"10001", "identifier":"TEST-1", "url":format!("{}/browse/TEST-1", mock.endpoint),
                    })
                );
                assert_eq!(payload["warning"]["code"], "issue_created_followup_failed");
                assert!(
                    payload["warning"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("TEST-1")
                );
                assert!(
                    payload.get("issue").is_none(),
                    "do not invent confirmed issue state"
                );
            } else {
                assert_eq!(payload["outcome"], "created");
                assert_eq!(payload["issue"]["identifier"], "TEST-1");
            }
            assert!(mock.responses.lock().unwrap().is_empty());
            assert_eq!(
                mock.requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(uri, _)| uri == "/rest/api/3/issue")
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn tracker_boundary_attachment_download_requires_issue_membership() {
        use tower::ServiceExt;
        let root = tempfile::tempdir().unwrap();
        let (own, own_config, _owner) =
            attachment_scope_test_router(root.path(), "owner/one").await;
        let (foreign, foreign_config, _other) =
            attachment_scope_test_router(root.path(), "owner/two").await;
        crate::fugue_ims::create_issue_for_config(
            &own_config.tracker,
            CreateFugueIssue {
                key: ImsIssueKey("TEST-1".into()),
                repo: "owner/one".into(),
                title: "Private issue".into(),
                description: None,
                stage: ImsStageName("Todo".into()),
                labels: vec![],
            },
        )
        .unwrap();
        // Even a file copied into the wrong scoped directory must not be read
        // when the tracker says the issue belongs to another repository.
        assert_ne!(
            own_config.attachment_storage_root(),
            foreign_config.attachment_storage_root()
        );
        for config in [&own_config, &foreign_config] {
            let files = issue_attachment_dir(config, "TEST-1");
            std::fs::create_dir_all(&files).unwrap();
            std::fs::write(files.join("trace.txt"), "private file contents").unwrap();
        }
        for (app, allowed) in [(foreign, false), (own, true)] {
            let response = app
                .oneshot(attachment_test_request(
                    None,
                    "GET",
                    "/api/v1/issues/TEST-1/attachments/trace.txt",
                    Body::empty(),
                    None,
                ))
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            if allowed {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(bytes.as_ref(), b"private file contents");
            } else {
                assert!(!status.is_success(), "foreign project read the file");
                assert!(!String::from_utf8_lossy(&bytes).contains("private file contents"));
            }
        }
    }

    #[tokio::test]
    async fn tracker_boundary_attachment_upload_denies_before_filesystem_writes() {
        use tower::ServiceExt;
        for endpoint in ["attachments", "comments"] {
            let root = tempfile::tempdir().unwrap();
            let (_, own_config, _owner) =
                attachment_scope_test_router(root.path(), "owner/one").await;
            let (foreign, foreign_config, _other) =
                attachment_scope_test_router(root.path(), "owner/two").await;
            crate::fugue_ims::create_issue_for_config(
                &own_config.tracker,
                CreateFugueIssue {
                    key: ImsIssueKey("TEST-1".into()),
                    repo: "owner/one".into(),
                    title: "Private issue".into(),
                    description: None,
                    stage: ImsStageName("Todo".into()),
                    labels: vec![],
                },
            )
            .unwrap();
            let directory = issue_attachment_dir(&foreign_config, "TEST-1");
            assert!(!directory.exists());
            let response = foreign
                .oneshot(attachment_test_request(
                    Some(&foreign_config),
                    "POST",
                    &format!("/api/v1/issues/TEST-1/{endpoint}"),
                    attachment_test_body(),
                    Some("multipart/form-data; boundary=fixture"),
                ))
                .await
                .unwrap();
            assert!(!response.status().is_success());
            assert!(
                !directory.exists(),
                "{endpoint} wrote to a foreign issue before refusing it"
            );
        }
    }

    async fn attachment_scope_test_router(
        root: &Path,
        project: &str,
    ) -> (Router, ServiceConfig, crate::orchestrator::Orchestrator) {
        let (_, owner) = managed_test_router(root, None).await;
        let mut config = owner.config().effective().clone();
        config.tracker.project_slug = Some(project.to_owned());
        let (refresh, _receiver) = mpsc::channel(1);
        let app = router(
            owner.shared_state(),
            config.clone(),
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            None,
        )
        .await
        .unwrap();
        (app, config, owner)
    }

    #[tokio::test]
    async fn tracker_boundary_owned_attachment_uploads_and_downloads_still_work() {
        use tower::ServiceExt;
        for endpoint in ["attachments", "comments"] {
            let root = tempfile::tempdir().unwrap();
            let (app, config, _owner) =
                attachment_scope_test_router(root.path(), "owner/one").await;
            crate::fugue_ims::create_issue_for_config(
                &config.tracker,
                CreateFugueIssue {
                    key: ImsIssueKey("TEST-1".into()),
                    repo: "owner/one".into(),
                    title: "Owned issue".into(),
                    description: None,
                    stage: ImsStageName("Todo".into()),
                    labels: vec![],
                },
            )
            .unwrap();
            let response = app
                .clone()
                .oneshot(attachment_test_request(
                    Some(&config),
                    "POST",
                    &format!("/api/v1/issues/TEST-1/{endpoint}"),
                    attachment_test_body(),
                    Some("multipart/form-data; boundary=fixture"),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let url = payload["attachments"][0]["url"].as_str().unwrap();
            let response = app
                .oneshot(attachment_test_request(
                    None,
                    "GET",
                    url,
                    Body::empty(),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), 8192).await.unwrap().as_ref(),
                b"attachment contents"
            );
            assert_eq!(
                IssueAdapter::from_config(&config.tracker)
                    .unwrap()
                    .issue_comments(&issue_ref_for_identifier("TEST-1"))
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn tracker_boundary_malformed_multipart_never_posts_partial_comment() {
        use tower::ServiceExt;
        for endpoint in ["attachments", "comments"] {
            let root = tempfile::tempdir().unwrap();
            let (app, config, _owner) =
                attachment_scope_test_router(root.path(), "owner/one").await;
            crate::fugue_ims::create_issue_for_config(
                &config.tracker,
                CreateFugueIssue {
                    key: ImsIssueKey("TEST-1".into()),
                    repo: "owner/one".into(),
                    title: "Owned issue".into(),
                    description: None,
                    stage: ImsStageName("Todo".into()),
                    labels: vec![],
                },
            )
            .unwrap();
            let response = app
                .oneshot(attachment_test_request(
                    Some(&config),
                    "POST",
                    &format!("/api/v1/issues/TEST-1/{endpoint}"),
                    Body::from("--fixture\r\nContent-Disposition: form-data; name=\"attachments\"; filename=\"trace.txt\"\r\nContent-Type: text/plain\r\n\r\nvalid first file\r\n--fixture\r\nInvalid header without a colon\r\n\r\npartial second file\r\n--fixture--\r\n"),
                    Some("multipart/form-data; boundary=fixture"),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{endpoint}");
            assert!(
                IssueAdapter::from_config(&config.tracker)
                    .unwrap()
                    .issue_comments(&issue_ref_for_identifier("TEST-1"))
                    .await
                    .unwrap()
                    .is_empty(),
                "{endpoint} posted a comment from an incomplete request"
            );
        }
    }

    #[tokio::test]
    async fn tracker_boundary_jira_browser_uploads_publish_native_files() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        use tower::ServiceExt;
        for endpoint in ["attachments", "comments"] {
            let contents = if endpoint == "attachments" {
                "x".repeat(2 * 1024 * 1024 + 1)
            } else {
                "attachment contents".to_string()
            };
            let mock = mock_jira(vec![]).await;
            let content_url = format!("{}/secure/attachment/7/trace.txt", mock.endpoint);
            let mut replies = vec![
                reply("GET", "/rest/api/3/issue/10001", issue("To Do")),
                reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")),
                reply(
                    "POST",
                    "/rest/api/3/issue/TEST-1/attachments",
                    json!([
                        {"filename":"trace.txt","content":content_url,"size":contents.len(),"mimeType":"text/plain"}
                    ]),
                ),
            ];
            if endpoint == "comments" {
                replies.push(reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")));
                replies.push(reply(
                    "POST",
                    "/rest/api/3/issue/TEST-1/comment",
                    json!({"id":"comment-7","body":"Attached"}),
                ));
            }
            mock.responses.lock().unwrap().extend(replies);
            let root = tempfile::tempdir().unwrap();
            let (_, mut config, owner) = attachment_scope_test_router(root.path(), "TEST").await;
            config.tracker = tracker(&mock.endpoint);
            let (refresh, _receiver) = mpsc::channel(1);
            let app = router(
                owner.shared_state(),
                config.clone(),
                refresh,
                owner.internal_sender(),
                owner.meta_runtime_registry(),
                None,
            )
            .await
            .unwrap();
            let response = app
                .oneshot(attachment_test_request(
                    Some(&config),
                    "POST",
                    &format!("/api/v1/issues/10001/{endpoint}"),
                    Body::from(format!("--fixture\r\nContent-Disposition: form-data; name=\"attachments\"; filename=\"trace.txt\"\r\nContent-Type: text/plain\r\n\r\n{contents}\r\n--fixture--\r\n")),
                    Some("multipart/form-data; boundary=fixture"),
                ))
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );
            let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(payload["attachments"][0]["url"], content_url);
            assert!(mock.responses.lock().unwrap().is_empty());
            let requests = mock.requests.lock().unwrap();
            let files = &requests[2].1["files"];
            assert_eq!(files[0]["name"], "trace.txt");
            assert_eq!(files[0]["bytes"], json!(contents.as_bytes().to_vec()));
            if endpoint == "comments" {
                assert!(requests[4].1.to_string().contains(&content_url));
                assert!(!requests[4].1.to_string().contains("/api/v1/issues"));
                assert!(
                    !requests[4]
                        .1
                        .to_string()
                        .contains(root.path().to_str().unwrap())
                );
            }
        }
    }

    #[tokio::test]
    async fn tracker_boundary_jira_upload_failures_retain_acknowledged_files() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        use tower::ServiceExt;
        for scenario in [
            "server",
            "malformed",
            "incomplete",
            "partial",
            "comment",
            "foreign_url",
            "rejected",
        ] {
            let mock = mock_jira(vec![]).await;
            let url = format!("{}/secure/attachment/7/trace.txt", mock.endpoint);
            let response_body = match scenario {
                "malformed" => json!({"private":"private-response-sentinel"}),
                "incomplete" => json!([]),
                "foreign_url" => {
                    json!([{"id":"7","filename":"trace.txt","content":"https://foreign.example.test/private","size":19}])
                }
                _ => json!([{"id":"7","filename":"trace.txt","content":url,"size":19}]),
            };
            let status = match scenario {
                "server" => StatusCode::SERVICE_UNAVAILABLE,
                "rejected" => StatusCode::FORBIDDEN,
                _ => StatusCode::OK,
            };
            let mut replies = vec![
                reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")),
                reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")),
                reply(
                    "POST",
                    "/rest/api/3/issue/TEST-1/attachments",
                    response_body,
                )
                .with_status(status),
            ];
            if scenario == "comment" {
                replies.push(reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")));
                replies.push(
                    reply(
                        "POST",
                        "/rest/api/3/issue/TEST-1/comment",
                        json!({"private":"private-response-sentinel"}),
                    )
                    .with_status(StatusCode::SERVICE_UNAVAILABLE),
                );
            }
            mock.responses.lock().unwrap().extend(replies);
            let root = tempfile::tempdir().unwrap();
            let (_, mut config, owner) = attachment_scope_test_router(root.path(), "TEST").await;
            config.tracker = tracker(&mock.endpoint);
            let (refresh, _receiver) = mpsc::channel(1);
            let app = router(
                owner.shared_state(),
                config.clone(),
                refresh,
                owner.internal_sender(),
                owner.meta_runtime_registry(),
                None,
            )
            .await
            .unwrap();
            let response = app
                .oneshot(attachment_test_request(
                    Some(&config),
                    "POST",
                    "/api/v1/issues/TEST-1/comments",
                    if scenario == "partial" {
                        Body::from("--fixture\r\nContent-Disposition: form-data; name=\"attachments\"; filename=\"trace.txt\"\r\n\r\nattachment contents\r\n--fixture\r\nContent-Disposition: form-data; name=\"attachments\"; filename=\"second.txt\"\r\n\r\nsecond\r\n--fixture--\r\n")
                    } else { attachment_test_body() },
                    Some("multipart/form-data; boundary=fixture"),
                ))
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("private-response-sentinel"));
            let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if scenario == "rejected" {
                assert_eq!(status, StatusCode::FORBIDDEN);
                assert_eq!(payload["error"]["code"], "jira_request_rejected");
            } else {
                assert_eq!(status, StatusCode::OK, "{scenario}: {payload}");
                assert_eq!(
                    payload["warning"]["code"],
                    if scenario == "comment" {
                        "comment_not_confirmed"
                    } else {
                        "upload_not_confirmed"
                    }
                );
                assert_eq!(
                    payload["attachments"].as_array().unwrap().len(),
                    usize::from(matches!(scenario, "comment" | "partial"))
                );
                assert!(payload["comment_id"].is_null());
                if scenario == "comment" {
                    assert_eq!(payload["attachments"][0]["url"], url);
                }
            }
            assert!(mock.responses.lock().unwrap().is_empty(), "{scenario}");
        }
    }

    #[tokio::test]
    async fn tracker_boundary_stale_browser_context_denied_before_provider_or_storage() {
        use crate::jira::tests::{mock_jira, tracker};
        use tower::ServiceExt;
        let mock = mock_jira(vec![]).await;
        let root = tempfile::tempdir().unwrap();
        let (_, mut config, owner) = attachment_scope_test_router(root.path(), "TEST").await;
        let old_context = config.tracker_context_id();
        config.tracker = tracker(&mock.endpoint);
        let storage = issue_attachment_dir(&config, "TEST-1");
        let (refresh, _receiver) = mpsc::channel(1);
        let app = router(
            owner.shared_state(),
            config,
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            None,
        )
        .await
        .unwrap();
        for missing in [false, true] {
            for endpoint in ["attachments", "comments"] {
                let mut request = attachment_test_request(
                    None,
                    "POST",
                    &format!("/api/v1/issues/TEST-1/{endpoint}"),
                    attachment_test_body(),
                    Some("multipart/form-data; boundary=fixture"),
                );
                if !missing {
                    request.headers_mut().insert(
                        "x-fugue-tracker-scope",
                        HeaderValue::from_str(&old_context).unwrap(),
                    );
                }
                let response = app.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::CONFLICT);
            }
        }
        assert!(mock.requests.lock().unwrap().is_empty());
        assert!(!storage.exists());
    }

    #[tokio::test]
    async fn tracker_boundary_jira_json_cannot_upload_host_paths() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        use tower::ServiceExt;
        let mock = mock_jira(vec![reply(
            "GET",
            "/rest/api/3/issue/TEST-1",
            issue("To Do"),
        )])
        .await;
        let root = tempfile::tempdir().unwrap();
        let secret = root.path().join("private.txt");
        std::fs::write(&secret, "private-file-sentinel").unwrap();
        let (_, mut config, owner) = attachment_scope_test_router(root.path(), "TEST").await;
        config.tracker = tracker(&mock.endpoint);
        let (refresh, _receiver) = mpsc::channel(1);
        let app = router(
            owner.shared_state(),
            config.clone(),
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            None,
        )
        .await
        .unwrap();
        let body = json!({"body_markdown":"Example", "attachments":[{"title":"private", "url":"https://example.test/file", "path":secret,"byte_len":21}]}).to_string();
        let response = app
            .oneshot(attachment_test_request(
                Some(&config),
                "POST",
                "/api/v1/issues/TEST-1/comments",
                Body::from(body),
                Some("application/json"),
            ))
            .await
            .unwrap();
        assert!(!response.status().is_success());
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tracker_boundary_jira_download_resolves_key_without_comments_or_remote_files() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        use tower::ServiceExt;
        let mock = mock_jira(vec![reply(
            "GET",
            "/rest/api/3/issue/10001",
            issue("To Do"),
        )])
        .await;
        let root = tempfile::tempdir().unwrap();
        let (_, mut config, owner) = attachment_scope_test_router(root.path(), "TEST").await;
        config.tracker = tracker(&mock.endpoint);
        let files = issue_attachment_dir(&config, "TEST-1");
        std::fs::create_dir_all(&files).unwrap();
        std::fs::write(files.join("trace.txt"), "owned Jira attachment").unwrap();
        let (refresh, _receiver) = mpsc::channel(1);
        let app = router(
            owner.shared_state(),
            config,
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            None,
        )
        .await
        .unwrap();
        let response = app
            .oneshot(attachment_test_request(
                None,
                "GET",
                "/api/v1/issues/10001/attachments/trace.txt",
                Body::empty(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 8192).await.unwrap().as_ref(),
            b"owned Jira attachment"
        );
        assert!(mock.responses.lock().unwrap().is_empty());
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn tracker_boundary_jira_environment_references_scope_actual_site_and_project() {
        const MARKER: &str = "FUGUE_TEST_TRACKER_SCOPE_ENV";
        if std::env::var_os(MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "http::tests::tracker_boundary_jira_environment_references_scope_actual_site_and_project",
                    "--exact", "--nocapture",
                ])
                .env(MARKER, "1")
                .env("FUGUE.TEST.TRACKER.SITE", "https://jira.example.test")
                .env("FUGUE.TEST.TRACKER.PROJECT", "TEST")
                .output().unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        let mut referenced = issue_board_config();
        referenced.tracker.kind = "jira".into();
        referenced.tracker.endpoint = "$FUGUE.TEST.TRACKER.SITE".into();
        referenced.tracker.project_slug = Some("$FUGUE.TEST.TRACKER.PROJECT".into());
        let mut literal = referenced.clone();
        literal.tracker.endpoint = "https://jira.example.test".into();
        literal.tracker.project_slug = Some("TEST".into());
        assert_eq!(
            referenced.tracker_context_id(),
            literal.tracker_context_id()
        );
        assert_eq!(
            referenced.attachment_storage_root(),
            literal.attachment_storage_root()
        );
        literal.tracker.endpoint = "https://other.example.test".into();
        assert_ne!(
            referenced.tracker_context_id(),
            literal.tracker_context_id()
        );
    }

    #[test]
    fn tracker_boundary_attachment_roots_are_scope_specific_and_credential_independent() {
        for managed in [false, true] {
            let mut first = issue_board_config();
            if managed {
                first.meerkat.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
                    repository: crate::ensemble::config::RepoId::try_from("first".to_owned())
                        .unwrap(),
                    state_path: PathBuf::from("/tmp/test-repository-state"),
                });
            }
            let root = first.attachment_storage_root();
            first.tracker.api_key = Some("changed-credential".into());
            assert_eq!(first.attachment_storage_root(), root);
            for component in ["provider", "endpoint", "project"] {
                let mut other = first.clone();
                match component {
                    "provider" => other.tracker.kind = "jira".into(),
                    "endpoint" => {
                        other.tracker.endpoint = "https://other.example.test/graphql".into()
                    }
                    _ => other.tracker.project_slug = Some("other-project".into()),
                }
                assert_ne!(root, other.attachment_storage_root(), "{component}");
            }
            first.tracker.project_slug =
                first.tracker.project_slug.map(|scope| format!(" {scope} "));
            assert_eq!(first.attachment_storage_root(), root);
        }
    }

    fn attachment_test_request(
        config: Option<&ServiceConfig>,
        method: &str,
        path: &str,
        body: Body,
        content_type: Option<&str>,
    ) -> Request {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("Host", "127.0.0.1:8788")
            .header("Origin", "http://127.0.0.1:8788");
        if let Some(config) = config {
            request = request.header("x-fugue-tracker-scope", config.tracker_context_id());
        }
        if let Some(value) = content_type {
            request = request.header(CONTENT_TYPE, value);
        }
        request.body(body).unwrap()
    }

    fn attachment_test_body() -> Body {
        Body::from(
            "--fixture\r\nContent-Disposition: form-data; name=\"attachments\"; filename=\"trace.txt\"\r\nContent-Type: text/plain\r\n\r\nattachment contents\r\n--fixture--\r\n",
        )
    }

    #[tokio::test]
    async fn managed_queue_guards_direct_child_reads_mutations_and_fallbacks() {
        use tower::ServiceExt;
        let root = tempfile::tempdir().unwrap();
        let (app, _owner) = managed_test_router(
            root.path(),
            Some(Router::new().fallback(|| async { "console" })),
        )
        .await;
        for (method, path) in [
            ("GET", "/api/v1/state"),
            ("POST", "/api/v1/refresh"),
            ("GET", "/console"),
        ] {
            for (host, origin, expected) in [
                (
                    "untrusted.example",
                    "https://untrusted.example",
                    StatusCode::FORBIDDEN,
                ),
                (
                    "127.0.0.1:8788",
                    "https://untrusted.example",
                    StatusCode::FORBIDDEN,
                ),
                ("127.0.0.1:8788", "http://127.0.0.1:8788", StatusCode::OK),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method(method)
                            .uri(path)
                            .header("Host", host)
                            .header("Origin", origin)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                if expected == StatusCode::OK {
                    assert!(
                        response.status().is_success(),
                        "{method} {path}: {}",
                        response.status()
                    );
                } else {
                    assert_eq!(response.status(), expected, "{method} {path}");
                }
            }
        }
    }

    #[tokio::test]
    async fn queue_with_repository_directory_has_one_owner_per_page() {
        use tower::ServiceExt;
        let root = tempfile::tempdir().unwrap();
        let directory =
            crate::setup::directory::Directory::open(root.path().join("directory"), 8788).unwrap();
        let (app, _owner) = managed_test_router(
            root.path(),
            Some(crate::setup::directory::api_router(directory)),
        )
        .await;
        for path in ["/overview", "/queues", "/api/workspaces"] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::get(path)
                        .header("Host", "127.0.0.1:8788")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response.status().is_success() || response.status().is_redirection(),
                "{path}"
            );
        }
    }

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const CONSOLE_TEST_USER_ENV: &str = "FUGUE_TEST_CONSOLE_USER";
    const CONSOLE_TEST_PASSWORD_ENV: &str = "FUGUE_TEST_CONSOLE_PASSWORD";

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let previous = std::env::var(name).ok();
            unsafe { std::env::set_var(name, value) };
            Self {
                name,
                previous,
                _lock: lock,
            }
        }

        fn unset(name: &'static str) -> Self {
            let lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let previous = std::env::var(name).ok();
            unsafe { std::env::remove_var(name) };
            Self {
                name,
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                unsafe { std::env::set_var(self.name, value) };
            } else {
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    struct EnvVarsGuard {
        previous: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvVarsGuard {
        fn set(values: &[(&'static str, &'static str)]) -> Self {
            let lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let previous = values
                .iter()
                .map(|(name, value)| {
                    let previous = std::env::var(name).ok();
                    unsafe { std::env::set_var(name, value) };
                    (*name, previous)
                })
                .collect();
            Self {
                previous,
                _lock: lock,
            }
        }

        fn unset(names: &[&'static str]) -> Self {
            let lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let previous = names
                .iter()
                .map(|name| {
                    let previous = std::env::var(name).ok();
                    unsafe { std::env::remove_var(name) };
                    (*name, previous)
                })
                .collect();
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvVarsGuard {
        fn drop(&mut self) {
            for (name, value) in self.previous.iter().rev() {
                if let Some(value) = value {
                    unsafe { std::env::set_var(name, value) };
                } else {
                    unsafe { std::env::remove_var(name) };
                }
            }
        }
    }

    fn assert_document_contract(html: &str) {
        assert!(html.contains("<!doctype html>"));
        assert!(html.contains("name=\"viewport\""));
        assert!(html.contains("charset=\"utf-8\""));
    }

    #[test]
    fn home_and_work_share_navigation_and_real_data_sources() {
        for html in [
            render_home_html(true),
            render_home_html(false),
            render_issues_board_html(&issue_board_config()),
        ] {
            for destination in ["/overview", "/queues", "/console"] {
                assert!(html.contains(&format!("href=\"{destination}\"")));
            }
            assert!(html.contains("Primary app navigation"));
            assert!(html.contains("Repository scope"));
        }
        let html = render_home_html(true);
        assert!(html.contains("/api/ensemble"));
        assert!(html.contains("/api/v1/issues/board"));
        assert!(html.contains("/api/workspaces"));
        assert!(html.contains("/ensemble#add"));
        assert!(!html.contains("__PORTFOLIO_JS__"));
    }

    #[test]
    fn issue_routes_keep_dashboard_navigation_targets() {
        let html = render_issues_board_html(&issue_board_config());

        assert_document_contract(&html);
        assert!(html.contains(r#"href="./overview""#));
        assert!(html.contains(r#"href="./console""#));
        assert!(html.contains(r#"href="./ensemble""#));
        assert!(html.contains(r#"href="./issues""#));
        assert!(html.contains("banana-sorbet ims-subsystem"));
        assert!(html.contains("MEERKAT FUGUE"));
        assert!(html.contains("to search…"));
        assert!(html.contains("--font-display"));
        assert!(html.contains("--bg-elev-1"));
        assert!(html.contains("--line-strong"));
        assert!(html.contains("--accent"));
        assert!(html.contains("button.primary { color: var(--bg); background: var(--accent);"));
        assert!(html.contains(".lane {"));
        assert!(html.contains("background: var(--bg-elev-1);"));
        assert!(html.contains(
            ".card:hover { background: var(--bg-elev-2); border-color: var(--line-strong);"
        ));
        assert!(
            html.contains(
                "textarea:focus, input:focus, select:focus { border-color: var(--accent);"
            )
        );
        assert!(html.contains(".hidden-panel {"));
        assert!(html.contains("background: var(--bg-sunken);"));
        assert!(
            html.contains(
                ".palette-row:hover, .palette-row.active { background: var(--bg-elev-2);"
            )
        );
        assert!(html.contains("+ New issue"));
        assert!(html.contains("openFullDetail(card.dataset.issue)"));
        assert!(!html.contains("loadDetail(card.dataset.issue);"));
        assert!(html.contains(r#"id="kpiTok">unavailable</b>"#));
        assert!(html.contains(r#"id="kpiTime">unavailable</b>"#));
        assert!(html.contains(r#"id="kpiShad">unavailable</b>"#));
        assert!(html.contains("app.board.totals.tokens ?? \"unavailable\""));
        assert!(!html.contains(concat!("app.board.totals.tokens ", "|| 0")));
        assert!(!html.contains("turns unavailable"));
        assert!(!html.contains(concat!(": \\\"", "0", "t\\\"")));
        for fabricated in [
            concat!("TOK <b id=\"kpiTok\">", "0"),
            concat!("T <b id=\"kpiTime\">", "0.0s"),
            concat!("SHAD <b id=\"kpiShad\">", "0r"),
        ] {
            assert!(
                !html.contains(fabricated),
                "IMS rail leaked fabricated static KPI: {fabricated}"
            );
        }
        assert!(html.contains(r#"id="createTitle""#));
        assert!(html.contains(r#"<textarea id="createDescription""#));
        assert!(html.contains("issue-editor-form"));
        assert!(html.contains("issue-editor-shell"));
        assert!(html.contains("issue-markdown-editor"));
        assert!(html.contains(r#"id="createAttachments""#));
        assert!(html.contains("createAttachmentPayload"));
        assert!(html.contains("attachments: createAttachmentPayload()"));
        assert!(
            html.contains("Add screenshots, logs, or other files that help explain the issue.")
        );
        assert!(!html.contains("createDescriptionWithAttachments"));
        assert!(!html.contains(concat!("File", "Reader")));
        assert!(!html.contains(concat!("readAs", "DataURL")));
        assert!(!html.contains(concat!("data", "Url")));
        assert!(html.contains("maxCreateAttachmentBytes"));
        assert!(html.contains("maxCreateAttachmentCount"));
        assert!(html.contains("25 MiB per-file attachment limit"));
        assert!(html.contains(r#"id="createLane""#));
        assert!(html.contains(r#"<option value="Backlog">Backlog</option>"#));
        assert!(html.contains(r#"<option value="Queue">Queue</option>"#));
    }

    #[test]
    fn unconfigured_ensemble_explains_same_app_configuration() {
        let html = inject_app_chrome(ENSEMBLE_LANDING_HTML, AppPage::Ensemble, true).unwrap();
        assert!(html.contains("--ensemble-config"));
        assert!(html.contains("No Ensemble is configured"));
        assert!(!html.contains("http://127.0.0.1"));
        assert!(html.contains(r#"href="/ensemble" class="active" aria-current="page""#));
    }

    #[test]
    fn issue_create_ui_renders_attachment_drag_drop_contract() {
        let html = render_issues_board_html(&issue_board_config());
        let source = std::fs::read_to_string("src/http.rs").unwrap();

        for required in [
            r#"id="createAttachmentDropZone""#,
            "data-attachment-drop-zone",
            "Drop files here to attach them",
            "attachment-drop-zone.drag-over",
            "aria-live=\"polite\"",
            "installAttachmentDropTarget",
            "dataTransferHasFiles",
            "dragenter",
            "dragover",
            "drop",
            "event.dataTransfer.files",
            "event.preventDefault()",
            "createAttachmentState",
            "validateCreateAttachment",
            "createAttachmentPayload",
            "attachments: createAttachmentPayload()",
            "At most 20 attachments can be added.",
            "25 MiB per-file attachment limit",
            "100 MiB total attachment limit",
        ] {
            assert!(
                html.contains(required),
                "missing create drag/drop contract: {required}"
            );
        }

        assert!(
            html.contains("document.addEventListener(\"drop\", (event) =>")
                && html.contains("if (dataTransferHasFiles(event)) event.preventDefault();"),
            "file drops must be captured to prevent browser navigation"
        );
        assert!(html.contains("draggable=\"true\""));
        assert!(html.contains("app.dragging"));
        for banned in [
            concat!("File", "Reader"),
            concat!("readAs", "DataURL"),
            concat!("data", "Url"),
        ] {
            assert!(
                !source.contains(banned),
                "source must not embed file content using {banned}"
            );
        }
    }

    #[test]
    fn issue_detail_editor_renders_attachment_drag_drop_contract() {
        let board_html = render_issues_board_html(&issue_board_config());

        for required in [
            r#"id="issueAttachmentDropZone""#,
            r#"id="issueAttachmentInput""#,
            r#"id="issueAttachmentList""#,
            r#"id="issueAttachmentError""#,
            "data-attachment-drop-zone",
            "Drop files here to attach them to this issue",
            "attachment-drop-zone.drag-over",
            "aria-live=\"polite\"",
            "installAttachmentDropTarget",
            "dataTransferHasFiles",
            "event.dataTransfer.files",
            "uploadIssueAttachments",
            "FormData",
            "/attachments",
            "Attached ${payload.attachments?.length || added.length} file(s).",
            "No attachments added for this issue editor.",
            "25 MiB per-file attachment limit",
            "100 MiB total attachment limit",
            "At most 20 attachments can be added.",
        ] {
            assert!(
                ISSUE_DETAIL_HTML.contains(required),
                "missing detail drag/drop contract: {required}"
            );
        }

        for required in [
            r#"id="detailAttachmentDropZone""#,
            r#"id="detailAttachmentInput""#,
            r#"id="detailAttachmentList""#,
            r#"id="detailAttachmentError""#,
            "uploadIssueAttachments",
            "/attachments",
        ] {
            assert!(
                board_html.contains(required),
                "missing drawer drag/drop contract: {required}"
            );
        }
    }

    #[test]
    fn issue_detail_page_keeps_integrated_navigation_and_ims_controls() {
        assert_document_contract(ISSUE_DETAIL_HTML);
        assert!(ISSUE_DETAIL_HTML.contains("Meerkat Fugue · Issue sheet"));
        assert!(ISSUE_DETAIL_HTML.contains("banana-sorbet issue-subsystem"));
        assert!(ISSUE_DETAIL_HTML.contains("--font-display"));
        assert!(ISSUE_DETAIL_HTML.contains("--paper"));
        assert!(ISSUE_DETAIL_HTML.contains("--hair"));
        assert!(ISSUE_DETAIL_HTML.contains("--accent"));
        assert!(ISSUE_DETAIL_HTML.contains(".save { width:240px; background:var(--accent);"));
        assert!(ISSUE_DETAIL_HTML.contains("textarea { width:100%;"));
        assert!(ISSUE_DETAIL_HTML.contains(".sheet {"));
        assert!(ISSUE_DETAIL_HTML.contains("background:var(--bg)"));
        assert!(ISSUE_DETAIL_HTML.contains(r#"href="/overview""#));
        assert!(ISSUE_DETAIL_HTML.contains(r#"href="/issues""#));
        assert!(ISSUE_DETAIL_HTML.contains(r#"href="/console""#));
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="workpadEditor""#));
        assert!(ISSUE_DETAIL_HTML.contains("issue-markdown-editor"));
        assert!(ISSUE_DETAIL_HTML.contains("ACTIVITY"));
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="lanePicker""#));
        assert!(ISSUE_DETAIL_HTML.contains("SAVE CHANGES"));
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="kpiRun""#));
        assert!(ISSUE_DETAIL_HTML.contains("const renderKpis = ()"));
        assert!(ISSUE_DETAIL_HTML.contains("detail.runtime"));
        assert!(ISSUE_DETAIL_HTML.contains("token_total"));
        assert!(ISSUE_DETAIL_HTML.contains("unavailable"));
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="commentEditor""#));
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="postComment""#));
        assert!(
            ISSUE_DETAIL_HTML
                .contains("/api/v1/issues/${encodeURIComponent(issueIdentifier)}/comments")
        );
        assert!(ISSUE_DETAIL_HTML.contains(r#"id="commentAttachmentInput""#));
        assert!(ISSUE_DETAIL_HTML.contains("commentAttachmentState"));
        assert!(ISSUE_DETAIL_HTML.contains("FormData"));
        assert!(ISSUE_DETAIL_HTML.contains("await load(true)"));
        assert!(ISSUE_DETAIL_HTML.contains("Comment or attachment is required."));
        assert!(ISSUE_DETAIL_HTML.contains(r#"$("commentEditor")?.focus()"#));
        for fabricated in [
            concat!("RUN&nbsp;<b>", "0"),
            concat!("RETRY&nbsp;<b>", "0"),
            concat!("TOK&nbsp;<b>", "0"),
            concat!("T&nbsp;<b>", "0.0s"),
            concat!("SHAD&nbsp;<b>", "0r"),
        ] {
            assert!(
                !ISSUE_DETAIL_HTML.contains(fabricated),
                "issue detail leaked fabricated static KPI: {fabricated}"
            );
        }
    }

    fn non_default_lane_config() -> ServiceConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "linear",
                    "api_key": "token",
                    "project_slug": "project",
                    "intake_states": ["Ready Queue"],
                    "in_flight_states": ["Planning", "Implementing"],
                    "active_states": ["Ready Queue", "Reviewing"],
                    "terminal_states": ["Done", "Canceled"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "required_states": [],
                    "lanes": {
                        "parked": "Icebox",
                        "ready": "Ready Queue",
                        "success": "Done",
                        "canceled": "Canceled",
                        "duplicate": "Duplicate"
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(std::path::PathBuf::from("/tmp/WORKFLOW.md"), &definition)
            .unwrap()
    }

    #[test]
    fn issue_creation_lanes_use_configured_parked_and_ready_anchors() {
        let config = non_default_lane_config();

        assert_eq!(issue_creation_lane(&config, "Icebox"), None);
        assert_eq!(issue_creation_lane(&config, "ready queue"), None);
        assert_eq!(
            issue_creation_lane(&config, "Backlog"),
            Some("Backlog".to_string())
        );
        assert_eq!(
            issue_creation_lane(&config, "Queue"),
            Some("Queue".to_string())
        );
    }

    #[test]
    fn issue_create_request_validation_rejects_blank_title_and_unconfigured_lane() {
        let config = non_default_lane_config();
        assert_eq!(
            validate_issue_create_request(&config, "   ", "Ready Queue"),
            Err(("missing_title", "title must be non-empty".to_string()))
        );
        assert_eq!(
            validate_issue_create_request(&config, "Valid title", "Todo"),
            Err((
                "invalid_lane",
                "lane must be one of: Backlog, Queue".to_string()
            ))
        );
        assert_eq!(
            validate_issue_create_request(&config, "  Valid title  ", "backlog"),
            Ok(("Valid title", "Backlog".to_string()))
        );
    }

    #[test]
    fn issue_create_ui_renders_configured_lane_anchors() {
        let html = render_issues_board_html(&non_default_lane_config());

        assert!(html.contains("<option value=\"Backlog\">Backlog</option>"));
        assert!(html.contains("<option value=\"Queue\">Queue</option>"));
        assert!(!html.contains("<option value=\"Icebox\">Icebox</option>"));
        assert!(!html.contains("<option value=\"Todo\">Todo</option>"));
    }

    #[test]
    fn issue_create_attachments_validate_and_upload_via_attachment_endpoint() {
        let request: IssueCreateRequest = serde_json::from_value(json!({
            "title": "Create with file",
            "description": "body",
            "lane": "Queue",
            "attachments": [{"name": "trace.txt", "size": 42, "content_type": "text/plain"}]
        }))
        .unwrap();

        assert_eq!(request.attachments.len(), 1);
        assert!(validate_issue_create_attachments(&request.attachments).is_ok());

        let too_large = vec![IssueCreateAttachmentRequest {
            name: "large.bin".to_string(),
            size: 25 * 1024 * 1024 + 1,
            content_type: None,
        }];
        assert_eq!(
            validate_issue_create_attachments(&too_large)
                .expect_err("oversized attachments must be rejected")
                .0,
            "attachment_too_large"
        );

        let source = std::fs::read_to_string("src/http.rs").unwrap();
        assert!(source.contains("api_issue_upload_attachments"));
        assert!(source.contains("Multipart"));
        assert!(source.contains("attachment_comment_markdown"));
        assert!(source.contains("/api/v1/issues/{issue_identifier}/attachments"));
        assert!(source.contains("uploadIssueAttachments(createdIdentifier, pending"));
        assert!(!source.contains(concat!("issue_create_attachments", "_unsupported")));
        assert!(!source.contains(concat!("readAs", "DataURL")));
        assert!(!source.contains(concat!("File", "Reader")));
        assert!(!source.contains(concat!("data", "Url")));
    }

    #[test]
    fn attachment_upload_persistence_helpers_store_safe_files_and_comment_links() {
        let workspace = tempfile::tempdir().unwrap();
        let mut config = issue_board_config();
        config.workflow_dir = workspace.path().to_path_buf();
        let bytes = b"hello attachment";
        let stored = stored_attachment_file_name(0, "../trace log.txt", bytes);

        assert!(stored.starts_with("001-"));
        assert!(stored.ends_with("trace-log.txt"));
        assert!(!stored.contains("/"));
        assert!(!stored.contains(".."));

        let dir = issue_attachment_dir(&config, "LUC/649");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(&stored);
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(
            path.starts_with(
                config
                    .attachment_storage_root()
                    .join("issue-attachments/LUC-649")
            )
        );

        let url = format!(
            "/api/v1/issues/{}/attachments/{}",
            percent_encode_path_segment("LUC/649"),
            percent_encode_path_segment(&stored)
        );
        let markdown = attachment_comment_markdown(&[IssueUploadedAttachment {
            name: "trace log.txt".to_string(),
            size: bytes.len() as u64,
            content_type: Some("text/plain".to_string()),
            url: url.clone(),
            path: None,
            bytes: Vec::new(),
        }]);
        assert!(markdown.contains("Attached files:"));
        assert!(markdown.contains("[trace log.txt]"));
        assert!(markdown.contains(&url));
        assert!(markdown.contains("16 bytes"));

        let comment = comment_body_with_attachments(
            "Please see the trace.",
            &[IssueUploadedAttachment {
                name: "trace log.txt".to_string(),
                size: bytes.len() as u64,
                content_type: Some("text/plain".to_string()),
                url,
                path: None,
                bytes: Vec::new(),
            }],
        );
        assert!(comment.starts_with("Please see the trace.\n\nAttached files:"));
        assert!(comment.contains("[trace log.txt]"));
    }

    #[test]
    fn issue_detail_snapshot_exposes_comment_attachments_for_reload_rendering() {
        let snapshot = IssueDetailSnapshot {
            issue: crate::tools::LinearIssueSnapshot {
                id: crate::tools::LinearIssueId("issue-1".to_string()),
                identifier: LinearIssueIdentifier("LUC-653".to_string()),
                title: "Comment attachments".to_string(),
                description: None,
                url: None,
                state: crate::tools::LinearStateSnapshot {
                    id: crate::tools::LinearStateId("state-1".to_string()),
                    name: "Active".to_string(),
                    state_type: crate::tools::LinearStateType::Started,
                },
                assignee: None,
                branch_name: None,
                labels: Vec::new(),
                updated_at: chrono::DateTime::<Utc>::UNIX_EPOCH,
                team_id: crate::tools::LinearTeamId("team-1".to_string()),
            },
            workpad: None,
            comments: vec![LinearIssueCommentSnapshot {
                comment_id: crate::tools::LinearCommentId("comment-1".to_string()),
                body_markdown: "See attached log.".to_string(),
                url: None,
            }],
            attachments: vec![crate::tools::LinearFileAttachmentSnapshot {
                source: crate::tools::LinearFileAttachmentSource::Comment {
                    comment_id: crate::tools::LinearCommentId("comment-1".to_string()),
                },
                title: Some("trace.txt".to_string()),
                url: "/api/v1/issues/LUC-653/attachments/001-trace.txt".to_string(),
                path: ".fugue/issue-attachments/LUC-653/001-trace.txt".to_string(),
                content_type: Some("text/plain".to_string()),
                byte_len: 16,
            }],
            runtime: None,
            lanes: Vec::new(),
            lane_actions: BTreeMap::new(),
        };

        let value = serde_json::to_value(snapshot).expect("snapshot serializes");

        assert_eq!(
            value
                .pointer("/attachments/0/url")
                .and_then(serde_json::Value::as_str),
            Some("/api/v1/issues/LUC-653/attachments/001-trace.txt")
        );
        assert!(value.to_string().contains("comment-1"));
        assert!(ISSUE_DETAIL_HTML.contains("persistedCommentAttachments"));
        assert!(ISSUE_DETAIL_HTML.contains("renderPersistedAttachmentChip"));
        assert!(ISSUE_DETAIL_HTML.contains("Comment attachments"));
    }

    #[test]
    fn create_issue_editor_uses_shared_editor_contract_with_detail_editor() {
        let html = render_issues_board_html(&issue_board_config());
        assert!(html.contains("issue-editor-form"));
        assert!(html.contains("issue-editor-shell"));
        assert!(html.contains("editor-actions"));
        assert!(html.contains("issue-markdown-editor"));
        assert!(ISSUE_DETAIL_HTML.contains("issue-markdown-editor"));

        let source = std::fs::read_to_string("src/http.rs").unwrap();
        assert!(source.contains("fn issue_markdown_editor_html"));
        assert!(source.contains("fn create_issue_editor_html"));
        assert!(source.contains("__FUGUE_CREATE_EDITOR__"));
        assert!(!ISSUES_BOARD_HTML.contains(r#"<form class="create-form issue-editor-form""#));
    }

    #[test]
    fn issue_board_http_uses_issue_adapter_boundary() {
        let source = std::fs::read_to_string("src/http.rs").unwrap();
        for banned_pattern in [
            &format!("{}::from_config", "LinearClient"),
            &format!("{}::new", "LinearToolExecutor"),
            &format!("{}::from_config", "FugueImsStore"),
        ] {
            assert!(
                !source.contains(banned_pattern),
                "http.rs issue-board routes must use IssueAdapter instead of {banned_pattern}"
            );
        }
        assert!(source.contains("IssueAdapter::from_config"));
    }

    #[test]
    fn issue_board_snapshot_uses_lightweight_issue_fetch() {
        let source = std::fs::read_to_string("src/http.rs").unwrap();
        assert!(source.contains("fetch_issue_board_issues_by_states"));
        assert!(!source.contains(concat!(
            "adapter.",
            "fetch_issues_by_states",
            "(&fetch_lanes).await"
        )));
    }

    #[test]
    fn meta_rpc_request_preserves_params_as_raw_json() {
        let input: MobkitRpcRequest = serde_json::from_str(
            r#"{"method":"mob/inspect_run","params":{"run_id":"native-execution:LUC-338","limit":20}}"#,
        )
        .unwrap();

        assert_eq!(input.method, "mob/inspect_run");
        assert_eq!(
            input.params.as_ref().map(|raw| raw.get()),
            Some(r#"{"run_id":"native-execution:LUC-338","limit":20}"#)
        );

        let input: MobkitRpcRequest =
            serde_json::from_str(r#"{"method":"mob/list_runs","params":null}"#).unwrap();
        assert_eq!(input.params.as_ref().map(|raw| raw.get()), Some("null"));

        let input: MobkitRpcRequest =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":42,"method":"meta/status"}"#).unwrap();
        assert_eq!(input.request_id(), JsonRpcRequestId::Number(42));
    }

    fn console_basic_auth_config() -> ServiceConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {
                    "port": 0,
                    "meta_console": {
                        "basic_auth": {
                            "username_env": CONSOLE_TEST_USER_ENV,
                            "password_env": CONSOLE_TEST_PASSWORD_ENV,
                            "realm": "Fugue Console"
                        }
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(std::path::PathBuf::from("/tmp/fugue.yaml"), &definition)
            .unwrap()
    }

    fn issue_board_config() -> ServiceConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "linear",
                    "api_key": "token",
                    "project_slug": "project",
                    "intake_states": ["Todo"],
                    "in_flight_states": ["Planning", "Implementing"],
                    "active_states": ["todo", "Reviewing"],
                    "terminal_states": ["Done", "Canceled"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "required_states": [],
                    "lanes": {
                        "review_ready": "Ready for review",
                        "blocked": "Blocked"
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(std::path::PathBuf::from("/tmp/WORKFLOW.md"), &definition)
            .unwrap()
    }

    #[test]
    fn managed_attachments_survive_configuration_generations_and_remain_repository_scoped() {
        let root = tempfile::tempdir().unwrap();
        let mut config = issue_board_config();
        config.workflow_dir = root.path().join("generation-one");
        config.meerkat.repository_runtime = Some(crate::config::RepositoryRuntimeBinding {
            repository: crate::ensemble::config::RepoId::try_from("first".to_owned()).unwrap(),
            state_path: root.path().join("teams/first"),
        });
        let path = issue_attachment_dir(&config, "TEST-1");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("trace.txt"), "first repository").unwrap();
        config.workflow_dir = root.path().join("generation-two");
        assert_eq!(
            std::fs::read_to_string(issue_attachment_dir(&config, "TEST-1").join("trace.txt"))
                .unwrap(),
            "first repository"
        );
        config
            .meerkat
            .repository_runtime
            .as_mut()
            .unwrap()
            .state_path = root.path().join("teams/second");
        assert_ne!(issue_attachment_dir(&config, "TEST-1"), path);
    }

    #[test]
    fn jira_board_preserves_actual_statuses_when_roles_overlap() {
        let mut config = issue_board_config();
        config.tracker.kind = "jira".into();
        config.linear.lanes.parked = "Open".into();
        config.linear.lanes.ready = "Open".into();
        config.linear.lanes.success = "Closed".into();
        config.linear.lanes.canceled = "Closed".into();
        config.linear.lanes.duplicate = "Closed".into();
        config.linear.lanes.blocked = None;
        config.linear.lanes.review_ready = Some("Resolved".into());
        config.linear.lanes.deploy = Some("Reopened".into());
        config.tracker.intake_states = vec!["Open".into()];
        config.tracker.in_flight_states = vec!["In Progress".into()];
        config.tracker.active_states = vec!["Open".into(), "In Progress".into()];
        config.tracker.terminal_states = vec!["Closed".into()];
        config.deploy.enabled = true;
        config.deploy.lane = "Reopened".into();
        config.deploy.success_lane = "Closed".into();
        config.deploy.procedure = "merge-only".into();
        let board = issue_board_snapshot_from_rows(&config, vec![], &[], &[], 0, 0);
        assert_eq!(
            board
                .lanes
                .iter()
                .map(|lane| lane.name.as_str())
                .collect::<Vec<_>>(),
            ["Open", "In Progress", "Resolved", "Reopened", "Closed"]
        );
        assert_eq!(
            board.lanes.last().unwrap().kind,
            IssueBoardLaneKind::Terminal
        );
        assert_eq!(lane_action(&config, "Reopened"), Some(LaneAction::Merge));
        assert_eq!(lane_action(&config, "Deploy"), None);
        config.independent_review.enabled = true;
        assert_eq!(lane_action(&config, "Resolved"), Some(LaneAction::Review));
        assert_eq!(lane_action(&config, "Review"), None);
        assert_eq!(canonical_lane_for_state(&config, "Closed"), "Closed");
        assert_eq!(issue_creation_lanes(&config), ["Open"]);
        // A real status that happens to match a canonical label stays literal.
        config.tracker.in_flight_states.push("Active".into());
        assert_eq!(
            tracker_state_for_canonical_lane(&config, "Active"),
            "Active"
        );
    }

    #[test]
    fn configured_tracker_statuses_preserve_intake_and_success_roles() {
        let mut config = issue_board_config();
        config.linear.lanes.parked = "Open".into();
        config.linear.lanes.ready = "Open".into();
        config.linear.lanes.success = "Resolved".into();
        config.linear.lanes.canceled = "Resolved".into();
        config.tracker.intake_states = vec!["Open".into()];
        config.tracker.terminal_states = vec!["Resolved".into(), "Closed".into()];
        config.deploy.enabled = true;
        config.deploy.success_lane = "Closed".into();
        assert_eq!(canonical_lane_for_state(&config, "Open"), "Queue");
        assert_eq!(canonical_lane_for_state(&config, "Resolved"), "Done");
        assert_eq!(canonical_lane_for_state(&config, "Closed"), "Done");
    }

    #[test]
    fn issue_board_lanes_project_to_eight_canonical_names() {
        let config = issue_board_config();

        assert_eq!(
            canonical_issue_board_lanes(),
            vec![
                "Backlog", "Queue", "Active", "Review", "Deploy", "Blocked", "Done", "Canceled"
            ]
        );
        assert_eq!(
            tracker_fetch_lanes(&config),
            vec![
                "Backlog",
                "Todo",
                "Planning",
                "Implementing",
                "Reviewing",
                "Ready for review",
                "Blocked",
                "Done",
                "Canceled",
                "Duplicate"
            ]
        );
    }

    #[test]
    fn initial_issue_board_fetch_includes_terminal_lanes() {
        let config = issue_board_config();
        let fetch_lanes = tracker_fetch_lanes(&config);

        assert!(fetch_lanes.iter().any(|lane| lane == "Todo"));
        assert!(fetch_lanes.iter().any(|lane| lane == "Reviewing"));
        for terminal in ["Done", "Canceled"] {
            assert!(
                fetch_lanes
                    .iter()
                    .any(|lane| lane.eq_ignore_ascii_case(terminal)),
                "IMS board fetch must include terminal lane {terminal} so Done/Canceled columns match tracker state"
            );
        }
        assert!(canonical_issue_board_lanes().contains(&"Done".to_string()));
        assert!(canonical_issue_board_lanes().contains(&"Canceled".to_string()));
    }

    #[test]
    fn initial_issue_board_fetch_preserves_terminal_states_from_all_sources() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "linear",
                    "api_key": "token",
                    "project_slug": "project",
                    "intake_states": ["Todo", "Canceled"],
                    "in_flight_states": ["Planning", "Done"],
                    "active_states": ["Todo", "Reviewing", "Done"],
                    "terminal_states": ["Done", "Canceled"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "required_states": [],
                    "lanes": {
                        "review_ready": "Done",
                        "blocked": "Canceled",
                        "deploy": "Done",
                        "success": "Done",
                        "canceled": "Canceled"
                    }
                },
                "deploy": {
                    "enabled": true,
                    "lane": "Done",
                    "allowed_repositories": [
                        {"owner": "lukacf", "name": "meerkat-fugue"}
                    ]
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config = ServiceConfig::from_definition(
            std::path::PathBuf::from("/tmp/WORKFLOW.md"),
            &definition,
        )
        .unwrap();

        let fetch_lanes = tracker_fetch_lanes(&config);

        assert!(fetch_lanes.iter().any(|lane| lane == "Todo"));
        assert!(fetch_lanes.iter().any(|lane| lane == "Planning"));
        assert!(fetch_lanes.iter().any(|lane| lane == "Reviewing"));
        for terminal in ["Done", "Canceled"] {
            assert!(
                fetch_lanes
                    .iter()
                    .any(|lane| lane.eq_ignore_ascii_case(terminal)),
                "IMS board fetch must keep terminal lane {terminal} regardless of which config source contributed it; lanes were {fetch_lanes:?}"
            );
        }
        assert!(canonical_issue_board_lanes().contains(&"Done".to_string()));
        assert!(canonical_issue_board_lanes().contains(&"Canceled".to_string()));
    }

    #[tokio::test]
    async fn issue_board_snapshot_with_terminal_history_remains_bounded() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ims_path = temp.path().join("ims.sqlite");
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "fugue-ims",
                    "endpoint": format!("sqlite://{}", ims_path.display()),
                    "project_slug": "owner/repo",
                    "intake_states": ["Todo"],
                    "in_flight_states": ["Planning", "Implementing"],
                    "active_states": ["Todo", "Planning", "Implementing", "Reviewing"],
                    "terminal_states": ["Done", "Canceled"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "required_states": [],
                    "lanes": {
                        "review_ready": "Reviewing",
                        "blocked": "Blocked"
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config = ServiceConfig::from_definition(temp.path().join("WORKFLOW.md"), &definition)
            .expect("config");
        for index in 0..1_500 {
            crate::fugue_ims::create_issue_for_config(
                &config.tracker,
                crate::fugue_ims::CreateFugueIssue {
                    key: crate::ims::ImsIssueKey(format!("HIST-{index}")),
                    repo: "owner/repo".to_string(),
                    title: format!("Historical issue {index}"),
                    description: Some("terminal history included in the IMS board".to_string()),
                    stage: crate::ims::ImsStageName("Done".to_string()),
                    labels: Vec::new(),
                },
            )
            .expect("historical issue");
        }
        for (key, stage) in [("FUG-10", "todo"), ("FUG-11", "Reviewing")] {
            crate::fugue_ims::create_issue_for_config(
                &config.tracker,
                crate::fugue_ims::CreateFugueIssue {
                    key: crate::ims::ImsIssueKey(key.to_string()),
                    repo: "owner/repo".to_string(),
                    title: format!("Visible {key}"),
                    description: None,
                    stage: crate::ims::ImsStageName(stage.to_string()),
                    labels: Vec::new(),
                },
            )
            .expect("visible issue");
        }

        let started = std::time::Instant::now();
        let adapter = IssueAdapter::from_config(&config.tracker).expect("adapter");
        let issues = adapter
            .fetch_issue_board_issues_by_states(&tracker_fetch_lanes(&config))
            .await
            .expect("board issues");
        let snapshot = issue_board_snapshot_from_rows(&config, issues, &[], &[], 0, 0);
        let elapsed = started.elapsed();
        eprintln!(
            "IMS board fetch plus snapshot over 1,500 terminal history rows completed in {elapsed:?}"
        );

        assert_eq!(snapshot.totals.issues, 1502);
        assert!(
            snapshot
                .lanes
                .iter()
                .find(|lane| lane.name == "Done")
                .is_some_and(|lane| lane.issues.len() == 1500)
        );
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "initial board visibility path should stay under 0.5s; elapsed {elapsed:?}"
        );
    }

    #[test]
    fn canonical_lane_kind_uses_action_and_terminal_roles() {
        assert_eq!(canonical_lane_kind("Backlog"), IssueBoardLaneKind::Parked);
        assert_eq!(canonical_lane_kind("Queue"), IssueBoardLaneKind::Intake);
        assert_eq!(canonical_lane_kind("Active"), IssueBoardLaneKind::Active);
        assert_eq!(canonical_lane_kind("Done"), IssueBoardLaneKind::Terminal);
        assert_eq!(canonical_lane_kind("Review"), IssueBoardLaneKind::Attention);
        assert_eq!(canonical_lane_kind("Deploy"), IssueBoardLaneKind::Attention);
        assert_eq!(
            canonical_lane_kind("Blocked"),
            IssueBoardLaneKind::Attention
        );
        assert_eq!(
            canonical_lane_kind("Needs Human"),
            IssueBoardLaneKind::Other
        );
    }

    #[test]
    fn console_basic_auth_allows_unconfigured_console_auth() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {"port": 0}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config = ServiceConfig::from_definition(
            std::path::PathBuf::from("/tmp/fugue.yaml"),
            &definition,
        )
        .unwrap();

        authorize_console_browser(&config, &HeaderMap::new()).unwrap();
    }

    #[test]
    fn console_basic_auth_fails_closed_when_env_missing() {
        let _env = EnvVarsGuard::unset(&[CONSOLE_TEST_USER_ENV, CONSOLE_TEST_PASSWORD_ENV]);
        let config = console_basic_auth_config();

        let err = authorize_console_browser(&config, &HeaderMap::new()).unwrap_err();

        assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn console_basic_auth_fails_closed_for_whitespace_env_values() {
        let config = console_basic_auth_config();
        for values in [
            [
                (CONSOLE_TEST_USER_ENV, " "),
                (CONSOLE_TEST_PASSWORD_ENV, "secret"),
            ],
            [
                (CONSOLE_TEST_USER_ENV, "operator"),
                (CONSOLE_TEST_PASSWORD_ENV, "\t"),
            ],
        ] {
            let _env = EnvVarsGuard::set(&values);
            let err = authorize_console_browser(&config, &HeaderMap::new()).unwrap_err();
            assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[test]
    fn console_basic_auth_rejects_missing_or_wrong_credentials() {
        let _env = EnvVarsGuard::set(&[
            (CONSOLE_TEST_USER_ENV, "operator"),
            (CONSOLE_TEST_PASSWORD_ENV, "secret"),
        ]);
        let config = console_basic_auth_config();
        let err = authorize_console_browser(&config, &HeaderMap::new()).unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
        assert!(err.headers().contains_key(WWW_AUTHENTICATE));

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic b3BlcmF0b3I6d3Jvbmc="),
        );
        let err = authorize_console_browser(&config, &headers).unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn console_basic_auth_accepts_configured_credentials() {
        let _env = EnvVarsGuard::set(&[
            (CONSOLE_TEST_USER_ENV, "operator"),
            (CONSOLE_TEST_PASSWORD_ENV, "secret"),
        ]);
        let config = console_basic_auth_config();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic b3BlcmF0b3I6c2VjcmV0"),
        );

        authorize_console_browser(&config, &headers).unwrap();

        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("basic b3BlcmF0b3I6c2VjcmV0"),
        );
        authorize_console_browser(&config, &headers).unwrap();
    }

    #[test]
    fn console_browser_compat_injects_random_uuid_polyfill_once() {
        let html = "<!doctype html><html><head><title>Meta</title></head><body></body></html>";
        let patched = inject_random_uuid_polyfill(html).expect("polyfill should be injected");

        assert!(patched.contains("data-fugue-random-uuid-polyfill"));
        assert!(patched.contains("cryptoRef.randomUUID"));
        assert!(patched.contains("<head><script"));
        assert!(inject_random_uuid_polyfill(&patched).is_none());
    }

    #[test]
    fn app_navigation_keeps_primary_links_stable_before_async_data_loads() {
        let html = "<html><head></head><body></body></html>";
        for queue_enabled in [false, true] {
            for active in [
                AppPage::Home,
                AppPage::Work,
                AppPage::Ensemble,
                AppPage::Console,
            ] {
                let page = inject_app_chrome(html, active, queue_enabled).unwrap();
                let primary = page
                    .split("aria-label=\"Primary app navigation\">")
                    .nth(1)
                    .unwrap()
                    .split("</nav>")
                    .next()
                    .unwrap();
                assert_eq!(primary.matches("<a ").count(), 3);
                let home = primary.find(">Home</a>").unwrap();
                let board = primary.find(">Board</a>").unwrap();
                let agents = primary.find(">Agents</a>").unwrap();
                assert!(home < board && board < agents);
                assert!(!primary.contains("Sign out"));
                assert!(!primary.contains("Overview"));
                assert!(!primary.contains("Settings"));
                assert!(!page.contains("nav.prepend"));
                assert!(!page.contains("nav.append"));
                assert!(page.contains("Meerkat Fugue home"));
                assert_eq!(page.matches("id=\"fugueWelcome\"").count(), 1);
                assert!(page.contains("meerkat-fugue.welcome.v1"));
                assert!(!page.contains("{brand}"));
            }
        }
    }

    #[test]
    fn console_browser_compat_embeds_meta_in_fugue_shell_once() {
        let html = r#"<!doctype html><html><head><title>Meta</title></head><body class="meta"><main id="meta-root"></main></body></html>"#;
        let patched = inject_console_browser_compat_html(html, true);

        assert!(patched.contains("data-fugue-random-uuid-polyfill"));
        assert!(patched.contains("data-fugue-console-chrome"));
        assert!(patched.contains("class=\"fugue-console-embedded meta\""));
        assert!(patched.contains("Fugue home"));
        assert!(patched.contains(r#"href="/ensemble""#));
        assert!(patched.contains(r#"href="/queues""#));
        assert!(patched.contains(r#"href="/console" class="active" aria-current="page""#));
        assert!(patched.contains(">Agents</a>"));
        assert!(patched.contains("meta-root"));
        assert!(patched.contains("--bs-accent:#a34924"));

        let repatched = inject_console_browser_compat_html(&patched, true);
        assert_eq!(
            repatched.matches("data-fugue-console-chrome").count(),
            patched.matches("data-fugue-console-chrome").count()
        );
        assert_eq!(
            repatched.matches("data-fugue-random-uuid-polyfill").count(),
            patched.matches("data-fugue-random-uuid-polyfill").count()
        );
    }

    #[test]
    fn console_browser_compat_hub_links_only_to_shared_pages() {
        let html = "<html><head></head><body><main id=\"meta-root\"></main></body></html>";
        let patched = inject_console_browser_compat_html(html, false);
        assert!(patched.contains(r#"href="/ensemble""#));
        assert!(patched.contains(r#"href="/console" class="active""#));
        assert!(patched.contains(r#"href="/queues""#));
        assert!(patched.contains(">Board</a>"));
        assert!(!patched.contains(r#"href="./issues""#));
        assert!(!patched.contains(r#"href="./overview""#));
    }

    #[tokio::test]
    async fn console_browser_compat_does_not_wrap_non_html_responses() {
        let response = (
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            r#"{"ok":true}"#,
        )
            .into_response();

        let response = inject_console_browser_compat(response).await;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();

        assert_eq!(content_type, "application/json");
        assert_eq!(body, r#"{"ok":true}"#);
        assert!(!body.contains("data-fugue-console-chrome"));
        assert!(!body.contains("MEERKAT FUGUE"));
    }

    #[test]
    fn meta_rpc_auth_rejects_when_token_unconfigured() {
        let _guard = EnvVarGuard::unset(META_RPC_BEARER_TOKEN_ENV);
        let err = authorize_meta_rpc(&HeaderMap::new()).unwrap_err();

        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn meta_rpc_auth_rejects_missing_or_wrong_bearer_token() {
        let _guard = EnvVarGuard::set(META_RPC_BEARER_TOKEN_ENV, "secret-token");
        let err = authorize_meta_rpc(&HeaderMap::new()).unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer wrong-token"),
        );
        let err = authorize_meta_rpc(&headers).unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn meta_rpc_auth_accepts_configured_bearer_token() {
        let _guard = EnvVarGuard::set(META_RPC_BEARER_TOKEN_ENV, "secret-token");
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer secret-token"),
        );

        authorize_meta_rpc(&headers).unwrap();
    }

    async fn run_meta_rpc_id_match_test(
        id: JsonRpcRequestId,
        mismatched_id: serde_json::Value,
        expected_request_id: serde_json::Value,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            let request: serde_json::Value = serde_json::from_str(request_line.trim_end()).unwrap();
            assert_eq!(request["method"], "mob/observe_events");
            assert_eq!(request["params"]["run_id"], "native-execution:LUC-338");
            assert_eq!(request["params"]["nested"]["cursor"], "sequence:12");
            assert_eq!(request["id"], expected_request_id);
            let request_id = request["id"].clone();
            let stream = reader.get_mut();
            let mismatch = json!({
                "jsonrpc": "2.0",
                "id": mismatched_id,
                "result": {}
            });
            stream
                .write_all(format!("{}\n", mismatch).as_bytes())
                .await
                .unwrap();
            let response = json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": { "events": [{ "sequence": 13 }] }
            });
            stream
                .write_all(format!("{}\n", response).as_bytes())
                .await
                .unwrap();
        });
        let input: MobkitRpcRequest = serde_json::from_str(
            r#"{"method":"mob/observe_events","params":{"run_id":"native-execution:LUC-338","nested":{"cursor":"sequence:12"}}}"#,
        )
        .unwrap();

        let response = forward_meta_rpc_to_addr_with_id(&addr.to_string(), &input, id)
            .await
            .unwrap();

        assert_eq!(response.method, "mob/observe_events");
        assert_eq!(response.response["result"]["events"][0]["sequence"], 13);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn meta_rpc_forwarding_preserves_params_and_matches_string_id_value() {
        run_meta_rpc_id_match_test(
            JsonRpcRequestId::String("fugue-meta-rpc-test".to_string()),
            json!(42),
            json!("fugue-meta-rpc-test"),
        )
        .await;
    }

    #[tokio::test]
    async fn deploy_projection_records_workpad_and_success_lane_through_ims_adapter() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ims_path = temp.path().join("ims.sqlite");
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "fugue-ims",
                    "endpoint": ims_path.display().to_string(),
                    "project_slug": "lukacf/meerkat-fugue",
                    "active_states": ["Todo", "Deploy", "Done"],
                    "terminal_states": ["Done"]
                },
                "deploy": {
                    "enabled": true,
                    "allowed_repositories": [
                        {"owner": "lukacf", "name": "meerkat-fugue"}
                    ]
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let mut config =
            ServiceConfig::from_definition(temp.path().join("fugue.yaml"), &definition).unwrap();
        config.meerkat =
            crate::meta_runtime::tests::test_meerkat_config(temp.path().join("runtime"));
        let gate = crate::workgraph_gate::WorkGraphTerminalGate::for_service(&config);
        std::fs::create_dir_all(gate.state_path.parent().unwrap()).unwrap();
        let store = meerkat::SqliteWorkGraphStore::open(&gate.state_path).unwrap();
        let graph = meerkat::WorkGraphService::with_scope(
            std::sync::Arc::new(store),
            meerkat_core::mob_realm_id(&gate.mob_id).unwrap().as_str(),
            meerkat::WorkNamespace::default(),
        );
        crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            CreateFugueIssue {
                key: ImsIssueKey("LUC-631".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Deploy feature".to_string(),
                description: None,
                stage: ImsStageName("Deploy".to_string()),
                labels: Vec::new(),
            },
        )
        .expect("create issue");
        let request = DeployStartRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-631".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("lukacf".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                },
                number: GitHubPullRequestNumber(631),
                title: "Deploy feature".to_string(),
                url: "https://github.com/lukacf/meerkat-fugue/pull/631".to_string(),
                state: PullRequestState::Open,
                draft: false,
                head_branch: BranchName("codex/deploy-feature".to_string()),
                base_branch: BranchName("main".to_string()),
                head_sha: GitSha("abc123".to_string()),
            },
            recorded_head_sha: Some(GitSha("abc123".to_string())),
            ci_status: Some(crate::tools::CiStatus::Success),
            unresolved_workgraph_blockers: 0,
            idempotency_key: IdempotencyKey("deploy:LUC-631:abc123".to_string()),
            retry_deployment: None,
        };
        let now = Utc::now();
        let outcome = DeployOutcome {
            schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
            issue_identifier: "LUC-631".to_string(),
            status: DeployOutcomeStatus::Succeeded,
            failure_category: None,
            pull_request: DeployPullRequestEvidence {
                repo: request.pull_request.repo.clone(),
                number: request.pull_request.number,
                url: request.pull_request.url.clone(),
                base_branch: request.pull_request.base_branch.clone(),
                head_branch: request.pull_request.head_branch.clone(),
                head_sha: request.pull_request.head_sha.clone(),
            },
            merge: Some(crate::deploy_gate::DeployMergeEvidence {
                method: config.deploy.merge_method,
                merged_sha: GitSha("def456".to_string()),
            }),
            ota: Some(DeployOtaEvidence {
                service_name: "meerkat-fugue".to_string(),
                deployed_checkout_sha: GitSha("def456".to_string()),
                installed_binary_hashes: Vec::new(),
                health_url: "http://127.0.0.1:4001/api/v1/state".to_string(),
                health_result: "ok".to_string(),
            }),
            started_at: now,
            completed_at: now,
        };

        let adapter = IssueAdapter::from_config(&config.tracker).expect("adapter");
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: request.issue.clone(),
                heading: "## Fugue Workpad".into(),
                body_markdown:
                    "### Plan\nReviewed feature plan.\n\n### Validation\nExact-head tests passed."
                        .into(),
                create_if_missing: true,
                idempotency_key: IdempotencyKey("before-deploy".into()),
            })
            .await
            .expect("initial workpad");
        record_successful_deploy_projection(&config, &request, &outcome)
            .await
            .expect("deploy projection");

        let adapter = IssueAdapter::from_config(&config.tracker).expect("adapter");
        let inspected = adapter
            .inspect_issue(InspectIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-631".to_string()),
                },
            })
            .await
            .expect("issue");
        assert_eq!(inspected.issue.state.name, "Done");
        let workpad = &inspected.workpad.as_ref().expect("workpad").body_markdown;
        assert!(workpad.contains("Reviewed feature plan."));
        assert!(workpad.contains("Exact-head tests passed."));
        assert!(
            inspected
                .workpad
                .as_ref()
                .is_some_and(|workpad| workpad.body_markdown.contains("### Deployment"))
        );
        adapter
            .upsert_workpad(UpsertWorkpadInput {
                issue: request.issue.clone(),
                heading: "## Fugue Workpad".into(),
                body_markdown: "### Validation\nLate reviewer update.".into(),
                create_if_missing: true,
                idempotency_key: IdempotencyKey("after-deploy".into()),
            })
            .await
            .expect("late workpad update");
        assert!(
            deploy_idempotency_already_recorded(&config, &request.issue, &request.idempotency_key)
                .await
                .expect("idempotency check")
        );
        graph
            .create(meerkat::CreateWorkItemRequest {
                realm_id: None,
                namespace: None,
                title: "New post-merge finding".into(),
                description: None,
                priority: meerkat::WorkPriority::High,
                completion_policy: meerkat::WorkCompletionPolicy::SelfAttest,
                failed_child_join_policy: Default::default(),
                cancelled_child_join_policy: Default::default(),
                labels: std::collections::BTreeSet::from([
                    "fugue".into(),
                    "blocking".into(),
                    "issue:LUC-631".into(),
                ]),
                due_at: None,
                not_before: None,
                snoozed_until: None,
                external_refs: Vec::new(),
                evidence_refs: Vec::new(),
                status: Some(meerkat::WorkStatus::Open),
            })
            .await
            .unwrap();
        adapter
            .move_issue(MoveIssueInput {
                issue: request.issue.clone(),
                state: LinearStateRef::Name {
                    name: "Deploy".into(),
                },
                expected_updated_at: None,
                idempotency_key: IdempotencyKey("reopen-for-projection".into()),
            })
            .await
            .unwrap();
        // A custom success lane is terminal for this operation even if the
        // tracker's generic terminal list does not include it.
        config.deploy.success_lane = "Shipped".into();
        let refused = record_successful_deploy_projection(&config, &request, &outcome)
            .await
            .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("unresolved Fugue WorkGraph obligation"),
            "{refused}"
        );
        let blocked = adapter
            .inspect_issue(InspectIssueInput {
                issue: request.issue.clone(),
            })
            .await
            .unwrap();
        assert_eq!(blocked.issue.state.name, "Deploy");
        assert!(
            blocked.workpad.unwrap().body_markdown.contains("def456"),
            "successful merge/deploy evidence remains available after projection refusal"
        );
        // A historical receipt is useful evidence, but cannot authorize a
        // changed request with the same key or bypass current policy.
        let mut retry = request.clone();
        retry.issue = LinearIssueRef::Id {
            id: inspected.issue.id.clone(),
        };
        retry.pull_request.repo.name = GitHubRepoName("unconfigured-repository".into());
        let registry = MetaRuntimeRegistry::new(config.meerkat.clone());
        let refused = execute_deploy_transaction(config, registry, retry).await;
        assert!(
            !refused.accepted,
            "a Workpad receipt must not authorize a deploy"
        );
        assert!(refused.plan.is_some(), "retry must evaluate current policy");
        assert_eq!(
            refused.issue_identifier, "LUC-631",
            "immutable IDs must resolve to the canonical issue key in deployment evidence"
        );
    }

    #[tokio::test]
    async fn deploy_failure_projection_records_workpad_and_target_lane_through_ims_adapter() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ims_path = temp.path().join("ims.sqlite");
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "fugue-ims",
                    "endpoint": ims_path.display().to_string(),
                    "project_slug": "lukacf/meerkat-fugue",
                    "active_states": ["Todo", "Deploy", "Rework", "Blocked", "Ready for review"],
                    "terminal_states": ["Done"]
                },
                "deploy": {
                    "enabled": true,
                    "allowed_repositories": [
                        {"owner": "lukacf", "name": "meerkat-fugue"}
                    ]
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            CreateFugueIssue {
                key: ImsIssueKey("LUC-632".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Deploy failure".to_string(),
                description: None,
                stage: ImsStageName("Deploy".to_string()),
                labels: Vec::new(),
            },
        )
        .expect("create issue");
        let request = DeployStartRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-632".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("lukacf".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                },
                number: GitHubPullRequestNumber(632),
                title: "Deploy failure".to_string(),
                url: "https://github.com/lukacf/meerkat-fugue/pull/632".to_string(),
                state: PullRequestState::Open,
                draft: false,
                head_branch: BranchName("codex/deploy-failure".to_string()),
                base_branch: BranchName("main".to_string()),
                head_sha: GitSha("abc123".to_string()),
            },
            recorded_head_sha: Some(GitSha("abc123".to_string())),
            ci_status: Some(crate::tools::CiStatus::Failure),
            unresolved_workgraph_blockers: 0,
            idempotency_key: IdempotencyKey("deploy:LUC-632:abc123".to_string()),
            retry_deployment: None,
        };
        let plan = plan_deploy_gate(DeployGateInput {
            config: config.deploy.clone(),
            issue_identifier: "LUC-632".to_string(),
            pull_request: Some(request.pull_request.clone()),
            recorded_head_sha: request.recorded_head_sha.clone(),
            ci_status: request.ci_status,
            unresolved_workgraph_blockers: request.unresolved_workgraph_blockers,
            ota_update_enabled: true,
            deploy_auth_available: true,
            idempotency_key: request.idempotency_key.clone(),
        });
        let outcome = deploy_failure_outcome(
            "LUC-632".to_string(),
            &request,
            plan.failure_category.expect("failure category"),
            Utc::now(),
            Utc::now(),
        );

        record_failed_deploy_projection(&config, &request, &outcome, &plan.target_lane)
            .await
            .expect("failed deploy projection");

        let adapter = IssueAdapter::from_config(&config.tracker).expect("adapter");
        let inspected = adapter
            .inspect_issue(InspectIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-632".to_string()),
                },
            })
            .await
            .expect("issue");
        assert_eq!(inspected.issue.state.name, "Rework");
        let workpad = inspected.workpad.as_ref().expect("workpad");
        assert!(workpad.body_markdown.contains("### Deployment"));
        assert!(workpad.body_markdown.contains("deploy:LUC-632:abc123"));
        assert!(workpad.body_markdown.contains("\"status\": \"failed\""));
        assert!(
            workpad
                .body_markdown
                .contains("\"failure_category\": \"ci_not_green\"")
        );
        assert!(
            !deploy_idempotency_already_recorded(&config, &request.issue, &request.idempotency_key)
                .await
                .expect("idempotency check"),
            "failed deploy attempts must not suppress a retry with the same idempotency key"
        );
    }

    #[test]
    fn deploy_ota_failure_outcome_preserves_merge_and_ota_failure_evidence() {
        let now = Utc::now();
        let request = DeployStartRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-637".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("lukacf".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                },
                number: GitHubPullRequestNumber(637),
                title: "Deploy failure".to_string(),
                url: "https://github.com/lukacf/meerkat-fugue/pull/637".to_string(),
                state: PullRequestState::Merged,
                draft: false,
                head_branch: BranchName("codex/deploy-timeout".to_string()),
                base_branch: BranchName("main".to_string()),
                head_sha: GitSha("head123".to_string()),
            },
            recorded_head_sha: Some(GitSha("head123".to_string())),
            ci_status: Some(crate::tools::CiStatus::Success),
            unresolved_workgraph_blockers: 0,
            idempotency_key: IdempotencyKey("deploy:LUC-637:head123".to_string()),
            retry_deployment: None,
        };
        let merge = MergePrOutput {
            pull_request: request.pull_request.clone(),
            merged: true,
            merge_commit_sha: Some(GitSha("merge456".to_string())),
        };
        let ota = OtaUpdateReport {
            deployment_attempt: None,
            status: OtaUpdateStatus::Failed,
            branch: "main".to_string(),
            remote: "origin".to_string(),
            repo_path: "/srv/fugue/source/meerkat-fugue".to_string(),
            staging_path: Some("/srv/fugue/source/.fugue-ota-staging-test".to_string()),
            previous_sha: Some("old000".to_string()),
            fetched_sha: Some("merge456".to_string()),
            started_at: now,
            completed_at: now,
            steps: vec![crate::ota_update::OtaUpdateStep {
                name: "build".to_string(),
                command: "cargo build --release --bins".to_string(),
                exit_code: None,
                stdout_excerpt: String::new(),
                stderr_excerpt: "timed out".to_string(),
            }],
            message: "ota_update build: timed out after 1200000 ms".to_string(),
        };

        let outcome = deploy_ota_failure_outcome(
            "LUC-637".to_string(),
            &request,
            &merge,
            &ota,
            crate::tools::MergeMethod::Squash,
        );

        outcome.validate().expect("failed outcome remains valid");
        assert_eq!(outcome.status, DeployOutcomeStatus::Failed);
        assert_eq!(
            outcome.failure_category,
            Some(DeployFailureCategory::OtaFailed)
        );
        assert_eq!(
            outcome.merge.as_ref().map(|merge| &merge.merged_sha),
            Some(&GitSha("merge456".to_string()))
        );
        let ota_evidence = outcome.ota.expect("ota evidence");
        assert_eq!(
            ota_evidence.deployed_checkout_sha,
            GitSha("merge456".to_string())
        );
        assert!(
            ota_evidence
                .health_result
                .contains("timed out after 1200000 ms")
        );
    }

    #[test]
    fn deploy_success_requires_ota_target_to_match_merge_commit() {
        let now = Utc::now();
        let ota = OtaUpdateReport {
            deployment_attempt: None,
            status: OtaUpdateStatus::Succeeded,
            branch: "main".to_string(),
            remote: "origin".to_string(),
            repo_path: "/srv/fugue/source/meerkat-fugue".to_string(),
            staging_path: Some("/srv/fugue/source/.fugue-ota-staging-test".to_string()),
            previous_sha: Some("old000".to_string()),
            fetched_sha: Some("different789".to_string()),
            started_at: now,
            completed_at: now,
            steps: Vec::new(),
            message: "OTA update complete".to_string(),
        };

        assert_eq!(
            validate_ota_deployed_merge_sha(&GitSha("merge456".to_string()), &ota),
            Err(DeploySuccessEvidenceError::OtaTargetMismatch {
                merged_sha: GitSha("merge456".to_string()),
                ota_sha: GitSha("different789".to_string())
            })
        );
    }

    #[test]
    fn deploy_success_outcome_records_merge_target_and_installed_binary_hashes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bin_path = temp.path().join("meerkat-fugue");
        std::fs::write(&bin_path, b"hello").expect("write binary");
        let command = format!(
            "install -m 0755 target/release/meerkat-fugue {0}.new && mv {0}.new {0}",
            bin_path.display()
        );
        let now = Utc::now();
        let request = DeployStartRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-642".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("lukacf".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                },
                number: GitHubPullRequestNumber(642),
                title: "Deploy cleanup".to_string(),
                url: "https://github.com/lukacf/meerkat-fugue/pull/642".to_string(),
                state: PullRequestState::Merged,
                draft: false,
                head_branch: BranchName("codex/deploy-cleanup".to_string()),
                base_branch: BranchName("main".to_string()),
                head_sha: GitSha("head123".to_string()),
            },
            recorded_head_sha: Some(GitSha("head123".to_string())),
            ci_status: Some(crate::tools::CiStatus::Success),
            unresolved_workgraph_blockers: 0,
            idempotency_key: IdempotencyKey("deploy:LUC-642:head123".to_string()),
            retry_deployment: None,
        };
        let ota = OtaUpdateReport {
            deployment_attempt: None,
            status: OtaUpdateStatus::Succeeded,
            branch: "main".to_string(),
            remote: "origin".to_string(),
            repo_path: temp.path().display().to_string(),
            staging_path: Some(
                temp.path()
                    .join(".fugue-ota-staging-test")
                    .display()
                    .to_string(),
            ),
            previous_sha: Some("old000".to_string()),
            fetched_sha: Some("merge456".to_string()),
            started_at: now,
            completed_at: now,
            steps: Vec::new(),
            message: "OTA update complete".to_string(),
        };
        let ota_config = crate::config::OtaUpdateConfig {
            enabled: true,
            repo_path: temp.path().to_path_buf(),
            branch: "main".to_string(),
            remote: "origin".to_string(),
            build_command: "cargo build --release --bins".to_string(),
            health_check_command: Some("meerkat-fugue --help".to_string()),
            deploy_command: command,
            timeout_ms: 60_000,
            require_clean: true,
            lock_path: None,
            trigger_token_env: None,
        };

        let outcome = deploy_success_outcome(
            "LUC-642".to_string(),
            &request,
            GitSha("merge456".to_string()),
            &ota,
            Some(&ota_config),
            crate::tools::MergeMethod::Squash,
        );

        outcome.validate().expect("valid success outcome");
        assert_eq!(
            outcome.merge.as_ref().map(|merge| &merge.merged_sha),
            Some(&GitSha("merge456".to_string()))
        );
        let ota_evidence = outcome.ota.expect("ota evidence");
        assert_eq!(
            ota_evidence.deployed_checkout_sha,
            GitSha("merge456".to_string())
        );
        assert_eq!(ota_evidence.installed_binary_hashes.len(), 1);
        assert_eq!(
            ota_evidence.installed_binary_hashes[0].path,
            bin_path.display().to_string()
        );
        assert_eq!(
            ota_evidence.installed_binary_hashes[0].sha256,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(ota_evidence.health_url, "command:meerkat-fugue --help");

        let mut merged = merge_only_success_outcome(
            "LUC-642".to_string(),
            &request,
            GitSha("merge456".to_string()),
            crate::tools::MergeMethod::Squash,
        );
        merged.validate().expect("valid merge-only outcome");
        assert_eq!(merged.status, DeployOutcomeStatus::Merged);
        assert!(merged.ota.is_none());
        let workpad = format!(
            "Idempotency-Key: `{}`\n```json\n{}\n```",
            request.idempotency_key.0,
            serde_json::to_string(&merged).expect("outcome")
        );
        assert!(deploy_success_idempotency_recorded_in_workpad(
            &workpad,
            &request.idempotency_key
        ));
        merged.ota = Some(ota_evidence);
        assert_eq!(
            merged.validate(),
            Err(crate::deploy_gate::DeployOutcomeError::MergeOnlyWithDeployment)
        );
        merged.ota = None;
        merged.merge = None;
        assert_eq!(
            merged.validate(),
            Err(crate::deploy_gate::DeployOutcomeError::MissingField(
                "merge"
            ))
        );
    }

    #[tokio::test]
    async fn independent_review_http_rejects_caller_verdicts_before_projection() {
        use tower::ServiceExt;

        let root = tempfile::tempdir().unwrap();
        let (_, owner) = managed_test_router(root.path(), None).await;
        let mut config = owner.config().effective().clone();
        config.independent_review.enabled = true;
        crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            CreateFugueIssue {
                key: ImsIssueKey("TEST-1".into()),
                repo: "test/repository".into(),
                title: "Review must use native results".into(),
                description: None,
                stage: ImsStageName("Todo".into()),
                labels: Vec::new(),
            },
        )
        .unwrap();
        let adapter = IssueAdapter::from_config(&config.tracker).unwrap();
        let issue = LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier("TEST-1".into()),
        };
        let before = adapter
            .inspect_issue(InspectIssueInput {
                issue: issue.clone(),
            })
            .await
            .unwrap();
        let request = IndependentReviewCompletionRequest {
            issue: issue.clone(),
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("test".into()),
                    name: GitHubRepoName("repository".into()),
                },
                number: GitHubPullRequestNumber(1),
                title: "Review candidate".into(),
                url: "https://github.com/test/repository/pull/1".into(),
                state: PullRequestState::Open,
                draft: false,
                head_branch: BranchName("feature".into()),
                base_branch: BranchName("main".into()),
                head_sha: GitSha("a".repeat(40)),
            },
            head_sha: GitSha("a".repeat(40)),
            workgraph_scope: crate::independent_review::IndependentReviewWorkGraphScope {
                realm_id: meerkat_core::mob_realm_id(&config.meerkat.mob_id())
                    .unwrap()
                    .to_string(),
                namespace: meerkat::WorkNamespace::default().as_str().into(),
            },
            native_review: crate::native_review::ReviewReference {
                runs: vec![meerkat_mob::RunId::new()],
            },
            idempotency_key: IdempotencyKey("review:test-1:round-1".into()),
        };
        let (refresh, _receiver) = mpsc::channel(1);
        let app = router(
            owner.shared_state(),
            config,
            refresh,
            owner.internal_sender(),
            owner.meta_runtime_registry(),
            None,
        )
        .await
        .unwrap();
        for endpoint in ["complete-independent-review", "complete-final-pr-gate"] {
            for forged_field in ["verdicts", "timed_out", "missing_native_review"] {
                let mut payload = serde_json::to_value(&request).unwrap();
                match forged_field {
                    "verdicts" => {
                        payload["verdicts"] = json!([test_review_verdict(
                            "claimed-reviewer",
                            crate::independent_review::IndependentReviewVerdictStatus::Green,
                        )])
                    }
                    "timed_out" => payload["timed_out"] = json!(true),
                    _ => {
                        payload.as_object_mut().unwrap().remove("native_review");
                    }
                }
                let response = app
                    .clone()
                    .oneshot(
                        axum::http::Request::post(format!("/api/v1/kapellmeister/{endpoint}"))
                            .header("host", "127.0.0.1:4719")
                            .header("origin", "http://127.0.0.1:4719")
                            .header("content-type", "application/json")
                            .body(axum::body::Body::from(payload.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "{forged_field}"
                );
            }
        }
        let admission = json!({
            "issue":issue, "repo":request.pull_request.repo,
            "pull_request":1, "head_sha":"a".repeat(40),
            "reviewers":{"domain":["repo:a", "repo:b", "repo:c"]},
            "task":"Review the candidate", "idempotency_key":"test-request"
        });
        for field in ["round", "verdicts", "accepted", "tracker_context"] {
            let mut forged = admission.clone();
            forged[field] = json!("caller-owned authority");
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::post("/api/v1/kapellmeister/admit-independent-review")
                        .header("host", "127.0.0.1:4719")
                        .header("origin", "http://127.0.0.1:4719")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(forged.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{field}"
            );
        }
        let after = adapter
            .inspect_issue(InspectIssueInput { issue })
            .await
            .unwrap();
        assert_eq!(
            after, before,
            "untrusted completion must not change issue, comments or workpad"
        );
    }

    #[tokio::test]
    async fn independent_review_red_projection_records_comment_workgraph_and_rework_lane() {
        let temp = tempfile::tempdir().expect("tempdir");
        let ims_path = temp.path().join("ims.sqlite");
        let state_root = temp.path().join("meerkat-state");
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "fugue-ims",
                    "endpoint": ims_path.display().to_string(),
                    "project_slug": "lukacf/meerkat-fugue",
                    "active_states": ["Todo", "Do Review", "Ready for review", "Rework", "Blocked"],
                    "terminal_states": ["Done"]
                },
                "meerkat": {
                    "state_root": state_root.display().to_string()
                },
                "independent_review": {
                    "enabled": true,
                    "lane": "Do Review",
                    "success_lane": "Ready for review",
                    "rework_lane": "Rework",
                    "blocked_lane": "Blocked",
                    "reviewer_count": 3
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            CreateFugueIssue {
                key: ImsIssueKey("LUC-633".to_string()),
                repo: "lukacf/meerkat-fugue".to_string(),
                title: "Independent review".to_string(),
                description: None,
                stage: ImsStageName("Do Review".to_string()),
                labels: Vec::new(),
            },
        )
        .expect("create issue");
        let verdicts = vec![
            test_review_verdict(
                "runtime-boundary",
                crate::independent_review::IndependentReviewVerdictStatus::Red,
            ),
            test_review_verdict(
                "operator-workflow",
                crate::independent_review::IndependentReviewVerdictStatus::Green,
            ),
            test_review_verdict(
                "test-coverage",
                crate::independent_review::IndependentReviewVerdictStatus::Green,
            ),
        ];
        let request = IndependentReviewCompletionRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-633".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
                    host: None,
                    owner: GitHubOwner("lukacf".to_string()),
                    name: GitHubRepoName("meerkat-fugue".to_string()),
                },
                number: GitHubPullRequestNumber(633),
                title: "Independent review".to_string(),
                url: "https://github.com/lukacf/meerkat-fugue/pull/633".to_string(),
                state: PullRequestState::Open,
                draft: false,
                head_branch: BranchName("codex/review-feature".to_string()),
                base_branch: BranchName("main".to_string()),
                head_sha: GitSha("abc123".to_string()),
            },
            head_sha: GitSha("abc123".to_string()),
            workgraph_scope: crate::independent_review::IndependentReviewWorkGraphScope {
                realm_id: meerkat_core::mob_realm_id(&config.meerkat.mob_id())
                    .unwrap()
                    .to_string(),
                namespace: WorkNamespace::default().as_str().to_string(),
            },
            native_review: crate::native_review::ReviewReference {
                runs: vec![meerkat_mob::RunId::new()],
            },
            idempotency_key: IdempotencyKey("review:LUC-633:abc123:1".to_string()),
        };
        let plan = aggregate_independent_review(IndependentReviewAggregationInput {
            config: config.independent_review.clone(),
            issue_identifier: "LUC-633".to_string(),
            head_sha: request.head_sha.clone(),
            verdicts: verdicts.clone(),
            timed_out: false,
        });

        let mut mismatched = request.clone();
        mismatched.pull_request.head_sha = GitSha("new-unreviewed-head".into());
        assert!(
            record_independent_review_projection(&config, &mismatched, "LUC-633", &plan, &verdicts)
                .await
                .is_err(),
            "a PR snapshot and review must name the same head before any projection"
        );

        let mut foreign_scope = request.clone();
        foreign_scope.workgraph_scope.realm_id = "another-repository".into();
        assert!(
            record_independent_review_projection(
                &config,
                &foreign_scope,
                "LUC-633",
                &plan,
                &verdicts
            )
            .await
            .is_err(),
            "review obligations must not escape the repository's native realm"
        );

        let mut foreign_namespace = request.clone();
        foreign_namespace.workgraph_scope.namespace = "hidden-review".into();
        assert!(
            record_independent_review_projection(
                &config,
                &foreign_namespace,
                "LUC-633",
                &plan,
                &verdicts
            )
            .await
            .is_err(),
            "review blockers must use the namespace granted to repository agents and merge guards"
        );

        let projection =
            record_independent_review_projection(&config, &request, "LUC-633", &plan, &verdicts)
                .await
                .expect("review projection");

        assert_eq!(projection.target_lane, "Rework");
        assert!(projection.findings_comment_id.is_some());
        assert_eq!(projection.workgraph_items.len(), 1);
        assert_eq!(
            projection.workgraph_items[0].realm_id,
            meerkat_core::mob_realm_id(&config.meerkat.mob_id())
                .unwrap()
                .as_str()
        );
        assert_eq!(
            projection.workgraph_items[0].namespace,
            WorkNamespace::default().as_str()
        );
        let repeated_projection =
            record_independent_review_projection(&config, &request, "LUC-633", &plan, &verdicts)
                .await
                .expect("repeat review projection");
        assert_eq!(repeated_projection.target_lane, "Rework");
        assert!(
            repeated_projection.findings_comment_id.is_none(),
            "repeated independent review completion must not duplicate the findings comment"
        );
        assert_eq!(repeated_projection.workgraph_items.len(), 1);
        let adapter = IssueAdapter::from_config(&config.tracker).expect("adapter");
        let inspected = adapter
            .inspect_issue(InspectIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-633".to_string()),
                },
            })
            .await
            .expect("issue");
        assert_eq!(inspected.issue.state.name, "Rework");
        assert!(
            inspected
                .workpad
                .as_ref()
                .is_some_and(|workpad| workpad.body_markdown.contains("### Independent Review"))
        );
        let store = SqliteWorkGraphStore::open(
            meta_runtime_state_path(&config.meerkat).join("workgraph.sqlite3"),
        )
        .expect("workgraph store");
        let service = WorkGraphService::with_scope(
            Arc::new(store),
            meerkat_core::mob_realm_id(&config.meerkat.mob_id())
                .unwrap()
                .as_str(),
            WorkNamespace::default(),
        );
        let items = service
            .list(WorkItemFilter {
                realm_id: Some(
                    meerkat_core::mob_realm_id(&config.meerkat.mob_id())
                        .unwrap()
                        .to_string(),
                ),
                namespace: Some(WorkNamespace::default()),
                all_namespaces: false,
                statuses: Vec::new(),
                labels: vec![
                    "fugue".to_string(),
                    "issue:LUC-633".to_string(),
                    "review".to_string(),
                    "blocking".to_string(),
                ],
                include_terminal: false,
                limit: None,
            })
            .await
            .expect("workgraph items");
        assert_eq!(items.len(), 1);
        let gate = crate::workgraph_gate::WorkGraphTerminalGate::for_service(&config);
        assert_eq!(
            gate.unresolved_blocking_items("LUC-633")
                .await
                .unwrap()
                .len(),
            1,
            "merge and terminal guards must see blockers created by independent review"
        );
    }

    fn test_review_verdict(
        reviewer_id: &str,
        status: crate::independent_review::IndependentReviewVerdictStatus,
    ) -> crate::independent_review::IndependentReviewVerdict {
        let findings = if status == crate::independent_review::IndependentReviewVerdictStatus::Red {
            vec![crate::independent_review::IndependentReviewFinding {
                severity: crate::independent_review::IndependentReviewFindingSeverity::Major,
                title: "Missing guard".to_string(),
                detail: "The deploy path lacks a guard.".to_string(),
                file: Some("src/http.rs".to_string()),
                line: Some(42),
            }]
        } else {
            Vec::new()
        };
        crate::independent_review::IndependentReviewVerdict {
            schema_version: crate::independent_review::INDEPENDENT_REVIEW_SCHEMA_VERSION
                .to_string(),
            reviewer_id: reviewer_id.to_string(),
            focus: format!("{reviewer_id} focus"),
            verdict: status,
            summary: "reviewed".to_string(),
            findings,
            evidence: crate::independent_review::IndependentReviewEvidence {
                base: "main".to_string(),
                head_sha: GitSha("abc123".to_string()),
                commands: vec!["git diff origin/main...HEAD".to_string()],
                checked_paths: vec!["src/http.rs".to_string()],
            },
        }
    }

    #[tokio::test]
    async fn meta_rpc_forwarding_matches_numeric_id_value() {
        run_meta_rpc_id_match_test(JsonRpcRequestId::Number(42), json!("42"), json!(42)).await;
    }
}
