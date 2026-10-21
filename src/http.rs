use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
use meerkat::{
    CreateWorkItemRequest, SqliteWorkGraphStore, WorkCompletionPolicy, WorkGraphService,
    WorkItemFilter, WorkNamespace, WorkPriority, WorkStatus,
};
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
use crate::github_tools::GitHubToolHost;
use crate::independent_review::{
    IndependentReviewAggregationInput, IndependentReviewCompletionRequest,
    IndependentReviewDecision, IndependentReviewStartRequest, IndependentReviewWorkGraphScope,
    aggregate_independent_review,
};
use crate::linear_adapter::IssueAdapter;
use crate::meta_runtime::{MetaRuntimeRegistry, meta_runtime_state_path};
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
    ensemble: Option<Arc<crate::ensemble::runtime::EnsembleRuntime>>,
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
    ensemble: Option<Arc<crate::ensemble::runtime::EnsembleRuntime>>,
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
    ensemble: Option<Arc<crate::ensemble::runtime::EnsembleRuntime>>,
) -> anyhow::Result<Router> {
    let app_state = AppState {
        state,
        config: config.clone(),
        refresh_tx,
        internal_tx,
    };
    let app = Router::new()
        .route("/api/v1/build", get(crate::build_identity::endpoint))
        .route("/", get(dashboard))
        .route("/overview", get(dashboard))
        .route("/issues", get(issues_board))
        .route("/issues/{issue_identifier}", get(issue_detail_page))
        .route("/api/v1/state", get(api_state))
        .route("/api/v1/issues/board", get(api_issue_board))
        .route("/api/v1/issues", post(api_issue_create))
        .route("/api/v1/issues/{issue_identifier}", get(api_issue_detail))
        .route(
            "/api/v1/issues/{issue_identifier}/comments",
            post(api_issue_add_comment),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/attachments",
            post(api_issue_upload_attachments),
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
            "/api/v1/kapellmeister/complete-independent-review",
            post(api_kapellmeister_complete_independent_review),
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
    if let Some(runtime) = ensemble {
        let routes = crate::ensemble::http::integrated_router(runtime).await?;
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

async fn inject_console_browser_compat(response: Response) -> Response {
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
    let html = inject_console_browser_compat_html(&html);
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(html))
}

fn inject_console_browser_compat_html(html: &str) -> String {
    let mut patched = html.to_string();
    if let Some(next) = inject_random_uuid_polyfill(&patched) {
        patched = next;
    }
    if let Some(next) = inject_console_app_chrome(&patched) {
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

fn inject_console_app_chrome(html: &str) -> Option<String> {
    inject_app_chrome(html, AppPage::Console, true)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppPage {
    Console,
    Ensemble,
}

pub(crate) fn inject_app_chrome(
    html: &str,
    active: AppPage,
    queue_enabled: bool,
) -> Option<String> {
    const MARKER: &str = "data-fugue-console-chrome";
    const STYLE: &str = r#"<style data-fugue-console-chrome>
:root { color-scheme: light; --bs-paper: #ECEAE4; --bs-paper-soft: #F6F4EE; --bs-paper-raised: #FAF8F2; --bs-ink: #1C1A17; --bs-muted: #6B6760; --bs-subtle: #A8A49B; --bs-line: #D6D2C8; --bs-accent: #D95A1E; --bs-accent-dark: #E76A2F; --bs-ok: #3E6B4F; --bs-warn: #A56A18; --bs-crit: #A43F32; --bs-font-sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; --bs-font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; --bg: var(--bs-paper); --surface: var(--bs-paper-soft); --ink: var(--bs-ink); --muted: var(--bs-muted); --hair: var(--bs-line); --accent: var(--bs-accent); --r-sm: 2px; --r-md: 6px; --type-micro: 10px; --type-rail: 11px; --type-meta: 12px; --type-body: 13px; --type-card: 14px; --type-panel-heading: 15px; --type-heading: 16px; --line-compact: 1.35; --control-h: 38px; }
body.fugue-console-embedded { margin:0; padding-top:var(--control-h); background:var(--bg); color:var(--ink); }
.fugue-console-rail { position:fixed; z-index:2147483000; inset:0 0 auto 0; height:var(--control-h); display:flex; align-items:center; border-bottom:1px solid var(--ink); background:var(--surface); color:var(--ink); font-family:var(--bs-font-mono); font-size:var(--type-rail); letter-spacing:.12em; text-transform:uppercase; line-height:var(--line-compact); }
.fugue-console-rail a { color:inherit; text-decoration:none; }
.fugue-console-brand { height:var(--control-h); display:flex; align-items:center; gap:10px; padding:0 16px; border-right:1px solid var(--hair); font-weight:500; }
.fugue-console-mark { width:12px; height:12px; border:1px solid var(--ink); transform:rotate(45deg); flex:0 0 auto; }
.fugue-console-tabs { display:flex; height:var(--control-h); }
.fugue-console-tabs a { display:flex; align-items:center; padding:0 18px; border-right:1px solid var(--hair); }
.fugue-console-tabs a.active { background:var(--ink); color:var(--bg); }
.fugue-console-spacer { flex:1; }
.fugue-console-meta { display:flex; height:var(--control-h); }
.fugue-console-meta span { display:flex; align-items:center; padding:0 10px; border-left:1px solid var(--hair); color:var(--muted); }
@media(max-width:760px) { .fugue-console-meta { display:none; } .fugue-console-brand { padding:0 10px; font-size:9px; } .fugue-console-brand span:last-child { display:none; } .fugue-console-tabs { overflow-x:auto; } .fugue-console-tabs a { padding:0 10px; font-size:10px; white-space:nowrap; } }
</style>"#;
    let mut nav = String::new();
    if queue_enabled {
        nav.push_str(r#"<a href="/overview">OVERVIEW</a><a href="/issues">IMS</a>"#);
        nav.push_str(if active == AppPage::Console {
            r#"<a class="active" href="/console">CONSOLE</a>"#
        } else {
            r#"<a href="/console">CONSOLE</a>"#
        });
    }
    nav.push_str(if active == AppPage::Ensemble {
        r#"<a class="active" href="/ensemble">ENSEMBLE</a>"#
    } else {
        r#"<a href="/ensemble">ENSEMBLE</a>"#
    });
    let home = if queue_enabled {
        "/overview"
    } else {
        "/ensemble"
    };
    let label = if active == AppPage::Ensemble {
        "REPOSITORY TEAMS"
    } else {
        "META CONSOLE"
    };
    let chrome = format!(
        r#"<div class="fugue-console-rail" data-fugue-console-chrome><a class="fugue-console-brand" href="{home}" aria-label="Meerkat Fugue overview"><span class="fugue-console-mark" aria-hidden="true"></span><span>MEERKAT FUGUE</span></a><nav class="fugue-console-tabs" aria-label="Primary app navigation">{nav}</nav><div class="fugue-console-spacer"></div><div class="fugue-console-meta" aria-label="Console context"><span>{label}</span></div></div>"#
    );

    if html.contains(MARKER) {
        return None;
    }

    let mut patched = html.to_string();
    let head_insertion = patched
        .find("</head>")
        .or_else(|| patched.find("<body"))
        .unwrap_or(0);
    patched.insert_str(head_insertion, STYLE);

    let Some(body_start) = patched.find("<body") else {
        return Some(format!("{STYLE}{chrome}{patched}"));
    };
    let Some(body_tag_end_offset) = patched[body_start..].find('>') else {
        return Some(format!("{STYLE}{chrome}{patched}"));
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
    if !replacement.contains("data-bs-theme=") {
        replacement.insert_str(replacement.len() - 1, " data-bs-theme=\"paper\"");
    }
    patched.replace_range(body_start..=body_tag_end, &replacement);
    let body_tag_end = body_start + replacement.len();
    patched.insert_str(body_tag_end, &chrome);
    Some(patched)
}

struct DashboardView {
    running: usize,
    retrying: usize,
    tokens: u64,
    seconds: f64,
    error: Option<String>,
    shadow_health: String,
    shadow_recommendations: usize,
    shadow_comparisons: usize,
    shadow_authority: String,
    running_rows: String,
    retry_rows: String,
    console_enabled: bool,
    in_flight_count: usize,
    picker_tabs: String,
    focus_identifier: String,
    focus_title: String,
    focus_lane: String,
    focus_runtime: String,
    active_role: String,
    current_action: String,
    workpad_files: String,
    activity_rows: String,
}

async fn dashboard(State(app): State<AppState>, headers: HeaderMap) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let snapshot = snapshot(&app.state, &app.config).await;
    let focus = snapshot.running.first();
    let in_flight_count = snapshot.running.len();
    let focus_identifier = focus
        .map(|row| row.issue_identifier.clone())
        .unwrap_or_else(|| "NO MOB".to_string());
    let focus_title = focus
        .map(|row| row.issue.title.clone())
        .unwrap_or_else(|| "Nothing is running right now.".to_string());
    let focus_lane = focus
        .map(|row| row.state.clone())
        .unwrap_or_else(|| "idle".to_string());
    let focus_runtime = focus
        .map(|row| compact_duration((Utc::now() - row.started_at).num_seconds().max(0) as u64))
        .unwrap_or_else(|| "unavailable".to_string());
    let active_role = focus
        .and_then(|row| role_from_state(&row.state))
        .unwrap_or("MOB")
        .to_string();
    let current_action = focus
        .and_then(|row| row.last_message.as_ref().or(row.last_event.as_ref()))
        .cloned()
        .unwrap_or_else(|| "Awaiting runtime event from Fugue.".to_string());
    let picker_tabs = dashboard_picker_tabs(&snapshot.running);
    let activity_rows = dashboard_activity_rows(&snapshot.running, &snapshot.retrying);
    // rework-note: the current runtime snapshot does not expose a structured workpad diff or live topology stream here; render honest runtime evidence instead of prototype file fixtures.
    let workpad_files = focus
        .map(|row| {
            format!(
                r#"<div class="file"><span class="meta">{} · {}</span><span class="meta">{}t</span></div>"#,
                html_escape(&row.workspace_key),
                html_escape(&row.state),
                row.turn_count
            )
        })
        .unwrap_or_else(|| {
            // rework-note: no focused runtime means the dashboard has no live workpad or turn-count evidence; render an explicit unavailable state instead of a measured-looking zero.
            r#"<div class="file unavailable"><span class="meta">No active mob workpad evidence available</span><span class="meta">turns unavailable</span></div>"#.to_string()
        });
    let view = DashboardView {
        running: snapshot.counts.running,
        retrying: snapshot.counts.retrying,
        tokens: snapshot.meerkat_totals.total_tokens,
        seconds: snapshot.meerkat_totals.seconds_running,
        error: snapshot.last_error.clone(),
        shadow_health: format!("{:?}", snapshot.kapellmeister_shadow.health),
        shadow_recommendations: snapshot.kapellmeister_shadow.recommendation_count,
        shadow_comparisons: snapshot.kapellmeister_shadow.comparison_count,
        shadow_authority: format!("{:?}", snapshot.kapellmeister_shadow.authority.mode),
        running_rows: snapshot
            .running
            .iter()
            .map(|row| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    html_escape(&row.issue_identifier),
                    html_escape(&format!("{:?}", row.policy_mode)),
                    html_escape(&row.state),
                    row.session_id
                        .as_deref()
                        .map(html_escape)
                        .unwrap_or_default(),
                    row.turn_count,
                    row.last_event
                        .as_deref()
                        .map(html_escape)
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
            .join(""),
        retry_rows: snapshot
            .retrying
            .iter()
            .map(|row| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    html_escape(&row.issue_identifier),
                    html_escape(&format!("{:?}", row.policy_mode)),
                    row.attempt,
                    row.due_at,
                    row.error.as_deref().map(html_escape).unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
            .join(""),
        console_enabled: meta_console_enabled(&app.config),
        in_flight_count,
        picker_tabs,
        focus_identifier,
        focus_title,
        focus_lane,
        focus_runtime,
        active_role,
        current_action,
        workpad_files,
        activity_rows,
    };
    Html(render_dashboard_html(&view)).into_response()
}

fn compact_duration(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}s", seconds)
    }
}

fn role_from_state(state: &str) -> Option<&'static str> {
    let state = state.to_ascii_lowercase();
    if state.contains("plan") {
        Some("PLANNER")
    } else if state.contains("review") {
        Some("REVIEWER")
    } else if state.contains("final") {
        Some("PUBLISHER")
    } else if state.contains("implement") || state.contains("progress") || state.contains("active")
    {
        Some("CODER")
    } else {
        None
    }
}

fn phase_label_for_runtime_state(state: &str) -> String {
    phase_label_for_state(state)
        .unwrap_or("fugue:phase:implementing")
        .to_string()
}

fn dashboard_picker_tabs(running: &[RunningRow]) -> String {
    if running.is_empty() {
        return r#"<div class="mob-tab focused"><span class="radio"></span><span class="mob-tab-main"><span class="meta">NO MOB IN FLIGHT</span><div class="mob-tab-title">Move an issue to Queue or Active to start one.</div></span><span class="meta">0s<br>OPEN IMS</span></div>"#.to_string();
    }
    running
        .iter()
        .take(3)
        .enumerate()
        .map(|(index, row)| {
            let focused = index == 0;
            let runtime = compact_duration((Utc::now() - row.started_at).num_seconds().max(0) as u64);
            let state_label = if focused {
                r#"<span class="badge">● FOCUSED</span>"#.to_string()
            } else {
                html_escape(&phase_label_for_runtime_state(&row.state))
            };
            format!(
                r#"<a class="mob-tab {focused_class}" href="/issues/{id}"><span class="radio"></span><span class="mob-tab-main"><span class="meta">{id} {state_label}</span><div class="mob-tab-title">{title}</div></span><span class="meta">{runtime}<br>{role}</span></a>"#,
                focused_class = if focused { "focused" } else { "" },
                id = html_escape(&row.issue_identifier),
                state_label = state_label,
                title = html_escape(&row.issue.title),
                runtime = html_escape(&runtime),
                role = if focused {
                    html_escape(role_from_state(&row.state).unwrap_or("MOB"))
                } else {
                    "SWITCH →".to_string()
                },
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn dashboard_activity_rows(running: &[RunningRow], retrying: &[RetryRow]) -> String {
    let mut rows = Vec::new();
    for row in running.iter().take(5) {
        rows.push(format!(
            r#"<div class="log-row"><span class="meta">{time}</span><span class="meta">{id}</span><span>{message}</span></div>"#,
            time = row
                .last_event_at
                .map(|time| time.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "running".to_string()),
            id = html_escape(&row.issue_identifier),
            message = html_escape(row.last_event.as_deref().unwrap_or("mob running")),
        ));
    }
    for row in retrying.iter().take(3) {
        rows.push(format!(
            r#"<div class="log-row"><span class="meta">retry</span><span class="meta">{id}</span><span>{message}</span></div>"#,
            id = html_escape(&row.issue_identifier),
            message = html_escape(row.error.as_deref().unwrap_or("retry scheduled")),
        ));
    }
    if rows.is_empty() {
        rows.push(r#"<div class="log-row"><span class="meta">idle</span><span class="meta">—</span><span>No mob runtime events are currently active.</span></div>"#.to_string());
    }
    rows.join("")
}

fn render_dashboard_html(view: &DashboardView) -> String {
    let console_hint = if view.console_enabled { "LIVE" } else { "OFF" };
    let _legacy_runtime_tables = (
        &view.shadow_health,
        &view.shadow_comparisons,
        &view.shadow_authority,
        &view.running_rows,
        &view.retry_rows,
    );
    let error = view
        .error
        .as_ref()
        .map(|error| format!(r#"<p class="error">{}</p>"#, html_escape(error)))
        .unwrap_or_default();
    let html = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Meerkat Fugue</title>
  <style>
    :root { color-scheme: light; --bs-paper: #ECEAE4; --bs-paper-soft: #F6F4EE; --bs-paper-raised: #FAF8F2; --bs-ink: #1C1A17; --bs-muted: #6B6760; --bs-subtle: #A8A49B; --bs-line: #D6D2C8; --bs-accent: #D95A1E; --bs-accent-dark: #E76A2F; --bs-ok: #3E6B4F; --bs-warn: #A56A18; --bs-crit: #A43F32; --bs-font-sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; --bs-font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; --bg: var(--bs-paper); --surface: var(--bs-paper-soft); --paper: var(--bs-paper-raised); --ink: var(--bs-ink); --muted: var(--bs-muted); --subtle: var(--bs-subtle); --hair: var(--bs-line); --accent: var(--bs-accent); --ok: var(--bs-ok); --warn: var(--bs-warn); --crit: var(--bs-crit); --r-sm: 2px; --r-md: 6px; --type-micro: 10px; --type-rail: 11px; --type-meta: 12px; --type-body: 13px; --type-card: 14px; --type-panel-heading: 15px; --type-heading: 16px; --line-compact: 1.35; --control-h: 38px; --font-display: var(--bs-font-sans); --shadow-sheet: 0 24px 60px rgba(28,26,23,.28); }
    * { box-sizing: border-box; } body { margin:0; min-height:100vh; overflow:auto; background:var(--bg); color:var(--ink); font-family:var(--bs-font-sans); font-size:var(--type-body); line-height:var(--line-compact); } a { color:inherit; text-decoration:none; }
    .mono, .top-rail, .eyebrow, .chip, .meta { font-family:var(--bs-font-mono); text-transform:uppercase; letter-spacing:.12em; font-size:var(--type-rail); line-height:var(--line-compact); }
    .top-rail { min-height:var(--control-h); display:flex; align-items:stretch; flex-wrap:nowrap; border-bottom:1px solid var(--ink); background:var(--surface); position:sticky; top:0; z-index:4; }
    .brand { height:var(--control-h); display:flex; align-items:center; gap:10px; padding:0 16px; border-right:1px solid var(--hair); font-weight:500; } .mark { width:12px; height:12px; border:1px solid var(--ink); transform:rotate(45deg); }
    .tabs { display:flex; height:var(--control-h); } .tabs a { display:flex; align-items:center; padding:0 18px; border-right:1px solid var(--hair); } .tabs a.active { background:var(--ink); color:var(--bg); }
    .spacer { flex:1 1 auto; min-width:12px; } .kpis { display:flex; min-height:var(--control-h); flex:0 1 auto; overflow-x:auto; max-width:100%; } .kpis span { display:flex; align-items:center; gap:6px; padding:0 10px; border-left:1px solid var(--hair); color:var(--muted); white-space:nowrap; } .kpis b { color:var(--ink); font-variant-numeric:tabular-nums; } .refresh { width:var(--control-h); height:var(--control-h); flex:0 0 var(--control-h); display:grid; place-items:center; border-left:1px solid var(--ink); }
    .picker { min-height:64px; display:flex; flex-wrap:wrap; align-items:stretch; border-bottom:1px solid var(--ink); background:var(--surface); } .picker-eye { flex:0 0 160px; min-height:64px; padding:9px 14px; background:var(--paper); border-right:1px solid var(--ink); } .picker-eye strong { font:600 var(--type-heading)/1 var(--bs-font-sans); margin-right:5px; }
    .mob-tab { flex:1 1 240px; min-width:220px; min-height:64px; display:flex; align-items:center; gap:14px; padding:8px 16px; border-right:1px solid var(--hair); border-top:3px solid transparent; color:var(--muted); } .mob-tab.focused { background:var(--bg); border-top-color:var(--accent); color:var(--ink); border-bottom:1px solid var(--bg); } .radio { width:18px; height:18px; flex:0 0 18px; border:1px solid var(--hair); border-radius:50%; background:var(--bg); position:relative; } .focused .radio { border-color:var(--accent); background:var(--accent); box-shadow:0 0 0 4px color-mix(in oklch, var(--accent) 18%, transparent); } .focused .radio::after { content:""; position:absolute; width:5px; height:5px; border-radius:50%; background:white; left:50%; top:50%; transform:translate(-50%, -50%); }
    .mob-tab-main { min-width:0; flex:1; } .mob-tab-title { font-size:var(--type-card); font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .badge { border:1px solid var(--accent); color:var(--accent); padding:1px 5px; background:color-mix(in oklch, var(--accent) 8%, transparent); }
    .focus-other { flex:0 0 170px; min-height:64px; display:grid; place-items:center; border-left:1px solid var(--ink); color:var(--ink); }
    .hero { min-height:520px; display:grid; grid-template-rows:auto auto minmax(360px,1fr); border-bottom:1px solid var(--ink); } .identity { min-height:42px; display:flex; align-items:center; gap:12px; flex-wrap:wrap; padding:8px 14px; border-bottom:1px solid var(--hair); } .identity h1 { margin:0; font-size:var(--type-heading); font-weight:500; flex:1 1 260px; min-width:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .identity .eyebrow, .identity .meta { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .chip { display:inline-flex; border:1px solid color-mix(in oklch, var(--accent) 35%, var(--hair)); color:var(--accent); padding:1px 6px; text-transform:none; } .chip.template { color:var(--muted); border-color:var(--hair); }
    .timeline { display:grid; grid-template-columns:repeat(4,minmax(180px,1fr)); background:var(--surface); border-bottom:1px solid var(--hair); overflow-x:auto; } .phase { min-width:0; min-height:72px; display:flex; align-items:center; gap:10px; padding:10px 18px; border-right:1px solid var(--hair); } .phase span:last-child { min-width:0; overflow:hidden; text-overflow:ellipsis; } .disc { width:22px; height:22px; flex:0 0 22px; border:1px solid var(--hair); display:grid; place-items:center; border-radius:50%; font-family:var(--bs-font-mono); font-size:var(--type-rail); } .phase.done .disc { background:var(--ok); color:white; border-color:var(--ok); } .phase.active { background:color-mix(in oklch, var(--accent) 8%, transparent); color:var(--accent); } .phase.active .disc { background:var(--accent); color:white; border-color:var(--accent); }
    .hero-body { min-height:0; display:grid; grid-template-columns:minmax(420px,1fr) minmax(280px,340px); align-items:stretch; } .flow { border-right:1px solid var(--hair); padding:24px; min-width:0; overflow:auto; } svg { width:100%; height:100%; min-width:520px; min-height:260px; } .agent { min-width:0; display:grid; grid-template-rows:minmax(0,1fr) minmax(0,1fr); } .agent-card { min-width:0; padding:18px; border-bottom:1px solid var(--hair); overflow:auto; } .agent-card h2 { margin:12px 0 4px; font-size:var(--type-heading); line-height:1.25; } .stats { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); border-top:1px solid var(--hair); border-bottom:1px solid var(--hair); margin:16px 0; } .stats div { min-width:0; padding:8px; border-right:1px solid var(--hair); overflow:hidden; text-overflow:ellipsis; } .file { display:flex; justify-content:space-between; gap:10px; padding:7px 0; border-bottom:1px solid var(--hair); } .file span { min-width:0; overflow:hidden; text-overflow:ellipsis; }
    .lower { min-height:220px; display:grid; grid-template-columns:minmax(0,1.4fr) minmax(280px,1fr); border-top:1px solid var(--ink); } .activity, .lanes { min-width:0; padding:14px 18px; overflow:auto; } .activity { border-right:1px solid var(--hair); } .log-row { display:grid; grid-template-columns:minmax(64px,74px) minmax(60px,72px) minmax(0,1fr); gap:12px; padding:8px 0; border-bottom:1px solid var(--hair); font-size:var(--type-body); line-height:var(--line-compact); } .log-row span { min-width:0; overflow:hidden; text-overflow:ellipsis; } .bar { display:grid; grid-template-columns:repeat(8,minmax(72px,1fr)); min-height:32px; overflow-x:auto; border:1px solid var(--ink); margin:12px 0; } .bar span { display:grid; place-items:center; min-width:0; padding:0 6px; color:white; background:var(--accent); border-right:1px solid var(--bg); text-align:center; } .lane-row { display:grid; grid-template-columns:28px minmax(0,1fr) minmax(70px,88px) auto; gap:8px; padding:5px 0; border-bottom:1px solid var(--hair); } .lane-row span { min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .error { color:var(--crit); padding:8px 14px; margin:0; border-bottom:1px solid var(--crit); }
    @media (max-height: 720px), (max-width: 980px) { body { min-height:100vh; overflow:auto; } .top-rail { flex-wrap:wrap; align-items:stretch; } .brand, .tabs, .refresh { flex:0 0 auto; } .kpis { order:2; flex:1 0 100%; border-top:1px solid var(--hair); } .picker { min-width:0; height:auto; } .picker-eye { flex:0 0 160px; } .mob-tab { flex:1 1 280px; } .hero { height:auto; min-height:0; grid-template-rows:auto auto auto; } .timeline { grid-template-columns:repeat(2,minmax(220px,1fr)); } .hero-body { grid-template-columns:1fr; } .flow { min-height:320px; border-right:0; border-bottom:1px solid var(--hair); } .agent { grid-template-columns:1fr 1fr; grid-template-rows:auto; } .lower { height:auto; min-height:220px; grid-template-columns:1fr; } .activity { border-right:0; border-bottom:1px solid var(--hair); } }
    @media (max-width: 720px) { .top-rail { position:sticky; top:0; } .tabs { overflow-x:auto; max-width:100%; } .picker { min-width:0; height:auto; flex-direction:column; } .picker-eye, .focus-other { width:100%; flex-basis:auto; min-height:48px; border-right:0; border-left:0; } .mob-tab { min-width:0; min-height:64px; } .timeline { grid-template-columns:1fr; } .phase { min-height:64px; } .agent { grid-template-columns:1fr; } .stats { grid-template-columns:1fr; } .log-row { grid-template-columns:64px minmax(0,1fr); } .log-row span:last-child { grid-column:1 / -1; } }
  </style>
</head>
<body data-bs-theme="paper">
  <div class="top-rail"><a class="brand" href="/overview"><span class="mark"></span><span>MEERKAT FUGUE</span></a><nav class="tabs"><a class="active" href="/overview">OVERVIEW</a><a href="/issues">IMS</a><a href="/console">CONSOLE</a><a href="/ensemble">ENSEMBLE</a></nav><div class="spacer"></div><div class="kpis"><span>RUN <b>__RUNNING__</b></span><span>RETRY <b>__RETRYING__</b></span><span>TOK <b>__TOKENS__</b></span><span>T <b>__SECONDS__s</b></span><span>SHAD <b>__SHADOW__r</b></span><span>CON <b>__CONSOLE_HINT__</b></span></div><a class="refresh" href="/overview">↻</a></div>
  __ERROR__
  <section class="picker"><div class="picker-eye"><div class="eyebrow">FOCUS MOB</div><strong>__IN_FLIGHT_COUNT__</strong><span class="meta">in flight</span></div>__PICKER_TABS__<a class="focus-other mono" href="/issues">◇ FOCUS OTHER</a></section>

  <main class="hero"><section class="identity"><span class="eyebrow">__FOCUS_IDENTIFIER__ · __FOCUS_LANE__</span><h1>__FOCUS_TITLE__</h1><span class="chip">__FOCUS_PHASE__</span><span class="chip template">fugue:template:planner-coder-reviewer</span><span class="meta">RUNTIME · __FOCUS_RUNTIME__</span><a class="meta" href="/issues">OPEN IMS</a><a class="meta" href="/console">CONSOLE</a></section><section class="timeline"><div class="phase done"><span class="disc">✓</span><span><span class="chip">fugue:phase:planning</span><br><span class="meta">done when runtime reports it</span></span></div><div class="phase active"><span class="disc">2</span><span><span class="chip">__FOCUS_PHASE__</span><br><span class="meta">active when mob is running</span></span></div><div class="phase"><span class="disc">3</span><span><span class="chip">fugue:phase:reviewing</span><br><span class="meta">pending</span></span></div><div class="phase"><span class="disc">4</span><span><span class="chip">fugue:phase:finalizing</span><br><span class="meta">pending</span></span></div></section><section class="hero-body"><div class="flow"><svg viewBox="0 0 760 300" preserveAspectRatio="xMidYMid meet" role="img" aria-label="actual Fugue mob topology: issue lead hub, planner coder reviewer worker mesh, and repo handoff"><defs><marker id="arr" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#D95A1E"/></marker><marker id="arr-ok" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#3E6B4F"/></marker></defs><g font-family="JetBrains Mono,monospace" font-size="11"><rect x="24" y="34" width="148" height="58" fill="#ECEAE4" stroke="#A8A49B"/><text x="38" y="58" fill="#6B6760">REPO MOB</text><text x="38" y="80" font-family="Inter,sans-serif" font-size="14" font-weight="600" fill="#1C1A17">Kapellmeister</text><rect x="296" y="24" width="168" height="72" fill="#F5E6DF" stroke="#D95A1E"/><text x="312" y="50" fill="#D95A1E">ISSUE COORDINATOR</text><text x="312" y="75" font-family="Inter,sans-serif" font-size="16" font-weight="600" fill="#1C1A17">Issue lead</text><text x="312" y="90" fill="#6B6760">Linear lane + Workpad</text><rect x="72" y="174" width="148" height="70" fill="#F6F4EE" stroke="#3E6B4F"/><text x="88" y="198" fill="#3E6B4F">PLANNER</text><text x="88" y="222" font-family="Inter,sans-serif" font-size="14" font-weight="600" fill="#1C1A17">Plan</text><rect x="306" y="174" width="148" height="70" fill="#F5E6DF" stroke="#D95A1E"/><text x="322" y="198" fill="#D95A1E">CODER</text><text x="322" y="222" font-family="Inter,sans-serif" font-size="14" font-weight="600" fill="#1C1A17">Implement</text><rect x="540" y="174" width="148" height="70" fill="#F6F4EE" stroke="#3E6B4F"/><text x="556" y="198" fill="#3E6B4F">REVIEWER</text><text x="556" y="222" font-family="Inter,sans-serif" font-size="14" font-weight="600" fill="#1C1A17">Gate</text><rect x="566" y="34" width="160" height="58" fill="#ECEAE4" stroke="#3E6B4F" stroke-dasharray="4 4"/><text x="582" y="58" fill="#3E6B4F">PR HANDOFF</text><text x="582" y="80" font-family="Inter,sans-serif" font-size="14" font-weight="600" fill="#1C1A17">Repo boundary</text><path d="M172 63 L296 60" stroke="#A8A49B" stroke-width="1.4" marker-end="url(#arr)"/><path d="M296 70 L172 72" stroke="#A8A49B" stroke-width="1.4" marker-end="url(#arr)"/><path d="M350 96 L168 174" stroke="#D95A1E" stroke-width="1.2" marker-end="url(#arr)"/><path d="M380 96 L380 174" stroke="#D95A1E" stroke-width="1.2" marker-end="url(#arr)"/><path d="M410 96 L592 174" stroke="#D95A1E" stroke-width="1.2" marker-end="url(#arr)"/><path d="M220 202 L306 202" stroke="#3E6B4F" stroke-width="1.4" marker-end="url(#arr-ok)"/><path d="M454 202 L540 202" stroke="#3E6B4F" stroke-width="1.4" marker-end="url(#arr-ok)"/><path d="M160 244 C250 286 510 286 600 244" stroke="#A8A49B" stroke-width="1.1" stroke-dasharray="4 4" fill="none" marker-end="url(#arr)"/><path d="M200 174 C285 130 475 130 560 174" stroke="#A8A49B" stroke-width="1.1" stroke-dasharray="4 4" fill="none" marker-end="url(#arr)"/><path d="M632 174 L646 92" stroke="#3E6B4F" stroke-width="1.4" stroke-dasharray="4 4" marker-end="url(#arr-ok)"/><text x="252" y="152" fill="#6B6760">issue lead wires every worker</text><text x="254" y="270" fill="#6B6760">worker peer mesh: planner↔coder · planner↔reviewer · coder↔reviewer</text><text x="70" y="116" fill="#6B6760">repo lead ↔ issue lead</text><text x="468" y="116" fill="#6B6760">review green → PR/check handoff</text></g></svg></div><aside class="agent"><div class="agent-card"><span class="eyebrow">ACTIVE AGENT <span class="badge">● RUNTIME</span></span><h2>__ACTIVE_ROLE__</h2><div class="meta">live state snapshot</div><div class="stats"><div><span class="meta">Runtime</span><br>__FOCUS_RUNTIME__</div><div><span class="meta">Tokens</span><br>__TOKENS__</div><div><span class="meta">Turn</span><br>runtime</div></div><div class="meta">CURRENT ACTION</div><p>→ __CURRENT_ACTION__</p><p class="meta">No live topology/workpad diff stream is exposed yet; showing runtime snapshot evidence only.</p></div><div class="agent-card"><span class="eyebrow">WORKPAD · RUNTIME EVIDENCE</span>__WORKPAD_FILES__</div></aside></section></main>

  <section class="lower"><div class="activity"><div class="eyebrow">RUNTIME · LAST EVENTS · TAIL -f</div>__ACTIVITY_ROWS__</div><div class="lanes"><div class="eyebrow">8 LANES · ISSUES</div><div class="bar"><span>Backlog</span><span>Queue</span><span>Active</span><span>Review</span><span>Deploy</span><span>Blocked</span><span>Done</span><span>Canceled</span></div><div class="lane-row"><span>◇</span><span>Review</span><span class="meta">▸ ACTION</span><span class="meta">see IMS</span></div><div class="lane-row"><span>↑</span><span>Deploy</span><span class="meta">▸ ACTION</span><span class="meta">see IMS</span></div><div class="lane-row"><span>≋</span><span>Ensemble</span><span class="meta">▸ LOCAL</span><a class="meta" href="/ensemble">open teams</a></div></div></section>
</body>
</html>"##;
    html.replace("__RUNNING__", &view.running.to_string())
        .replace("__RETRYING__", &view.retrying.to_string())
        .replace("__TOKENS__", &view.tokens.to_string())
        .replace("__SECONDS__", &format!("{:.1}", view.seconds))
        .replace("__SHADOW__", &view.shadow_recommendations.to_string())
        .replace("__ERROR__", &error)
        .replace("__IN_FLIGHT_COUNT__", &view.in_flight_count.to_string())
        .replace("__PICKER_TABS__", &view.picker_tabs)
        .replace("__FOCUS_IDENTIFIER__", &html_escape(&view.focus_identifier))
        .replace("__FOCUS_TITLE__", &html_escape(&view.focus_title))
        .replace(
            "__FOCUS_LANE__",
            &html_escape(&view.focus_lane.to_ascii_uppercase()),
        )
        .replace("__FOCUS_RUNTIME__", &html_escape(&view.focus_runtime))
        .replace(
            "__FOCUS_PHASE__",
            &html_escape(&phase_label_for_runtime_state(&view.focus_lane)),
        )
        .replace("__ACTIVE_ROLE__", &html_escape(&view.active_role))
        .replace("__CURRENT_ACTION__", &html_escape(&view.current_action))
        .replace("__WORKPAD_FILES__", &view.workpad_files)
        .replace("__ACTIVITY_ROWS__", &view.activity_rows)
        .replace("__CONSOLE_HINT__", console_hint)
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
        Some(
            "Write the issue in Markdown. Attachments are submitted to the create API for explicit backend handling.",
        ),
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
            <span>Attachments upload after issue creation and are persisted as issue attachment comments.</span>
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
    ISSUES_BOARD_HTML.replace(
        "__FUGUE_CREATE_EDITOR__",
        &create_issue_editor_html(&lane_options),
    )
}

async fn issue_detail_page(
    State(app): State<AppState>,
    headers: HeaderMap,
    AxumPath(_issue_identifier): AxumPath<String>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    Html(ISSUE_DETAIL_HTML).into_response()
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
  <title>Fugue IMS</title>
  <style>
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
    <section class="lane-ribbon" id="laneRibbon" aria-label="Lane filter ribbon"></section>
    <section class="toolbar sub-toolbar">
      <div class="summary" id="summary"><span><span class="metric">0</span> shown</span></div>
      <div class="toolbar-actions"><button class="active" type="button">VIEW: BOARD</button><button type="button">VIEW: LIST</button><label class="command" for="search"><span class="kbd">/</span><input id="search" autocomplete="off" placeholder="to search…"></label><button id="newIssue" type="button">+ NEW</button><button class="icon ghost" id="shortcutButton" type="button" title="Shortcuts" aria-label="Shortcuts">?</button><span class="updated" id="selectionHint"></span></div>
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
      const response = await fetch(url, {
        ...options,
        headers: {
          "Accept": "application/json",
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


    const isActionLane = (name) => name === "Review" || name === "Deploy";
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
      const turns = issue.runtime?.kind === "running" ? `${escapeHtml(issue.runtime.turn_count)}t` : "turns unavailable";
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
      if (!issues.length && !action) {
        return `<section class="lane empty-strip" data-lane="${escapeHtml(lane.name)}" data-kind="${escapeHtml(lane.kind)}"><div class="lane-head">${laneGlyph(lane.name)}</div><div class="rotated">${escapeHtml(lane.name)} · 0</div></section>`;
      }
      const cards = issues.length ? issues.map(renderIssue).join("") : '<div class="empty action-drop">empty · action drop target</div>';
      const hint = action ? `<div class="lane-action-hint">▸ Drop to run ${lane.name.toLowerCase()} procedure</div>` : "";
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
      window.location.href = `/issues/${encodeURIComponent(identifier)}`;
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
      const comments = detail.comments.length
        ? detail.comments.map(renderComment).join("")
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
              ${issue.url ? `<a class="pill" href="${escapeHtml(issue.url)}">Open Linear</a>` : ""}
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
            <div class="section-head"><h3>Workpad</h3></div>
            <textarea id="workpadEditor" class="issue-markdown-editor" spellcheck="false">${escapeHtml(workpad)}</textarea>
            <div class="editor-actions">
              <button id="saveWorkpad" class="primary" type="button">Save Workpad</button>
            </div>
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

    const renderCreateAttachments = () => renderAttachmentList(createAttachmentList, createAttachmentState, "No attachments selected.");
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
      const response = await fetch(`/api/v1/issues/${encodeURIComponent(identifier)}/attachments`, {
        method: "POST",
        headers: { "Accept": "application/json" },
        body: form,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error?.message || `Attachment upload failed: ${response.status}`);
      }
      const message = `Attached ${payload.attachments?.length || attachments.length} file(s).`;
      if (errorTarget) errorTarget.textContent = message;
      showNotice(message);
      return payload;
    };

    const resetCreateForm = () => {
      createForm.reset();
      createAttachmentState.length = 0;
      renderCreateAttachments();
    };

    const createIssue = async () => {
      const title = createTitle.value.trim();
      const lane = createLane.value;
      createError.textContent = "";
      if (!title) {
        createError.textContent = "Title is required.";
        createTitle.focus();
        return;
      }
      createSubmit.disabled = true;
      try {
        const payload = await api("/api/v1/issues", {
          method: "POST",
          body: JSON.stringify({
            title,
            description: createDescription.value,
            lane,
            attachments: createAttachmentPayload(),
          }),
        });
        const createdIdentifier = payload.issue?.identifier;
        if (createdIdentifier && createAttachmentState.length) {
          await uploadIssueAttachments(createdIdentifier, createAttachmentState, createError);
        }
        resetCreateForm();
        createPanel.classList.remove("open");
        if (createdIdentifier) {
          app.selected = createdIdentifier;
        }
        await loadBoard();
        showNotice(`Created ${createdIdentifier || "issue"}.`);
      } catch (error) {
        createError.textContent = error.message;
        showNotice(error.message);
      } finally {
        createSubmit.disabled = false;
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
      createError.textContent = "";
      createPanel.classList.toggle("open");
      if (createPanel.classList.contains("open")) createTitle.focus();
    });
    createCancel.addEventListener("click", () => {
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
    <header class="sheet-head"><div class="state-chip" id="stateChip">◐ RUNNING</div><div class="issue-id" id="issueId">ISSUE</div><div class="sheet-title" id="title">loading…</div><div class="dirty" id="dirtyFlag" hidden>● UNSAVED · 1 CHANGE</div><button class="close" id="close" type="button">×</button></header>
    <section class="body"><aside class="props" id="props"></aside><section class="middle" id="middle"><div class="meta">loading…</div></section><aside class="lanes" id="lanePicker"></aside></section>
    <footer class="footer"><div class="diff" id="diff">No changes</div><button class="cancel" id="cancel" type="button">CANCEL <span class="meta">ESC</span></button><button class="save" id="save" type="button" disabled>SAVE CHANGES <span class="meta">⌘S</span></button></footer>
  </main>
<script>
  const issueIdentifier = decodeURIComponent(window.location.pathname.split("/").pop());
  const $ = (id) => document.getElementById(id);
  let detail = null; let targetLane = null; const issueAttachmentState = []; const commentAttachmentState = []; const maxAttachmentCount = 20; const maxAttachmentBytes = 25 * 1024 * 1024; const maxAttachmentTotalBytes = 100 * 1024 * 1024;
  const escapeHtml = (value) => String(value ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");
  const newtype = (value) => value && typeof value === "object" && "0" in value ? value["0"] : value;
  const api = async (url, options = {}) => { const response = await fetch(url, { ...options, headers: { "Accept":"application/json", ...(options.body ? {"Content-Type":"application/json"} : {}), ...(options.headers || {}) }}); const payload = await response.json().catch(() => ({})); if (!response.ok) throw new Error(payload?.error?.message || `Request failed: ${response.status}`); return payload; };
  const showNotice = (message) => { $("notice").textContent = message; $("notice").style.display = "block"; };
  const clearNotice = () => { $("notice").textContent = ""; $("notice").style.display = "none"; };
  const laneGlyph = (name) => ({Backlog:"○", Queue:"◉", Active:"◐", Review:"◇", Deploy:"↑", Blocked:"!", Done:"✓", Canceled:"×"}[name] || "○");
  const isAction = (lane) => lane === "Review" || lane === "Deploy";
  const renderKpis = () => { const runtime = detail.runtime; $("kpiTime").textContent = "unavailable"; $("kpiShadow").textContent = "unavailable"; if (!runtime) { $("kpiRun").textContent = "unavailable"; $("kpiRetry").textContent = "unavailable"; $("kpiTok").textContent = "unavailable"; return; } if (runtime.kind === "running") { $("kpiRun").textContent = "1"; $("kpiRetry").textContent = "0"; $("kpiTok").textContent = String(runtime.token_total ?? "unavailable"); return; } if (runtime.kind === "retrying") { $("kpiRun").textContent = "0"; $("kpiRetry").textContent = String(runtime.attempt ?? "1"); $("kpiTok").textContent = "unavailable"; return; } $("kpiRun").textContent = "unavailable"; $("kpiRetry").textContent = "unavailable"; $("kpiTok").textContent = "unavailable"; };
  const formatFileSize = (bytes) => bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  const attachmentCommentId = (attachment) => newtype(attachment?.source?.Comment?.comment_id ?? attachment?.source?.comment?.comment_id ?? attachment?.source?.comment_id ?? null);
  const persistedCommentAttachments = (commentId) => (detail.attachments || []).filter((attachment) => attachmentCommentId(attachment) === commentId);
  const renderPersistedAttachmentChip = (attachment) => { const label = attachment.title || attachment.url || "attachment"; const size = attachment.byte_len ? ` · ${escapeHtml(formatFileSize(attachment.byte_len))}` : ""; return `<a class="attachment-chip" href="${escapeHtml(attachment.url)}" target="_blank" rel="noopener">${escapeHtml(label)}${size}</a>`; };
  const renderComment = (comment) => { const attachments = persistedCommentAttachments(newtype(comment.comment_id)); const chips = attachments.length ? `<div class="attachment-list comment-attachments" aria-label="Comment attachments">${attachments.map(renderPersistedAttachmentChip).join("")}</div>` : ""; return `<article class="comment"><span class="meta">${escapeHtml(newtype(comment.comment_id))}</span><br>${escapeHtml(comment.body_markdown)}${chips}</article>`; };
  const dataTransferHasFiles = (event) => Array.from(event.dataTransfer?.types || []).includes("Files");
  const renderIssueAttachments = () => { const list = $("issueAttachmentList"); if (!list) return; const persistedHtml = (detail.attachments || []).map(renderPersistedAttachmentChip).join(""); const pendingHtml = issueAttachmentState.length ? issueAttachmentState.map((attachment) => `<span class="attachment-chip">${escapeHtml(attachment.name)} · ${escapeHtml(formatFileSize(attachment.size))}</span>`).join("") : ""; list.innerHTML = persistedHtml || pendingHtml ? `${persistedHtml}${pendingHtml}` : '<span class="meta">No attachments added for this issue editor.</span>'; };
  const validateIssueAttachment = (file, state = issueAttachmentState) => { if (state.length >= maxAttachmentCount) throw new Error("At most 20 attachments can be added."); if (file.size > maxAttachmentBytes) throw new Error(`${file.name} exceeds the 25 MiB per-file attachment limit.`); const nextTotal = state.reduce((sum, attachment) => sum + attachment.size, 0) + file.size; if (nextTotal > maxAttachmentTotalBytes) throw new Error("Attachments exceed the 100 MiB total attachment limit."); return { name:file.name, size:file.size, content_type:file.type || null, file }; };
  const uploadIssueAttachments = async (attachments) => { const form = new FormData(); for (const attachment of attachments) form.append("attachments", attachment.file, attachment.name); const response = await fetch(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/attachments`, { method:"POST", headers:{"Accept":"application/json"}, body:form }); const payload = await response.json().catch(() => ({})); if (!response.ok) throw new Error(payload?.error?.message || `Attachment upload failed: ${response.status}`); return payload; };
  const renderCommentAttachments = () => { const list = $("commentAttachmentList"); if (!list) return; list.innerHTML = commentAttachmentState.length ? commentAttachmentState.map((attachment, index) => `<span class="attachment-chip">${escapeHtml(attachment.name)} · ${escapeHtml(formatFileSize(attachment.size))} <button type="button" data-comment-attachment-remove="${index}" aria-label="Remove ${escapeHtml(attachment.name)}">×</button></span>`).join("") : '<span class="meta">No files selected for this comment.</span>'; };
  const installCommentAttachmentInput = () => { const input = $("commentAttachmentInput"); if (!input || input.dataset.attachmentReady === "true") return; input.dataset.attachmentReady = "true"; input.addEventListener("change", () => { const error = $("commentError"); try { for (const file of Array.from(input.files || [])) commentAttachmentState.push(validateIssueAttachment(file, commentAttachmentState)); renderCommentAttachments(); } catch (caught) { if (error) error.textContent = caught.message; showNotice(caught.message); } finally { input.value = ""; } }); };
  const installAttachmentDropTarget = () => { const zone = $("issueAttachmentDropZone"); if (!zone || zone.dataset.dropReady === "true") return; zone.dataset.dropReady = "true"; const error = $("issueAttachmentError"); const setActive = (active) => zone.classList.toggle("drag-over", active); for (const type of ["dragenter", "dragover"]) zone.addEventListener(type, (event) => { if (!dataTransferHasFiles(event)) return; event.preventDefault(); event.stopPropagation(); event.dataTransfer.dropEffect = "copy"; setActive(true); }); zone.addEventListener("dragleave", (event) => { if (!zone.contains(event.relatedTarget)) setActive(false); }); const addFiles = async (files) => { error.textContent = ""; const added = []; for (const file of Array.from(files || [])) { const attachment = validateIssueAttachment(file); issueAttachmentState.push(attachment); added.push(attachment); } renderIssueAttachments(); if (added.length) { const payload = await uploadIssueAttachments(added); error.textContent = `Attached ${payload.attachments?.length || added.length} file(s).`; showNotice(error.textContent); await load(true); } }; zone.addEventListener("drop", async (event) => { if (!dataTransferHasFiles(event)) return; event.preventDefault(); event.stopPropagation(); setActive(false); try { await addFiles(event.dataTransfer.files); } catch (caught) { error.textContent = caught.message; showNotice(caught.message); } }); const input = $("issueAttachmentInput"); input?.addEventListener("change", async () => { try { await addFiles(input.files); } catch (caught) { error.textContent = caught.message; showNotice(caught.message); } finally { input.value = ""; } }); };
  document.addEventListener("dragover", (event) => { if (dataTransferHasFiles(event)) event.preventDefault(); }, true); document.addEventListener("drop", (event) => { if (dataTransferHasFiles(event)) event.preventDefault(); }, true);
  const render = () => { renderKpis(); const issue = detail.issue; const id = newtype(issue.identifier); const current = issue.state.name; targetLane = targetLane || current; $("issueId").textContent = id; $("title").textContent = issue.title; $("stateChip").textContent = `${laneGlyph(current)} ${current.toUpperCase()}`; const dirty = targetLane !== current; $("dirtyFlag").hidden = !dirty; $("save").disabled = !dirty; $("save").innerHTML = dirty && isAction(targetLane) ? `SAVE & RUN ${targetLane.toUpperCase()} <span class="meta">⌘S</span>` : `SAVE CHANGES <span class="meta">⌘S</span>`; $("diff").innerHTML = dirty ? `● 1 UNSAVED CHANGE&nbsp;&nbsp; Lane: ${escapeHtml(current)} → ${escapeHtml(targetLane)} ${isAction(targetLane) ? `<span class="trigger">▸ triggers ${targetLane.toLowerCase()} procedure</span>` : ""}` : "No changes"; $("props").innerHTML = `<div class="field ${dirty ? "dirty" : ""}"><div class="field-head"><span>LANE</span>${dirty ? "<span>● CHANGED</span>" : ""}</div><div class="value">${dirty ? `<del>${escapeHtml(current)}</del> → <strong>${escapeHtml(targetLane)}</strong>` : escapeHtml(current)}</div></div><div class="field"><div class="field-head"><span>LABELS</span></div><div>${issue.labels.map((l) => `<span class="label-chip">${escapeHtml(l)}</span>`).join("")}</div></div><div class="field"><div class="field-head"><span>ASSIGNEE</span></div><div>${escapeHtml(issue.assignee?.name || "Unassigned")}</div></div><div class="field"><div class="field-head"><span>BRANCH</span></div><div>${escapeHtml(newtype(issue.branch_name) || "None")}</div></div>`; $("middle").innerHTML = `<h2 class="section-title mono">DESCRIPTION</h2><div class="description">${escapeHtml(issue.description || "No description")}</div><h2 class="section-title mono">ACTIVITY</h2>${detail.comments.map(renderComment).join("") || '<div class="meta">No comments</div>'}<h2 class="section-title mono">ATTACHMENTS</h2><section class="attachment-drop-zone" id="issueAttachmentDropZone" data-attachment-drop-zone aria-label="Drop files to attach them to this issue"><label class="attachment-input-label" for="issueAttachmentInput"><span>Choose files</span><input id="issueAttachmentInput" type="file" multiple></label><div class="attachment-drop-affordance" aria-hidden="true">Drop files here to attach them to this issue</div><div class="attachment-list" id="issueAttachmentList" aria-live="polite"></div><div class="attachment-error meta" id="issueAttachmentError" role="alert"></div></section><h2 class="section-title mono">NEW COMMENT</h2><section class="comment-composer" aria-label="New comment"><textarea id="commentEditor" class="issue-markdown-editor" spellcheck="true" placeholder="Add a comment…"></textarea><label class="attachment-input-label" for="commentAttachmentInput"><span>Attach files to this comment</span><input id="commentAttachmentInput" type="file" multiple></label><div class="attachment-list" id="commentAttachmentList" aria-live="polite"></div><div class="comment-actions"><div class="comment-error meta" id="commentError" role="alert"></div><button class="comment-submit" id="postComment" type="button">POST COMMENT <span class="meta">C</span></button></div></section><h2 class="section-title mono">WORKPAD</h2><textarea id="workpadEditor" class="issue-markdown-editor">${escapeHtml(detail.workpad?.body_markdown || "")}</textarea>`; $("lanePicker").innerHTML = detail.lanes.map((lane) => `<button class="lane-row ${targetLane === lane ? "target" : ""}" data-lane="${escapeHtml(lane)}" type="button"><span>${laneGlyph(lane)}</span><span>${escapeHtml(lane)}${isAction(lane) ? `<span class="sub">▸ runs ${lane.toLowerCase()} procedure</span>` : ""}</span><span class="meta">${lane === current ? "FROM" : targetLane === lane ? "NEW LANE" : ""}</span></button>`).join(""); renderIssueAttachments(); renderCommentAttachments(); installAttachmentDropTarget(); installCommentAttachmentInput(); };
  const load = async (preserveTarget = false) => { const previousTarget = targetLane; try { detail = await api(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}`); targetLane = preserveTarget && previousTarget ? previousTarget : detail.issue.state.name; render(); } catch (error) { showNotice(error.message); } };
  const postComment = async () => { const editor = $("commentEditor"); const button = $("postComment"); const error = $("commentError"); if (!editor || !button || !error) return; const body = editor.value.trim(); clearNotice(); error.textContent = ""; if (!body && commentAttachmentState.length === 0) { error.textContent = "Comment or attachment is required."; editor.focus(); return; } button.disabled = true; try { if (commentAttachmentState.length) { const form = new FormData(); form.append("body_markdown", body); for (const attachment of commentAttachmentState) form.append("attachments", attachment.file, attachment.name); const response = await fetch(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/comments`, { method:"POST", headers:{"Accept":"application/json"}, body:form }); const payload = await response.json().catch(() => ({})); if (!response.ok) throw new Error(payload?.error?.message || `Comment failed: ${response.status}`); commentAttachmentState.length = 0; renderCommentAttachments(); } else { await api(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/comments`, { method: "POST", body: JSON.stringify({ body_markdown: body }) }); } editor.value = ""; await load(true); } catch (caught) { error.textContent = caught.message; showNotice(caught.message); } finally { button.disabled = false; } };
  document.addEventListener("click", async (event) => { if (event.target.id === "close" || event.target.id === "cancel" || event.target === document.body) location.href = "/issues"; const row = event.target.closest(".lane-row"); if (row) { targetLane = row.dataset.lane; render(); } if (event.target.dataset.commentAttachmentRemove) { commentAttachmentState.splice(Number(event.target.dataset.commentAttachmentRemove), 1); renderCommentAttachments(); } if (event.target.id === "postComment") { await postComment(); } if (event.target.id === "save" && detail && targetLane !== detail.issue.state.name) { try { await api(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/state`, { method:"PATCH", body: JSON.stringify({ state: targetLane }) }); location.href = "/issues"; } catch (error) { showNotice(error.message); } } });
  document.addEventListener("keydown", async (event) => { const target = event.target; const editing = target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement; if (event.key === "Escape") location.href = "/issues"; if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "s") { event.preventDefault(); $("save").click(); } else if (!editing && event.key.toLowerCase() === "c") { event.preventDefault(); $("commentEditor")?.focus(); } });
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
}

async fn api_issue_create(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IssueCreateRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
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
        Ok(issue) => Json(
            json!({ "issue": issue_board_issue(issue, "Queue", &HashMap::new(), &HashMap::new()) }),
        )
        .into_response(),
        Err(error) => issue_adapter_error_response(error),
    }
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
        .move_issue(MoveIssueInput {
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
}

#[derive(Debug, Serialize)]
struct IssueUploadedAttachment {
    name: String,
    size: u64,
    content_type: Option<String>,
    url: String,
    #[serde(skip_serializing)]
    path: Option<String>,
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
    let mut uploaded = Vec::new();
    let upload_dir = issue_attachment_dir(&app.config, &issue_identifier);
    if let Err(error) = tokio::fs::create_dir_all(&upload_dir).await {
        return server_error(
            "attachment_upload_failed",
            &format!("failed to prepare attachment storage: {error}"),
        );
    }
    while let Ok(Some(field)) = multipart.next_field().await {
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
        });
    }
    if uploaded.is_empty() {
        return bad_request(
            "missing_attachment",
            "at least one attachment file is required",
        );
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
    let adapter = match IssueAdapter::from_config(&app.config.tracker) {
        Ok(adapter) => adapter,
        Err(error) => return linear_config_error_response(error),
    };
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
    let path = issue_attachment_dir(&app.config, &issue_identifier).join(&file_name);
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

fn issue_attachment_dir(config: &ServiceConfig, issue_identifier: &str) -> PathBuf {
    config
        .workflow_dir
        .join(".fugue")
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
    while let Ok(Some(field)) = multipart.next_field().await {
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
        attachments: uploaded
            .into_iter()
            .map(|attachment| LinearCommentAttachmentInput {
                title: Some(attachment.name),
                url: attachment.url,
                path: attachment.path,
                content_type: attachment.content_type,
                byte_len: attachment.size,
            })
            .collect(),
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
    let parsed = match parse_issue_comment_request(&app, &issue_identifier, &headers, request).await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };
    if parsed.body_markdown.trim().is_empty() && parsed.attachments.is_empty() {
        return bad_request(
            "missing_comment",
            "body_markdown or at least one attachment must be provided",
        );
    }
    let adapter = match IssueAdapter::from_config(&app.config.tracker) {
        Ok(adapter) => adapter,
        Err(error) => return linear_config_error_response(error),
    };
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
    // rework-note: the adapter may still project Linear/Todo/Planning-style states; the UI exposes only Fugue canonical lanes and keeps phase information on labels.
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
        lanes: canonical_issue_board_lanes(),
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

    let lanes = canonical_issue_board_lanes();
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
                kind: canonical_lane_kind(&name),
                name,
                issues,
            }
        })
        .collect::<Vec<_>>();
    // rework-note: the IMS board always renders the eight canonical Fugue lanes; empty non-action lanes collapse in the client instead of disappearing into a sidebar.
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

fn issue_creation_lanes(_config: &ServiceConfig) -> Vec<String> {
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
        let lane = "Active".to_string();
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
    let normalized = state.trim().to_ascii_lowercase();
    if state.eq_ignore_ascii_case(&config.linear.lanes.parked)
        || normalized == "backlog"
        || normalized == "icebox"
    {
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
        return if normalized == "done" {
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
            .review_ready
            .clone()
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

async fn api_kapellmeister_complete_independent_review(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IndependentReviewCompletionRequest>,
) -> axum::response::Response {
    if let Err(response) = authorize_console_browser(&app.config, &headers) {
        return response;
    }
    let issue_identifier =
        issue_identifier_from_ref_http(&request.issue).unwrap_or_else(|| "unknown".to_string());
    if !app.config.independent_review.enabled {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "result": {
                    "accepted": false,
                    "issue_identifier": issue_identifier,
                    "message": "independent review lane is disabled by configuration"
                }
            })),
        )
            .into_response();
    }
    if let Err(error) = request.workgraph_scope.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "result": {
                    "accepted": false,
                    "issue_identifier": issue_identifier,
                    "message": format!("invalid independent review WorkGraph scope: {error}")
                }
            })),
        )
            .into_response();
    }
    for verdict in &request.verdicts {
        if let Err(error) = verdict.validate() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "result": {
                        "accepted": false,
                        "issue_identifier": issue_identifier,
                        "message": format!("invalid independent review verdict: {error}")
                    }
                })),
            )
                .into_response();
        }
    }
    let plan = aggregate_independent_review(IndependentReviewAggregationInput {
        config: app.config.independent_review.clone(),
        issue_identifier: issue_identifier.clone(),
        head_sha: request.head_sha.clone(),
        verdicts: request.verdicts.clone(),
        timed_out: request.timed_out,
    });
    if plan.decision == IndependentReviewDecision::Waiting {
        return (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {
                    "accepted": true,
                    "issue_identifier": issue_identifier,
                    "message": plan.reasons.join("; "),
                    "plan": plan
                }
            })),
        )
            .into_response();
    }
    match record_independent_review_projection(&app.config, &request, &issue_identifier, &plan)
        .await
    {
        Ok(result) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "result": {
                    "accepted": true,
                    "issue_identifier": issue_identifier,
                    "message": "independent review completion recorded",
                    "plan": plan,
                    "projection": result
                }
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "result": {
                    "accepted": false,
                    "issue_identifier": issue_identifier,
                    "message": error.to_string(),
                    "plan": plan
                }
            })),
        )
            .into_response(),
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
    let result = execute_deploy_transaction(app.config, request).await;
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
    match deploy_idempotency_already_recorded(&config, &request.issue, &request.idempotency_key)
        .await
    {
        Ok(true) => {
            return DeployTransactionResult {
                accepted: true,
                issue_identifier,
                message: "deploy idempotency key is already recorded; skipping duplicate merge/OTA attempt".to_string(),
                failure_category: None,
                plan: None,
                outcome: None,
                ota: None,
            };
        }
        Ok(false) => {}
        Err(error) => {
            return DeployTransactionResult {
                accepted: false,
                issue_identifier,
                message: error.to_string(),
                failure_category: Some(DeployFailureCategory::Unknown),
                plan: None,
                outcome: None,
                ota: None,
            };
        }
    }
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
    let merge_result = GitHubToolHost::new().merge_pr(merge_input);
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
    let ota_report = match run_ota_update(&ota_config).await {
        Ok(report) => report,
        Err(error) => {
            let outcome = deploy_failure_outcome(
                issue_identifier.clone(),
                &request,
                DeployFailureCategory::OtaFailed,
                Utc::now(),
                Utc::now(),
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
    adapter
        .upsert_workpad(UpsertWorkpadInput {
            issue: request.issue.clone(),
            heading: "## Fugue Workpad".to_string(),
            body_markdown: format!(
                "### Deployment\n\nIdempotency-Key: `{}`\n\n```json\n{evidence}\n```",
                request.idempotency_key.0
            ),
            create_if_missing: true,
            idempotency_key: IdempotencyKey(format!(
                "deploy-workpad:{}:{}",
                outcome.issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;
    adapter
        .move_issue(MoveIssueInput {
            issue: request.issue.clone(),
            state: LinearStateRef::Name {
                name: target_lane.to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey(format!(
                "{}:{}:{}",
                move_key_prefix, outcome.issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;
    Ok(())
}

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
        Ok(outcome) => outcome.status == DeployOutcomeStatus::Succeeded,
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

#[derive(Debug, Serialize)]
struct IndependentReviewProjectionResult {
    target_lane: String,
    workpad_comment_id: String,
    findings_comment_id: Option<String>,
    workgraph_items: Vec<IndependentReviewWorkGraphItemRef>,
}

#[derive(Debug, Serialize)]
struct IndependentReviewWorkGraphItemRef {
    id: String,
    realm_id: String,
    namespace: String,
}

async fn record_independent_review_projection(
    config: &ServiceConfig,
    request: &IndependentReviewCompletionRequest,
    issue_identifier: &str,
    plan: &crate::independent_review::IndependentReviewAggregationPlan,
) -> anyhow::Result<IndependentReviewProjectionResult> {
    let target_lane = plan
        .target_lane
        .clone()
        .ok_or_else(|| anyhow::anyhow!("independent review projection has no target lane"))?;
    let adapter = IssueAdapter::from_config(&config.tracker)?;
    let review_summary = serde_json::to_string_pretty(&json!({
        "schema_version": crate::independent_review::INDEPENDENT_REVIEW_SCHEMA_VERSION,
        "issue_identifier": issue_identifier,
        "pull_request": request.pull_request,
        "head_sha": request.head_sha,
        "decision": plan.decision,
        "target_lane": target_lane,
        "reasons": plan.reasons,
        "verdicts": request.verdicts,
        "timed_out": request.timed_out,
    }))?;
    let workpad = adapter
        .upsert_workpad(UpsertWorkpadInput {
            issue: request.issue.clone(),
            heading: "## Fugue Workpad".to_string(),
            body_markdown: format!("### Independent Review\n\n```json\n{review_summary}\n```"),
            create_if_missing: true,
            idempotency_key: IdempotencyKey(format!(
                "independent-review-workpad:{}:{}",
                issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;

    let workgraph_projection =
        create_independent_review_workgraph_items(config, &request.workgraph_scope, plan).await?;

    let mut findings_comment_id = None;
    if let Some(comment) = &plan.consolidated_comment
        && (workgraph_projection.created_any || workgraph_projection.item_refs.is_empty())
    {
        let comment = adapter
            .add_comment(AddCommentInput {
                issue: request.issue.clone(),
                body_markdown: format!(
                    "{}\n\nIdempotency-Key: `{}`",
                    comment, request.idempotency_key.0
                ),
                attachments: Vec::new(),
            })
            .await?;
        findings_comment_id = Some(comment.comment_id.0);
    }

    adapter
        .move_issue(MoveIssueInput {
            issue: request.issue.clone(),
            state: LinearStateRef::Name {
                name: target_lane.clone(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey(format!(
                "independent-review-move:{}:{}",
                issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;

    Ok(IndependentReviewProjectionResult {
        target_lane,
        workpad_comment_id: workpad.comment_id.0,
        findings_comment_id,
        workgraph_items: workgraph_projection.item_refs,
    })
}

struct IndependentReviewWorkGraphProjection {
    item_refs: Vec<IndependentReviewWorkGraphItemRef>,
    created_any: bool,
}

async fn create_independent_review_workgraph_items(
    config: &ServiceConfig,
    scope: &IndependentReviewWorkGraphScope,
    plan: &crate::independent_review::IndependentReviewAggregationPlan,
) -> anyhow::Result<IndependentReviewWorkGraphProjection> {
    if plan.workgraph_items.is_empty() {
        return Ok(IndependentReviewWorkGraphProjection {
            item_refs: Vec::new(),
            created_any: false,
        });
    }
    scope.validate()?;
    let workgraph_path = meta_runtime_state_path(&config.meerkat).join("workgraph.sqlite3");
    if let Some(parent) = workgraph_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let store = SqliteWorkGraphStore::open(&workgraph_path)?;
    let namespace = WorkNamespace::new(scope.namespace.clone())?;
    let service = WorkGraphService::with_scope(Arc::new(store), &scope.realm_id, namespace.clone());
    let mut item_refs = Vec::new();
    let mut created_any = false;
    for item in &plan.workgraph_items {
        let labels = item.labels.iter().cloned().collect::<BTreeSet<_>>();
        let existing = service
            .list(WorkItemFilter {
                realm_id: Some(scope.realm_id.clone()),
                namespace: Some(namespace.clone()),
                all_namespaces: false,
                statuses: Vec::new(),
                labels: item.labels.clone(),
                include_terminal: false,
                limit: None,
            })
            .await?
            .into_iter()
            .find(|existing| {
                existing.title == item.title
                    && existing.description.as_deref() == Some(item.detail.as_str())
            });
        let work_item = match existing {
            Some(existing) => existing,
            None => {
                created_any = true;
                service
                    .create(CreateWorkItemRequest {
                        realm_id: Some(scope.realm_id.clone()),
                        namespace: Some(namespace.clone()),
                        title: item.title.clone(),
                        description: Some(item.detail.clone()),
                        priority: WorkPriority::High,
                        completion_policy: WorkCompletionPolicy::SelfAttest,
                        failed_child_join_policy: Default::default(),
                        cancelled_child_join_policy: Default::default(),
                        labels,
                        due_at: None,
                        not_before: None,
                        snoozed_until: None,
                        external_refs: Vec::new(),
                        evidence_refs: Vec::new(),
                        status: Some(WorkStatus::Open),
                    })
                    .await?
            }
        };
        item_refs.push(IndependentReviewWorkGraphItemRef {
            id: work_item.id.to_string(),
            realm_id: work_item.realm_id,
            namespace: work_item.namespace.to_string(),
        });
    }
    Ok(IndependentReviewWorkGraphProjection {
        item_refs,
        created_any,
    })
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

    fn sample_dashboard_view(console_enabled: bool) -> DashboardView {
        DashboardView {
            running: 2,
            retrying: 1,
            tokens: 144,
            seconds: 12.5,
            error: None,
            shadow_health: "Healthy".to_string(),
            shadow_recommendations: 3,
            shadow_comparisons: 4,
            shadow_authority: "ObserveOnly".to_string(),
            running_rows: "<tr><td>LUC-621</td><td>Native</td><td>Implementing</td><td>s1</td><td>2</td><td>started</td></tr>".to_string(),
            retry_rows: "<tr><td>LUC-620</td><td>Native</td><td>1</td><td>soon</td><td>retry</td></tr>".to_string(),
            console_enabled,
            in_flight_count: 1,
            picker_tabs: r#"<a class="mob-tab focused" href="/issues/TEST-1"><span class="radio"></span><span class="mob-tab-main"><span class="meta">TEST-1 <span class="badge">● FOCUSED</span></span><div class="mob-tab-title">Runtime supplied issue</div></span><span class="meta">42s<br>CODER</span></a>"#.to_string(),
            focus_identifier: "TEST-1".to_string(),
            focus_title: "Runtime supplied issue".to_string(),
            focus_lane: "Active".to_string(),
            focus_runtime: "42s".to_string(),
            active_role: "CODER".to_string(),
            current_action: "Runtime supplied event".to_string(),
            workpad_files: r#"<div class="file"><span class="meta">runtime evidence</span><span class="meta">2t</span></div>"#.to_string(),
            activity_rows: r#"<div class="log-row"><span class="meta">now</span><span class="meta">TEST-1</span><span>Runtime supplied event</span></div>"#.to_string(),
        }
    }

    fn assert_banana_sorbet_contract(html: &str) {
        for required in [
            "--bs-paper: #ECEAE4",
            "--bs-paper-soft: #F6F4EE",
            "--bs-paper-raised: #FAF8F2",
            "--bs-ink: #1C1A17",
            "--bs-muted: #6B6760",
            "--bs-line: #D6D2C8",
            "--bs-accent: #D95A1E",
            "--bs-accent-dark: #E76A2F",
            "--bs-font-sans: Inter",
            r#"--bs-font-mono: "JetBrains Mono""#,
            "--r-sm: 2px",
            "--r-md: 6px",
            "--type-micro: 10px",
            "--type-rail: 11px",
            "--type-meta: 12px",
            "--type-body: 13px",
            "--type-card: 14px",
            "--type-panel-heading: 15px",
            "--type-heading: 16px",
            "--line-compact: 1.35",
            "--control-h: 38px",
            r#"data-bs-theme="paper""#,
        ] {
            assert!(
                html.contains(required),
                "missing Banana Sorbet token: {required}"
            );
        }
        for banned in [
            &format!("--{}cc", "_"),
            &format!("data-{}-theme", "cc"),
            &format!("{}-gradient", "linear"),
            &format!("border-radius: {}px", 12),
            &format!("border-left: {}px solid var(--accent)", 3),
            &format!("border-left: {}px solid var(--brand)", 3),
            &format!("border-top: {}px solid var(--accent)", 2),
            &format!("border-top: {}px solid var(--brand)", 2),
            &format!("deck{}reveal", "-"),
            &format!("{}: translateY(", "transform"),
            &format!("{}: deck", "animation"),
            &format!("{}px", 999),
            &format!("#{}{}", "78", "f7ff"),
            &format!("#{}{}", "6c", "f5ff"),
            &format!("cockpit{}scan", "-"),
            "font:600 28px",
            "font-size:20px",
            "font-size: 20px",
            "font-size:17px",
            "font-size: 17px",
            "font-size=\"22\"",
        ] {
            assert!(
                !html.contains(banned),
                "found stale console styling token: {banned}"
            );
        }
    }

    #[test]
    fn fugue_status_shells_use_meta_console_compact_type_scale() {
        let dashboard = render_dashboard_html(&sample_dashboard_view(true));
        let ims = render_issues_board_html(&issue_board_config());

        for (name, html) in [
            ("dashboard", dashboard.as_str()),
            ("ims", ims.as_str()),
            ("issue detail", ISSUE_DETAIL_HTML),
        ] {
            assert_banana_sorbet_contract(html);
            for token in [
                "--type-rail",
                "--type-body",
                "--type-card",
                "--type-panel-heading",
                "--type-heading",
                "--line-compact",
            ] {
                assert!(
                    html.contains(token),
                    "{name} missing compact type-scale token {token}"
                );
            }
        }

        assert!(dashboard.contains(
            ".agent-card h2 { margin:12px 0 4px; font-size:var(--type-heading); line-height:1.25; }"
        ));
        assert!(
            ims.contains("button.icon { width: 32px; padding: 0; font-size: var(--type-card); }")
        );
        assert!(ims.contains(".palette-input"));
        assert!(ims.contains("font-size: var(--type-body);"));
        assert!(ISSUE_DETAIL_HTML.contains(
            ".sheet-title { flex:1; padding:0 14px; font-size:var(--type-panel-heading);"
        ));
        assert!(
            ISSUE_DETAIL_HTML
                .contains(".lane-row { width:100%; min-height:50px; font-size:var(--type-meta);")
        );
    }

    #[test]
    fn dashboard_renders_framing_and_console_issues_tabs() {
        let html = render_dashboard_html(&sample_dashboard_view(true));

        assert_banana_sorbet_contract(&html);
        assert!(html.contains("MEERKAT FUGUE"));
        assert!(html.contains("FOCUS MOB"));
        assert!(html.contains(".top-rail"));
        assert!(html.contains(".hero {"));
        assert!(html.contains("actual Fugue mob topology"));
        assert!(html.contains("Issue lead"));
        assert!(html.contains("Kapellmeister"));
        assert!(
            html.contains("worker peer mesh: planner↔coder · planner↔reviewer · coder↔reviewer")
        );
        assert!(html.contains("review green → PR/check handoff"));
        assert!(!html.contains("Active role"));
        assert!(!html.contains("FLOW</text>"));
        assert!(html.contains("fugue:phase:implementing"));
        assert!(html.contains("CON <b>LIVE</b>"));
        assert!(html.contains("@media (max-height: 720px), (max-width: 980px)"));
        assert!(html.contains("see IMS"));
        assert!(
            !html.contains("<span>Review</span><span class=\"meta\">▸ ACTION</span><span>0</span>")
        );
        assert!(html.contains("Runtime supplied issue"));
        assert!(html.contains("--shadow-sheet"));
        assert!(html.contains(r#"href="/issues""#));
        assert!(html.contains(r#"href="/console""#));
        assert!(html.contains(r#"href="/console""#));
        assert!(html.contains(r#"href="/issues""#));
        assert!(html.contains("CONSOLE"));
        assert!(html.contains(r#"href="/issues""#));
        assert!(html.contains("TEST-1"));
        for fabricated in ["LUC-642", "Tracker simplification", "29m 46s", "12m 08s"] {
            assert!(
                !html.contains(fabricated),
                "dashboard leaked fabricated prototype value: {fabricated}"
            );
        }
    }

    #[test]
    fn dashboard_no_focus_workpad_empty_state_is_honest() {
        let mut view = sample_dashboard_view(true);
        view.focus_runtime = "unavailable".to_string();
        view.workpad_files = r#"<div class="file unavailable"><span class="meta">No active mob workpad evidence available</span><span class="meta">turns unavailable</span></div>"#.to_string();
        let html = render_dashboard_html(&view);

        assert!(html.contains("No active mob workpad evidence available"));
        assert!(html.contains("turns unavailable"));
        assert!(!html.contains(concat!("No active ", "workpad files")));
        assert!(!html.contains(concat!(">", "0", "t<")));
    }

    #[test]
    fn dashboard_renders_disabled_console_panel_without_losing_issues_tab() {
        let html = render_dashboard_html(&sample_dashboard_view(false));

        assert_banana_sorbet_contract(&html);
        assert!(html.contains("CONSOLE"));
        assert!(html.contains("MEERKAT FUGUE"));
        assert!(html.contains("CON <b>OFF</b>"));
        assert!(html.contains("FOCUS MOB"));
        assert!(html.contains("OPEN IMS"));
        assert!(html.contains(r#"href="/issues""#));
    }

    #[test]
    fn dashboard_overview_layout_is_responsive_and_readable() {
        let html = render_dashboard_html(&sample_dashboard_view(true));

        assert_banana_sorbet_contract(&html);
        for required in [
            "body { margin:0; min-height:100vh; overflow:auto;",
            ".top-rail { min-height:var(--control-h); display:flex; align-items:stretch; flex-wrap:nowrap;",
            ".kpis { display:flex; min-height:var(--control-h); flex:0 1 auto; overflow-x:auto;",
            ".picker { min-height:64px; display:flex; flex-wrap:wrap; align-items:stretch;",
            ".mob-tab { flex:1 1 240px; min-width:220px; min-height:64px;",
            ".hero { min-height:520px; display:grid; grid-template-rows:auto auto minmax(360px,1fr);",
            ".identity h1 { margin:0; font-size:var(--type-heading); font-weight:500; flex:1 1 260px;",
            ".timeline { display:grid; grid-template-columns:repeat(4,minmax(180px,1fr));",
            ".hero-body { min-height:0; display:grid; grid-template-columns:minmax(420px,1fr) minmax(280px,340px);",
            ".flow { border-right:1px solid var(--hair); padding:24px; min-width:0; overflow:auto;",
            "svg { width:100%; height:100%; min-width:520px; min-height:260px; }",
            ".agent-card { min-width:0; padding:18px; border-bottom:1px solid var(--hair); overflow:auto; }",
            ".lower { min-height:220px; display:grid; grid-template-columns:minmax(0,1.4fr) minmax(280px,1fr);",
            ".log-row { display:grid; grid-template-columns:minmax(64px,74px) minmax(60px,72px) minmax(0,1fr);",
            ".lane-row { display:grid; grid-template-columns:28px minmax(0,1fr) minmax(70px,88px) auto;",
            "@media (max-height: 720px), (max-width: 980px) { body { min-height:100vh; overflow:auto; } .top-rail { flex-wrap:wrap;",
            ".kpis { order:2; flex:1 0 100%; border-top:1px solid var(--hair); }",
            "@media (max-width: 720px) { .top-rail { position:sticky; top:0; } .tabs { overflow-x:auto; max-width:100%; }",
        ] {
            assert!(
                html.contains(required),
                "dashboard missing responsive layout contract: {required}"
            );
        }

        for preserved in [
            r#"href="/overview""#,
            r#"href="/issues""#,
            r#"href="/console""#,
            "CON <b>LIVE</b>",
            "Runtime supplied issue",
            "worker peer mesh: planner↔coder · planner↔reviewer · coder↔reviewer",
        ] {
            assert!(
                html.contains(preserved),
                "dashboard lost protected behavior while changing layout: {preserved}"
            );
        }

        assert!(!html.contains("height:100vh; overflow:hidden;"));
        assert!(!html.contains(".picker { min-width:900px;"));
        assert!(!html.contains("height:calc(100vh - 38px - 64px - 220px)"));
        assert!(!html.contains(".activity, .lanes { padding:14px 18px; overflow:hidden;"));
    }

    #[test]
    fn issue_routes_keep_dashboard_navigation_targets() {
        let html = render_issues_board_html(&issue_board_config());

        assert_banana_sorbet_contract(&html);
        assert!(html.contains(r#"href="/overview""#));
        assert!(html.contains(r#"href="/console""#));
        assert!(html.contains(r#"href="/ensemble""#));
        assert!(html.contains(r#"href="/issues""#));
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
        assert!(html.contains("+ NEW"));
        assert!(html.contains("openFullDetail(card.dataset.issue)"));
        assert!(!html.contains("loadDetail(card.dataset.issue);"));
        assert!(html.contains(r#"id="kpiTok">unavailable</b>"#));
        assert!(html.contains(r#"id="kpiTime">unavailable</b>"#));
        assert!(html.contains(r#"id="kpiShad">unavailable</b>"#));
        assert!(html.contains("app.board.totals.tokens ?? \"unavailable\""));
        assert!(!html.contains(concat!("app.board.totals.tokens ", "|| 0")));
        assert!(html.contains("turns unavailable"));
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
        assert!(html.contains("Attachments upload after issue creation and are persisted as issue attachment comments."));
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
        assert!(html.contains(r#"class="active" href="/ensemble""#));
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
        assert_banana_sorbet_contract(ISSUE_DETAIL_HTML);
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
        assert!(ISSUE_DETAIL_HTML.contains("method: \"POST\""));
        assert!(ISSUE_DETAIL_HTML.contains("body_markdown: body"));
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
        assert!(source.contains("uploadIssueAttachments(createdIdentifier, createAttachmentState"));
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
        assert!(path.starts_with(workspace.path().join(".fugue/issue-attachments/LUC-649")));

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
    fn console_browser_compat_embeds_meta_in_fugue_shell_once() {
        let html = r#"<!doctype html><html><head><title>Meta</title></head><body class="meta"><main id="meta-root"></main></body></html>"#;
        let patched = inject_console_browser_compat_html(html);

        assert!(patched.contains("data-fugue-random-uuid-polyfill"));
        assert!(patched.contains("data-fugue-console-chrome"));
        assert!(patched.contains("class=\"fugue-console-embedded meta\""));
        assert!(patched.contains("MEERKAT FUGUE"));
        assert!(patched.contains(r#"href="/overview""#));
        assert!(patched.contains(r#"href="/issues""#));
        assert!(patched.contains(r#"class="active" href="/console""#));
        assert!(patched.contains("META CONSOLE"));
        assert!(patched.contains("meta-root"));
        assert_banana_sorbet_contract(&patched);

        let repatched = inject_console_browser_compat_html(&patched);
        assert_eq!(
            repatched.matches("data-fugue-console-chrome").count(),
            patched.matches("data-fugue-console-chrome").count()
        );
        assert_eq!(
            repatched.matches("data-fugue-random-uuid-polyfill").count(),
            patched.matches("data-fugue-random-uuid-polyfill").count()
        );
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
                    "project_slug": "fugue-ims",
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
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
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
        assert!(
            inspected
                .workpad
                .as_ref()
                .is_some_and(|workpad| workpad.body_markdown.contains("### Deployment"))
        );
        assert!(
            deploy_idempotency_already_recorded(&config, &request.issue, &request.idempotency_key)
                .await
                .expect("idempotency check")
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
                    "project_slug": "fugue-ims",
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
        };
        let merge = MergePrOutput {
            pull_request: request.pull_request.clone(),
            merged: true,
            merge_commit_sha: Some(GitSha("merge456".to_string())),
        };
        let ota = OtaUpdateReport {
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
        };
        let ota = OtaUpdateReport {
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
                    "project_slug": "fugue-ims",
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
        let request = IndependentReviewCompletionRequest {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-633".to_string()),
            },
            pull_request: GitHubPullRequestSnapshot {
                repo: GitHubRepoRef {
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
                realm_id: "fugue-review-realm".to_string(),
                namespace: "fugue".to_string(),
            },
            verdicts: vec![
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
            ],
            timed_out: false,
            idempotency_key: IdempotencyKey("review:LUC-633:abc123:1".to_string()),
        };
        let plan = aggregate_independent_review(IndependentReviewAggregationInput {
            config: config.independent_review.clone(),
            issue_identifier: "LUC-633".to_string(),
            head_sha: request.head_sha.clone(),
            verdicts: request.verdicts.clone(),
            timed_out: false,
        });

        let projection = record_independent_review_projection(&config, &request, "LUC-633", &plan)
            .await
            .expect("review projection");

        assert_eq!(projection.target_lane, "Rework");
        assert!(projection.findings_comment_id.is_some());
        assert_eq!(projection.workgraph_items.len(), 1);
        assert_eq!(projection.workgraph_items[0].realm_id, "fugue-review-realm");
        assert_eq!(projection.workgraph_items[0].namespace, "fugue");
        let repeated_projection =
            record_independent_review_projection(&config, &request, "LUC-633", &plan)
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
            "fugue-review-realm",
            WorkNamespace::new("fugue").expect("namespace"),
        );
        let items = service
            .list(WorkItemFilter {
                realm_id: Some("fugue-review-realm".to_string()),
                namespace: Some(WorkNamespace::new("fugue").expect("namespace")),
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
