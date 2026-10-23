use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
#[error("Native cleanup is incomplete; host restart required: {0}")]
pub(crate) struct NativeCleanupIncomplete(#[source] pub(crate) anyhow::Error);

#[derive(Debug, Error)]
pub enum FugueError {
    #[error("missing_workflow_file: {0}")]
    MissingWorkflowFile(PathBuf),

    #[error("workflow_parse_error: {0}")]
    WorkflowParseError(String),

    #[error("workflow_front_matter_not_a_map")]
    WorkflowFrontMatterNotMap,

    #[error("template_parse_error: {0}")]
    TemplateParseError(String),

    #[error("template_render_error: {0}")]
    TemplateRenderError(String),

    #[error("unsupported_tracker_kind: {0}")]
    UnsupportedTrackerKind(String),

    #[error("missing_tracker_api_key")]
    MissingTrackerApiKey,

    #[error("missing_tracker_project_slug")]
    MissingTrackerProjectSlug,

    #[error("invalid_config: {0}")]
    InvalidConfig(String),

    #[error("workspace_error: {0}")]
    Workspace(String),

    #[error("hook_failed: {0}")]
    HookFailed(String),

    #[error("linear_api_request: {0}")]
    LinearApiRequest(String),

    #[error("linear_api_status: {0}")]
    LinearApiStatus(String),

    #[error("linear_graphql_errors: {0}")]
    LinearGraphqlErrors(String),

    #[error("linear_unknown_payload: {0}")]
    LinearUnknownPayload(String),

    #[error("linear_missing_end_cursor")]
    LinearMissingEndCursor,

    #[error("jira_api_status: {status}")]
    JiraApiStatus { status: u16 },

    #[error("issue_created_needs_attention: {}: {source}", created.identifier)]
    IssueCreatedNeedsAttention {
        created: crate::domain::CreatedIssueReceipt,
        #[source]
        source: Box<FugueError>,
    },

    #[error("jira_stale_issue: refresh issue state before retrying")]
    JiraStaleIssue,

    #[error("jira_request: {0}")]
    JiraRequest(String),

    #[error("meerkat_rpc_not_found: {0}")]
    MeerkatRpcNotFound(String),

    #[error("invalid_workspace_cwd: {0}")]
    InvalidWorkspaceCwd(String),

    #[error("response_timeout: {0}")]
    ResponseTimeout(String),

    #[error("turn_timeout: {0}")]
    TurnTimeout(String),

    #[error("process_exit: {0}")]
    ProcessExit(String),

    #[error("response_error: {0}")]
    ResponseError(String),

    #[error("turn_failed: {0}")]
    TurnFailed(String),

    #[error("turn_cancelled: {0}")]
    TurnCancelled(String),

    #[error("turn_input_required: {0}")]
    TurnInputRequired(String),
}

pub type Result<T> = std::result::Result<T, FugueError>;
