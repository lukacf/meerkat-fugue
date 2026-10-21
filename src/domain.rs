use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockerRef {
    pub id: Option<String>,
    pub identifier: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Issue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: Option<String>,
    pub priority: Option<i64>,
    pub state: String,
    pub branch_name: Option<String>,
    pub url: Option<String>,
    pub labels: Vec<String>,
    pub blocked_by: Vec<BlockerRef>,
    #[serde(default)]
    pub attachments: Vec<IssueAttachment>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssueAttachment {
    pub id: Option<String>,
    pub title: Option<String>,
    pub url: String,
    pub local_path: Option<PathBuf>,
}

impl Issue {
    pub fn normalized_state(&self) -> String {
        normalize_state(&self.state)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceInfo {
    pub path: PathBuf,
    pub workspace_key: String,
    pub created_now: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

impl TokenTotals {
    pub fn add_delta(&mut self, next: &TokenTotals, previous: &mut TokenTotals) {
        self.input_tokens += next.input_tokens.saturating_sub(previous.input_tokens);
        self.output_tokens += next.output_tokens.saturating_sub(previous.output_tokens);
        self.total_tokens += next.total_tokens.saturating_sub(previous.total_tokens);
        *previous = next.clone();
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveSession {
    pub session_id: Option<String>,
    pub session_ref: Option<String>,
    pub active_turn_request_id: Option<String>,
    pub stream_id: Option<String>,
    pub stream_opened_at: Option<DateTime<Utc>>,
    pub stream_closed_at: Option<DateTime<Utc>>,
    pub last_stream_sequence: Option<u64>,
    pub stream_close_reason: Option<String>,
    pub stream_error: Option<String>,
    pub meerkat_rpc_pid: Option<String>,
    pub last_meerkat_event: Option<String>,
    pub last_meerkat_timestamp: Option<DateTime<Utc>>,
    pub last_meerkat_message: Option<String>,
    pub meerkat_tokens: TokenTotals,
    pub last_reported_tokens: TokenTotals,
    pub turn_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentEvent {
    pub at: DateTime<Utc>,
    pub event: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentUpdate {
    pub event: String,
    pub timestamp: DateTime<Utc>,
    pub session_id: Option<String>,
    pub session_ref: Option<String>,
    pub active_turn_request_id: Option<String>,
    pub stream_id: Option<String>,
    pub stream_sequence: Option<u64>,
    pub stream_close_reason: Option<String>,
    pub stream_error: Option<String>,
    pub meerkat_rpc_pid: Option<String>,
    pub message: Option<String>,
    pub absolute_usage: Option<TokenTotals>,
    pub rate_limits: Option<Value>,
    pub turn_count: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerExit {
    Normal,
    Failed,
    TimedOut,
    Stalled,
    CanceledByReconciliation,
}

#[derive(Debug, Clone)]
pub struct WorkerOutcome {
    pub exit: WorkerExit,
    pub error: Option<String>,
    pub session: LiveSession,
    pub final_issue: Issue,
}

pub fn normalize_state(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub fn sanitize_workspace_key(identifier: &str) -> String {
    identifier
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn is_todo_state(state: &str) -> bool {
    normalize_state(state) == "todo"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_workspace_key() {
        assert_eq!(sanitize_workspace_key("ABC-123"), "ABC-123");
        assert_eq!(sanitize_workspace_key("Team 42/hello"), "Team_42_hello");
        assert_eq!(sanitize_workspace_key("../x"), ".._x");
    }
}
