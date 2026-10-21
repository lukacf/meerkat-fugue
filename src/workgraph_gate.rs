use std::path::{Path, PathBuf};
use std::sync::Arc;

use meerkat::{SqliteWorkGraphStore, WorkGraphService, WorkItem, WorkItemFilter, WorkStatus};

use crate::config::TrackerConfig;
use crate::domain::normalize_state;
use crate::tools::{LinearIssueRef, LinearStateRef, MoveIssueInput};

pub const WORKGRAPH_STATE_PATH_ENV: &str = "FUGUE_WORKGRAPH_STATE_PATH";
pub const WORKGRAPH_BLOCKED_STATES_ENV: &str = "FUGUE_WORKGRAPH_BLOCKED_STATES";
pub const FUGUE_WORKGRAPH_LABEL: &str = "fugue";
pub const FUGUE_WORKGRAPH_BLOCKING_LABEL: &str = "blocking";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkGraphTerminalGate {
    pub state_path: PathBuf,
    pub terminal_states: Vec<String>,
    pub allowed_terminal_states: Vec<String>,
}

impl WorkGraphTerminalGate {
    pub fn new(
        state_path: impl Into<PathBuf>,
        terminal_states: Vec<String>,
        allowed_terminal_states: Vec<String>,
    ) -> Self {
        Self {
            state_path: state_path.into(),
            terminal_states,
            allowed_terminal_states,
        }
    }

    pub fn from_env(config: &TrackerConfig) -> Option<Self> {
        let state_path = std::env::var(WORKGRAPH_STATE_PATH_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())?;
        Some(Self::new(
            state_path,
            config.terminal_states.clone(),
            env_list(WORKGRAPH_BLOCKED_STATES_ENV),
        ))
    }

    pub async fn guard_terminal_move(&self, input: &MoveIssueInput) -> Result<(), String> {
        let Some(target_state) = target_state_name(input) else {
            return Ok(());
        };
        if !self.blocks_target_state(target_state) {
            return Ok(());
        }
        let issue = issue_label_value(&input.issue);
        let unresolved = self.unresolved_blocking_items(&issue).await?;
        if unresolved.is_empty() {
            return Ok(());
        }
        Err(unresolved_terminal_move_message(
            &issue,
            target_state,
            &unresolved,
        ))
    }

    fn blocks_target_state(&self, state: &str) -> bool {
        let normalized = normalize_state(state);
        let is_terminal = self
            .terminal_states
            .iter()
            .any(|candidate| normalize_state(candidate) == normalized);
        let explicitly_allowed = self
            .allowed_terminal_states
            .iter()
            .any(|candidate| normalize_state(candidate) == normalized);
        is_terminal && !explicitly_allowed
    }

    pub async fn unresolved_blocking_items(&self, issue: &str) -> Result<Vec<WorkItem>, String> {
        let service = workgraph_service(&self.state_path)?;
        service
            .list(WorkItemFilter {
                statuses: vec![
                    WorkStatus::Open,
                    WorkStatus::InProgress,
                    WorkStatus::Blocked,
                ],
                labels: vec![
                    FUGUE_WORKGRAPH_LABEL.to_string(),
                    issue_workgraph_label(issue),
                    FUGUE_WORKGRAPH_BLOCKING_LABEL.to_string(),
                ],
                include_terminal: false,
                ..WorkItemFilter::default()
            })
            .await
            .map_err(|error| format!("query WorkGraph review obligations: {error}"))
    }
}

pub fn issue_workgraph_label(issue: &str) -> String {
    format!("issue:{}", issue.trim())
}

pub fn workgraph_blocked_states_env_value(states: &[String]) -> String {
    states.join(",")
}

fn workgraph_service(path: &Path) -> Result<WorkGraphService, String> {
    let store = SqliteWorkGraphStore::open(path)
        .map_err(|error| format!("open WorkGraph store at {}: {error}", path.display()))?;
    let realm = meerkat_core::mob_realm_id(crate::native_mob_contract::FUGUE_MOB_ID)
        .map_err(|error| format!("resolve Fugue WorkGraph realm: {error}"))?;
    Ok(WorkGraphService::with_scope(
        Arc::new(store),
        realm.as_str(),
        meerkat::WorkNamespace::default(),
    ))
}

fn target_state_name(input: &MoveIssueInput) -> Option<&str> {
    match &input.state {
        LinearStateRef::Name { name } => Some(name.as_str()),
        LinearStateRef::Id { .. } => None,
    }
}

fn issue_label_value(issue: &LinearIssueRef) -> String {
    match issue {
        LinearIssueRef::Identifier { identifier } => identifier.0.clone(),
        LinearIssueRef::Id { id } => id.0.clone(),
    }
}

fn unresolved_terminal_move_message(issue: &str, target_state: &str, items: &[WorkItem]) -> String {
    let summary = items
        .iter()
        .take(5)
        .map(|item| format!("{} ({:?}, rev {})", item.title, item.status, item.revision))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "refusing to move {issue} to non-Blocked terminal state {target_state:?}: {} unresolved Fugue WorkGraph obligation(s) remain. Send the issue back through the pipeline and have the reviewer close completed items before terminal success. Open items: {summary}",
        items.len()
    )
}

fn env_list(key: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use meerkat::{CreateWorkItemRequest, WorkCompletionPolicy, WorkPriority};

    use super::*;
    use crate::tools::{IdempotencyKey, LinearIssueIdentifier};

    #[tokio::test]
    async fn terminal_gate_blocks_success_with_open_fugue_issue_obligation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("workgraph.sqlite3");
        // Create through the native mob scope, independently of the gate helper.
        let store = SqliteWorkGraphStore::open(&path).expect("store");
        let native_realm = meerkat_core::mob_realm_id(crate::native_mob_contract::FUGUE_MOB_ID)
            .expect("mob realm");
        let service = WorkGraphService::with_scope(
            Arc::new(store),
            native_realm.as_str(),
            meerkat::WorkNamespace::default(),
        );
        service
            .create(CreateWorkItemRequest {
                realm_id: None,
                namespace: None,
                title: "Fix review finding".to_string(),
                description: None,
                priority: WorkPriority::High,
                completion_policy: WorkCompletionPolicy::SelfAttest,
                failed_child_join_policy: Default::default(),
                cancelled_child_join_policy: Default::default(),
                labels: BTreeSet::from([
                    FUGUE_WORKGRAPH_LABEL.to_string(),
                    issue_workgraph_label("LUC-1"),
                    FUGUE_WORKGRAPH_BLOCKING_LABEL.to_string(),
                ]),
                due_at: None,
                not_before: None,
                snoozed_until: None,
                external_refs: Vec::new(),
                evidence_refs: Vec::new(),
                status: Some(WorkStatus::Open),
            })
            .await
            .expect("create item");
        let gate = WorkGraphTerminalGate::new(
            &path,
            vec![
                "Done".to_string(),
                "Canceled".to_string(),
                "Blocked".to_string(),
            ],
            vec!["Blocked".to_string()],
        );

        let error = gate
            .guard_terminal_move(&MoveIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-1".to_string()),
                },
                state: LinearStateRef::Name {
                    name: "Done".to_string(),
                },
                expected_updated_at: None,
                idempotency_key: IdempotencyKey("move-1".to_string()),
            })
            .await
            .unwrap_err();

        assert!(error.contains("unresolved Fugue WorkGraph obligation"));
    }

    #[tokio::test]
    async fn terminal_gate_allows_blocked_lane_with_open_obligation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("workgraph.sqlite3");
        let gate = WorkGraphTerminalGate::new(
            &path,
            vec!["Done".to_string(), "Blocked".to_string()],
            vec!["Blocked".to_string()],
        );

        gate.guard_terminal_move(&MoveIssueInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-1".to_string()),
            },
            state: LinearStateRef::Name {
                name: "Blocked".to_string(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("move-1".to_string()),
        })
        .await
        .expect("blocked is allowed for human intervention");
    }
}
