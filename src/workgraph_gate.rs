use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use meerkat::{SqliteWorkGraphStore, WorkGraphService, WorkItem, WorkItemFilter, WorkStatus};

use crate::config::TrackerConfig;
use crate::domain::normalize_state;
use crate::tools::{
    ClaimIssueInput, FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolResult,
    LinearIssueRef, LinearStateRef, MergePrInput, MoveIssueInput, RetryAdvice,
    ToolExecutionMetadata,
};

pub const WORKGRAPH_STATE_PATH_ENV: &str = "FUGUE_WORKGRAPH_STATE_PATH";
pub const WORKGRAPH_BLOCKED_STATES_ENV: &str = "FUGUE_WORKGRAPH_BLOCKED_STATES";
pub const WORKGRAPH_MOB_ID_ENV: &str = "FUGUE_WORKGRAPH_MOB_ID";
pub const FUGUE_WORKGRAPH_LABEL: &str = "fugue";
pub const FUGUE_WORKGRAPH_BLOCKING_LABEL: &str = "blocking";

/// Shared meaning of the native labels used by Fugue's existing merge gate.
pub(crate) const WORKGRAPH_PREREQUISITE_CONTRACT: &str = "WorkGraph prerequisite contract: \
Use blocking only for prerequisites that can be completed before the guarded merge, \
such as implementation, review execution, failing checks and actionable findings. \
Do not create a blocking item for performing the host-owned merge, deployment, or \
recording its eventual receipt: that would require the action to complete before \
its own gate permits it. The host's typed deployment transaction owns execution \
and its receipt. Optional follow-up notes may remain open with fugue and issue \
labels, without blocking. Never remove blocking from an unresolved prerequisite \
to force progress, and never claim a future action completed. A misclassified \
follow-up requires reviewer inspection of the actual item and evidence, with \
explicit classification correction preserved in its history; any remaining \
prerequisite must stay blocking. Only an observed successful merge can establish \
merge completion.";

/// Extract the lane change before dispatch, including the optional change in a claim.
/// Invalid arguments must fail before either assignment or state mutation occurs.
pub(crate) fn lane_move_for_tool(
    operation: FugueToolOperation,
    arguments: &serde_json::Value,
) -> Result<Option<MoveIssueInput>, serde_json::Error> {
    match operation {
        FugueToolOperation::LinearMoveIssue => serde_json::from_value(arguments.clone()).map(Some),
        FugueToolOperation::LinearClaimIssue => {
            let input: ClaimIssueInput = serde_json::from_value(arguments.clone())?;
            Ok(input.target_state.map(|state| MoveIssueInput {
                issue: input.issue,
                state,
                expected_updated_at: None,
                idempotency_key: input.idempotency_key,
            }))
        }
        _ => Ok(None),
    }
}

pub(crate) fn guard_public_lane_move(
    tracker: &TrackerConfig,
    review_handoff_states: &[String],
    action_states: &[String],
    input: &MoveIssueInput,
) -> Result<(), String> {
    let target = match &input.state {
        LinearStateRef::Name { name } => name.trim(),
        LinearStateRef::Id { .. } => {
            return Err(
                "Issue lane changes require a named Fugue public lane, not an opaque Linear state id"
                    .to_string(),
            );
        }
    };
    if target.is_empty() {
        return Err("Issue lane change target must not be empty".to_string());
    }
    if tracker.kind != "jira"
        && let Some(replacement) = legacy_public_lane_replacement(target)
    {
        return Err(format!(
            "{target:?} is a legacy Fugue/Linear phase lane and must not be written by agents; use {replacement:?} and record phase/detail as labels, WorkGraph, Workpad, or Meta evidence"
        ));
    }
    if direct_public_lane_is_action(action_states, target) {
        return Err(format!(
            "{target:?} is a Fugue action lane. Action lanes are operator or host-observed control inputs only; agents must not move issues there with linear_move_issue or linear_claim_issue. Keep ordinary running work on the active projection lane, record PR/review/deploy intent as evidence, or use Blocked/terminal lanes when those facts are true."
        ));
    }
    if direct_public_lane_allowed(tracker, review_handoff_states, target) {
        return Ok(());
    }
    Err(format!(
        "{target:?} is not a configured Fugue public lane for this queue. Allowed lanes: {}",
        PublicLaneList::new(tracker, review_handoff_states, action_states)
    ))
}

fn direct_public_lane_is_action(action_states: &[String], target: &str) -> bool {
    let normalized = normalize_state(target);
    action_states
        .iter()
        .any(|candidate| normalize_state(candidate) == normalized)
}

fn direct_public_lane_allowed(
    tracker: &TrackerConfig,
    review_handoff_states: &[String],
    target: &str,
) -> bool {
    let normalized = normalize_state(target);
    tracker
        .intake_states
        .iter()
        .chain(tracker.in_flight_states.iter())
        .chain(tracker.active_states.iter())
        .chain(tracker.terminal_states.iter())
        .chain(review_handoff_states.iter())
        .any(|candidate| normalize_state(candidate) == normalized)
}

fn legacy_public_lane_replacement(target: &str) -> Option<&'static str> {
    match normalize_state(target).as_str() {
        "planning" | "implementing" | "reviewing" | "in progress" => Some("Active"),
        "do review" | "in review" | "ready for review" | "human review" => Some("Review"),
        "rework" => Some("Active"),
        "merging" => Some("Deploy"),
        _ => None,
    }
}

struct PublicLaneList<'a> {
    tracker: &'a TrackerConfig,
    review_handoff_states: &'a [String],
    action_states: &'a [String],
}

impl<'a> PublicLaneList<'a> {
    fn new(
        tracker: &'a TrackerConfig,
        review_handoff_states: &'a [String],
        action_states: &'a [String],
    ) -> Self {
        Self {
            tracker,
            review_handoff_states,
            action_states,
        }
    }
}

impl std::fmt::Display for PublicLaneList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        let mut seen = BTreeSet::new();
        for lane in self
            .tracker
            .intake_states
            .iter()
            .chain(self.tracker.in_flight_states.iter())
            .chain(self.tracker.active_states.iter())
            .chain(self.tracker.terminal_states.iter())
            .chain(self.review_handoff_states.iter())
        {
            if direct_public_lane_is_action(self.action_states, lane) {
                continue;
            }
            if !seen.insert(normalize_state(lane)) {
                continue;
            }
            if !first {
                f.write_str(", ")?;
            }
            first = false;
            f.write_str(lane)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkGraphTerminalGate {
    pub state_path: PathBuf,
    pub mob_id: String,
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
            mob_id: crate::native_mob_contract::FUGUE_MOB_ID.to_owned(),
            terminal_states,
            allowed_terminal_states,
        }
    }

    pub fn for_mob(mut self, mob_id: impl Into<String>) -> Self {
        self.mob_id = mob_id.into();
        self
    }

    pub fn for_service(config: &crate::config::ServiceConfig) -> Self {
        Self::new(
            crate::meta_runtime::meta_runtime_state_path(&config.meerkat).join("workgraph.sqlite3"),
            config.tracker.terminal_states.clone(),
            Vec::new(),
        )
        .for_mob(config.meerkat.mob_id().into_owned())
    }

    /// Check native obligations immediately before the irreversible GitHub action.
    pub async fn merge_pr_with(
        &self,
        tracker: &TrackerConfig,
        issue: &LinearIssueRef,
        input: MergePrInput,
        merge: impl FnOnce(MergePrInput) -> FugueToolResult,
    ) -> FugueToolResult {
        let reason = match canonical_issue_identifier(tracker, issue).await {
            Ok(issue) => match self.unresolved_blocking_items(&issue).await {
                Ok(items) if items.is_empty() => return merge(input),
                Ok(items) => format!(
                    "Cannot merge {issue}: {} unresolved blocking WorkGraph items remain",
                    items.len()
                ),
                Err(error) => format!("Cannot verify WorkGraph obligations before merge: {error}"),
            },
            Err(error) => format!("Cannot resolve the issue before merge: {error}"),
        };
        FugueToolResult::Err {
            error: FugueToolError {
                operation: FugueToolOperation::GitHubMergePr,
                code: FugueToolErrorCode::InvalidInput,
                message: reason,
                retry: RetryAdvice::DoNotRetry,
                idempotency_key: Some(input.idempotency_key),
            },
            metadata: ToolExecutionMetadata::default(),
        }
    }

    pub fn from_env(config: &TrackerConfig) -> Option<Self> {
        let state_path = std::env::var(WORKGRAPH_STATE_PATH_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())?;
        Some(
            Self::new(
                state_path,
                config.terminal_states.clone(),
                env_list(WORKGRAPH_BLOCKED_STATES_ENV),
            )
            .for_mob(
                std::env::var(WORKGRAPH_MOB_ID_ENV)
                    .unwrap_or_else(|_| crate::native_mob_contract::FUGUE_MOB_ID.to_owned()),
            ),
        )
    }

    pub async fn guard_terminal_move(
        &self,
        tracker: &TrackerConfig,
        input: &MoveIssueInput,
    ) -> Result<(), String> {
        let target_state = target_state_name(input)?;
        if !self.blocks_target_state(target_state) {
            return Ok(());
        }
        let issue = canonical_issue_identifier(tracker, &input.issue).await?;
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
        let service = workgraph_service_for_mob(&self.state_path, &self.mob_id)?;
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

fn workgraph_service_for_mob(path: &Path, mob_id: &str) -> Result<WorkGraphService, String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("native WorkGraph database is unavailable: {error}"))?;
    if !metadata.is_file() {
        return Err("native WorkGraph database is not a regular file".into());
    }
    let store = SqliteWorkGraphStore::open(path)
        .map_err(|error| format!("open WorkGraph store at {}: {error}", path.display()))?;
    let realm = meerkat_core::mob_realm_id(mob_id)
        .map_err(|error| format!("resolve Fugue WorkGraph realm: {error}"))?;
    Ok(WorkGraphService::with_scope(
        Arc::new(store),
        realm.as_str(),
        meerkat::WorkNamespace::default(),
    ))
}

fn target_state_name(input: &MoveIssueInput) -> Result<&str, String> {
    match &input.state {
        LinearStateRef::Name { name } if !name.trim().is_empty() => Ok(name.as_str()),
        _ => Err(
            "Use a nonempty tracker state name so terminal WorkGraph policy can be verified".into(),
        ),
    }
}

pub(crate) async fn canonical_issue_identifier(
    tracker: &TrackerConfig,
    issue: &LinearIssueRef,
) -> Result<String, String> {
    let adapter = crate::linear_adapter::IssueAdapter::from_config(tracker)
        .map_err(|error| error.to_string())?;
    let inspected = adapter
        .inspect_issue(crate::tools::InspectIssueInput {
            issue: issue.clone(),
        })
        .await
        .map_err(|error| error.to_string())?;
    let identifier = inspected.issue.identifier.0;
    if identifier.trim().is_empty() {
        return Err("tracker returned an empty issue identifier".into());
    }
    Ok(identifier)
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

    fn test_tracker(path: &Path, key: &str) -> (TrackerConfig, LinearIssueRef) {
        let definition = crate::config::WorkflowDefinition {
            config: serde_json::json!({"tracker": {
                "kind": "fugue-ims", "endpoint": path.display().to_string(),
                "project_slug": "example/api", "terminal_states": ["Done"]
            }})
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "test".into(),
        };
        let tracker =
            crate::config::ServiceConfig::from_definition(path.with_extension("yaml"), &definition)
                .unwrap()
                .tracker;
        let issue = crate::fugue_ims::create_issue_for_config(
            &tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey(key.into()),
                repo: "example/api".into(),
                title: "Gate fixture".into(),
                description: None,
                stage: crate::ims::ImsStageName("Active".into()),
                labels: Vec::new(),
            },
        )
        .unwrap();
        (
            tracker,
            LinearIssueRef::Id {
                id: crate::tools::LinearIssueId(issue.id.0),
            },
        )
    }

    #[tokio::test]
    async fn repository_gate_reads_its_native_mob_scope_without_cross_repository_leakage() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workgraph.sqlite3");
        let (tracker, issue_id) = test_tracker(&temp.path().join("issues.sqlite3"), "DEMO-1");
        let store = SqliteWorkGraphStore::open(&path).unwrap();
        let realm = meerkat_core::mob_realm_id("ensemble-api").unwrap();
        let service = WorkGraphService::with_scope(
            Arc::new(store),
            realm.as_str(),
            meerkat::WorkNamespace::default(),
        );
        service
            .create(CreateWorkItemRequest {
                realm_id: None,
                namespace: None,
                title: "Specialist review is still required".into(),
                description: None,
                priority: WorkPriority::High,
                completion_policy: WorkCompletionPolicy::SelfAttest,
                failed_child_join_policy: Default::default(),
                cancelled_child_join_policy: Default::default(),
                labels: BTreeSet::from([
                    FUGUE_WORKGRAPH_LABEL.into(),
                    issue_workgraph_label("DEMO-1"),
                    FUGUE_WORKGRAPH_BLOCKING_LABEL.into(),
                ]),
                due_at: None,
                not_before: None,
                snoozed_until: None,
                external_refs: Vec::new(),
                evidence_refs: Vec::new(),
                status: Some(WorkStatus::Open),
            })
            .await
            .unwrap();
        let gate = WorkGraphTerminalGate::new(&path, vec!["Done".into()], Vec::new())
            .for_mob("ensemble-api");
        let merge: MergePrInput = serde_json::from_value(serde_json::json!({
            "repo": {"owner":"example", "name":"api"},
            "pull_request": {"kind":"number", "number":1},
            "method":"squash", "expected_head_sha":"reviewed-head",
            "idempotency_key":"merge:DEMO-1"
        }))
        .unwrap();
        let result = gate
            .merge_pr_with(
                &tracker,
                &LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("DEMO-1".into()),
                },
                merge.clone(),
                |_| panic!("GitHub must not be called before native obligations are clear"),
            )
            .await;
        assert!(
            matches!(result, FugueToolResult::Err {error, ..} if error.message.contains("unresolved blocking"))
        );
        let id_result = gate
            .merge_pr_with(&tracker, &issue_id, merge.clone(), |_| {
                panic!("an immutable issue ID must resolve to the same blocking issue key")
            })
            .await;
        assert!(
            matches!(id_result, FugueToolResult::Err {error, ..} if error.message.contains("unresolved blocking"))
        );
        let unresolved_id = LinearIssueRef::Id {
            id: crate::tools::LinearIssueId("opaque-tracker-id".into()),
        };
        let result = gate
            .merge_pr_with(&tracker, &unresolved_id, merge, |_| {
                panic!("an unresolved tracker ID must never bypass issue-key obligations")
            })
            .await;
        assert!(matches!(result, FugueToolResult::Err { .. }));
        assert_eq!(
            gate.unresolved_blocking_items("DEMO-1")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            gate.for_mob("ensemble-web")
                .unresolved_blocking_items("DEMO-1")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn terminal_gate_blocks_success_with_open_fugue_issue_obligation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("workgraph.sqlite3");
        let (tracker, _) = test_tracker(&temp.path().join("issues.sqlite3"), "LUC-1");
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
            .guard_terminal_move(
                &tracker,
                &MoveIssueInput {
                    issue: LinearIssueRef::Identifier {
                        identifier: LinearIssueIdentifier("LUC-1".to_string()),
                    },
                    state: LinearStateRef::Name {
                        name: "Done".to_string(),
                    },
                    expected_updated_at: None,
                    idempotency_key: IdempotencyKey("move-1".to_string()),
                },
            )
            .await
            .unwrap_err();

        assert!(error.contains("unresolved Fugue WorkGraph obligation"));
    }

    #[tokio::test]
    async fn terminal_gate_refuses_missing_native_state_without_creating_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workgraph.sqlite3");
        let (tracker, issue) = test_tracker(&temp.path().join("issues.sqlite3"), "DEMO-1");
        let gate = WorkGraphTerminalGate::new(&path, vec!["Done".into()], Vec::new());
        let result = gate
            .guard_terminal_move(
                &tracker,
                &MoveIssueInput {
                    issue,
                    state: LinearStateRef::Name {
                        name: "Done".into(),
                    },
                    expected_updated_at: None,
                    idempotency_key: IdempotencyKey("missing-native-state".into()),
                },
            )
            .await;
        assert!(
            result.is_err(),
            "missing native state must not mean no obligations"
        );
        assert!(
            !path.exists(),
            "the read gate must not initialize a replacement store"
        );
    }

    #[tokio::test]
    async fn hosted_acceptance_future_review_blocks_terminal_success_but_not_shared_handoff() {
        use crate::jira::tests::{issue, mock_jira, reply, tracker};
        let mock = mock_jira(vec![
            reply("GET", "/rest/api/3/issue/TEST-1", issue("In Progress")),
            reply(
                "GET",
                "/rest/api/3/issue/TEST-1/comment",
                serde_json::json!({"comments":[],"total":0}),
            ),
        ])
        .await;
        let tracker = tracker(&mock.endpoint);
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workgraph.sqlite3");
        let store = SqliteWorkGraphStore::open(&path).unwrap();
        let realm = meerkat_core::mob_realm_id(crate::native_mob_contract::FUGUE_MOB_ID).unwrap();
        let service = WorkGraphService::with_scope(
            Arc::new(store),
            realm.as_str(),
            meerkat::WorkNamespace::default(),
        );
        service
            .create(CreateWorkItemRequest {
                realm_id: None,
                namespace: None,
                title: "Repository review after handoff".into(),
                description: None,
                priority: WorkPriority::High,
                completion_policy: WorkCompletionPolicy::SelfAttest,
                failed_child_join_policy: Default::default(),
                cancelled_child_join_policy: Default::default(),
                labels: BTreeSet::from([
                    FUGUE_WORKGRAPH_LABEL.into(),
                    issue_workgraph_label("TEST-1"),
                    FUGUE_WORKGRAPH_BLOCKING_LABEL.into(),
                ]),
                due_at: None,
                not_before: None,
                snoozed_until: None,
                external_refs: vec![],
                evidence_refs: vec![],
                status: Some(WorkStatus::Open),
            })
            .await
            .unwrap();
        let gate = WorkGraphTerminalGate::new(&path, tracker.terminal_states.clone(), vec![]);
        let mut input = MoveIssueInput {
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("TEST-1".into()),
            },
            state: LinearStateRef::Name {
                name: "Awaiting review".into(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey("handoff".into()),
        };
        guard_public_lane_move(
            &tracker,
            &["Awaiting review".into()],
            &["Deploy".into()],
            &input,
        )
        .unwrap();
        gate.guard_terminal_move(&tracker, &input).await.unwrap();
        input.state = LinearStateRef::Name {
            name: "Done".into(),
        };
        assert!(
            gate.guard_terminal_move(&tracker, &input)
                .await
                .unwrap_err()
                .contains("unresolved Fugue WorkGraph obligation")
        );
        assert_eq!(
            gate.unresolved_blocking_items("TEST-1")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn terminal_gate_allows_blocked_lane_with_open_obligation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("workgraph.sqlite3");
        let (tracker, _) = test_tracker(&temp.path().join("issues.sqlite3"), "LUC-1");
        let gate = WorkGraphTerminalGate::new(
            &path,
            vec!["Done".to_string(), "Blocked".to_string()],
            vec!["Blocked".to_string()],
        );

        gate.guard_terminal_move(
            &tracker,
            &MoveIssueInput {
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier("LUC-1".to_string()),
                },
                state: LinearStateRef::Name {
                    name: "Blocked".to_string(),
                },
                expected_updated_at: None,
                idempotency_key: IdempotencyKey("move-1".to_string()),
            },
        )
        .await
        .expect("blocked is allowed for human intervention");
    }
}
