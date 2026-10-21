//! Repo-level final PR review gate contracts and aggregation.
//!
//! The final PR gate is modeled as Kapellmeister-owned typed data and decisions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

use crate::config::{FinalReviewConfig, FinalReviewMergeStrategy, FinalReviewMode};
use crate::domain::normalize_state;
use crate::tools::{
    CheckCiOutput, CiStatus, FugueToolOperation, GitHubPullRequestRef, GitHubPullRequestSnapshot,
    GitSha, IdempotencyKey, LinearIssueRef, LinearStateRef, MergeMethod, MergePrInput,
    MobEventKind, MobEventSnapshot, MobTemplateId, MoveIssueInput, SpawnOrResumeExecutionInput,
    UpsertWorkpadInput, WorkspaceKey,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalReviewFinding {
    pub severity: String,
    pub title: String,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalReviewVerdictStatus {
    Approve,
    RequestChanges,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalReviewVerdict {
    pub reviewer: String,
    pub status: FinalReviewVerdictStatus,
    pub pr: GitHubPullRequestSnapshot,
    pub head_sha: GitSha,
    #[serde(default)]
    pub findings: Vec<FinalReviewFinding>,
    #[serde(default)]
    pub required_checks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalPrGateCiStatus {
    Success,
    Failed,
    Pending,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalPrGateDecision {
    Merge,
    Rework,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalPrGateResult {
    pub issue: LinearIssueRef,
    pub pr: GitHubPullRequestSnapshot,
    pub reviewed_head_sha: GitSha,
    pub reviewers: Vec<FinalReviewVerdict>,
    pub ci_status: FinalPrGateCiStatus,
    pub decision: FinalPrGateDecision,
    pub target_lane: String,
    #[serde(default)]
    pub findings: Vec<FinalReviewFinding>,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge: Option<MergePrInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateInput {
    pub issue: LinearIssueRef,
    pub config: FinalReviewConfig,
    pub reviewed_pr: GitHubPullRequestSnapshot,
    pub current_pr: GitHubPullRequestSnapshot,
    pub verdicts: Vec<FinalReviewVerdict>,
    pub ci: Option<CheckCiOutput>,
    pub merge_authorized: bool,
    pub mergeable: bool,
    pub extra_blockers: Vec<String>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalPrGateTriggerDecision {
    Disabled,
    Wait { reason: String },
    Start { trigger_mode: FinalReviewMode },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateTriggerInput {
    pub config: FinalReviewConfig,
    pub issue_state: String,
    pub review_ready_lane: Option<String>,
    pub typed_pr_handoff_present: bool,
    pub external_lane_move: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalPrGateStartRequest {
    pub issue: LinearIssueRef,
    pub pr: GitHubPullRequestSnapshot,
    pub trigger_mode: FinalReviewMode,
    pub reviewers: Vec<FinalReviewerMemberSpec>,
    pub spawn_input: SpawnOrResumeExecutionInput,
    pub mob_definition: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateRuntimeTriggerInput {
    pub config: FinalReviewConfig,
    pub issue: LinearIssueRef,
    pub issue_state: String,
    pub review_ready_lane: Option<String>,
    pub typed_pr_handoff_present: bool,
    pub external_lane_move: bool,
    pub pr: GitHubPullRequestSnapshot,
    pub workspace_key: WorkspaceKey,
    pub worktree_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateRuntimePlanInput {
    pub config: FinalReviewConfig,
    pub start_request: FinalPrGateStartRequest,
    pub current_pr: GitHubPullRequestSnapshot,
    pub verdicts: Vec<FinalReviewVerdict>,
    pub ci: Option<CheckCiOutput>,
    pub merge_authorized_by_kapellmeister: bool,
    pub mergeable: bool,
    pub extra_blockers: Vec<String>,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateRuntimePlan {
    pub result: FinalPrGateResult,
    pub projection: FinalPrGateProjectionPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateProjectionPlan {
    pub workpad: UpsertWorkpadInput,
    pub move_issue: MoveIssueInput,
    pub merge: Option<MergePrInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalPrGateCompletionInput {
    pub config: FinalReviewConfig,
    pub start_request: FinalPrGateStartRequest,
    pub current_pr: GitHubPullRequestSnapshot,
    pub ci: Option<CheckCiOutput>,
    pub merge_authorized_by_kapellmeister: bool,
    pub mergeable: bool,
    pub idempotency_key: IdempotencyKey,
    #[allow(dead_code)]
    pub extra_blockers: Vec<String>,
    pub events: Vec<MobEventSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "retain the typed application contract without incidental boxing"
)]
pub enum FinalPrGateCompletionPlan {
    Waiting { reason: String },
    Ready(FinalPrGateRuntimePlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalReviewerMemberSpec {
    pub id: String,
    pub profile: String,
    pub required: bool,
    pub merge_tool_allowed: bool,
}

impl From<FinalReviewMergeStrategy> for MergeMethod {
    fn from(strategy: FinalReviewMergeStrategy) -> Self {
        match strategy {
            FinalReviewMergeStrategy::Merge => Self::Merge,
            FinalReviewMergeStrategy::Squash => Self::Squash,
            FinalReviewMergeStrategy::Rebase => Self::Rebase,
        }
    }
}

pub fn final_pr_gate_trigger_decision(
    input: FinalPrGateTriggerInput,
) -> FinalPrGateTriggerDecision {
    if !input.config.enabled {
        return FinalPrGateTriggerDecision::Disabled;
    }
    let issue_state = normalize_state(&input.issue_state);
    let final_lane = normalize_state(&input.config.lane);
    if input.external_lane_move && issue_state == final_lane {
        return FinalPrGateTriggerDecision::Start {
            trigger_mode: input.config.mode,
        };
    }
    match input.config.mode {
        FinalReviewMode::Manual => FinalPrGateTriggerDecision::Wait {
            reason: "manual mode waits for an external move into the configured final gate lane"
                .to_string(),
        },
        FinalReviewMode::Auto => {
            let review_ready_matches = input
                .review_ready_lane
                .as_deref()
                .is_some_and(|lane| normalize_state(lane) == issue_state);
            if input.typed_pr_handoff_present && review_ready_matches {
                FinalPrGateTriggerDecision::Start {
                    trigger_mode: FinalReviewMode::Auto,
                }
            } else {
                FinalPrGateTriggerDecision::Wait {
                    reason: "auto mode requires typed PR handoff evidence in the configured review-ready lane".to_string(),
                }
            }
        }
    }
}

pub fn final_pr_gate_start_request(
    input: FinalPrGateRuntimeTriggerInput,
) -> Option<FinalPrGateStartRequest> {
    let trigger = final_pr_gate_trigger_decision(FinalPrGateTriggerInput {
        config: input.config.clone(),
        issue_state: input.issue_state,
        review_ready_lane: input.review_ready_lane,
        typed_pr_handoff_present: input.typed_pr_handoff_present,
        external_lane_move: input.external_lane_move,
    });
    let FinalPrGateTriggerDecision::Start { trigger_mode } = trigger else {
        return None;
    };
    let reviewers = final_reviewer_member_specs(&input.config);
    let mob_definition = final_reviewer_mob_definition(&input.config, &input.pr, trigger_mode);
    let template_id = MobTemplateId(format!("final-pr-gate-{}", input.pr.number.0));
    let spawn_input = SpawnOrResumeExecutionInput {
        issue: input.issue.clone(),
        workspace_key: input.workspace_key,
        template_id,
        worktree_path: input.worktree_path,
        instructions: Some(final_pr_gate_reviewer_instructions(
            &input.config,
            &input.issue,
            &input.pr,
            trigger_mode,
        )),
        idempotency_key: IdempotencyKey(format!(
            "final-pr-gate:{}:{}:{}",
            input.pr.repo.name.0, input.pr.number.0, input.pr.head_sha.0
        )),
    };
    Some(FinalPrGateStartRequest {
        issue: input.issue,
        pr: input.pr,
        trigger_mode,
        reviewers,
        spawn_input,
        mob_definition,
    })
}

pub fn final_reviewer_mob_definition(
    config: &FinalReviewConfig,
    pr: &GitHubPullRequestSnapshot,
    trigger_mode: FinalReviewMode,
) -> Value {
    let mut profiles = Map::new();
    let mut members = Vec::new();
    for spec in final_reviewer_member_specs(config) {
        profiles.entry(spec.profile.clone()).or_insert_with(|| {
            json!({
                "peer_description": format!("Repo-level final PR reviewer {} for PR #{}", spec.id, pr.number.0),
                "tools": {
                    "builtins": true,
                    "shell": true,
                    "comms": true,
                    "workgraph": true,
                    "mcp": ["github", "linear"]
                },
                "merge_tool_allowed": false
            })
        });
        members.push(json!({
            "id": spec.id,
            "profile": spec.profile,
            "required": spec.required,
            "merge_tool_allowed": false
        }));
    }
    json!({
        "id": format!("final-pr-gate-{}", pr.number.0),
        "owner": "kapellmeister",
        "trigger_mode": trigger_mode,
        "pr": pr,
        "profiles": profiles,
        "members": members,
        "authority": {
            "reviewers_emit_typed_verdicts_only": true,
            "reviewers_may_merge": false,
            "merge_owner": "kapellmeister"
        }
    })
}

pub fn final_pr_gate_reviewer_instructions(
    config: &FinalReviewConfig,
    issue: &LinearIssueRef,
    pr: &GitHubPullRequestSnapshot,
    trigger_mode: FinalReviewMode,
) -> String {
    format!(
        "Repo-level final PR gate for PR #{} ({}) via {:?}. Review exact head SHA `{}`. Each configured final reviewer must emit a typed FinalReviewVerdict with reviewer id, status, PR, head_sha, findings, and required_checks. Required reviewers: {}. Optional reviewers: {}. Reviewers must not merge. Each reviewer must independently inspect the diff against the PR base using shell/GitHub tools and query WorkGraph obligations using workgraph_* tools; Fugue does not provide a precomputed diff or WorkGraph dump. If you find a blocking issue, create or update WorkGraph items labeled `fugue`, `issue:{}`, and `blocking`; only reviewer-role agents may close completed review obligations. Kapellmeister aggregates typed verdicts, verifies fresh head and CI, then chooses configured lanes success=`{}`, rework=`{}`, blocked=`{}`.",
        pr.number.0,
        pr.url,
        trigger_mode,
        pr.head_sha.0,
        config
            .reviewers
            .iter()
            .filter(|r| r.required)
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
            .join(","),
        config
            .reviewers
            .iter()
            .filter(|r| !r.required)
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>()
            .join(","),
        final_pr_gate_issue_label(issue),
        config.success_lane,
        config.rework_lane,
        config.blocked_lane
    )
}

fn final_pr_gate_issue_label(issue: &LinearIssueRef) -> String {
    match issue {
        LinearIssueRef::Identifier { identifier } => identifier.0.clone(),
        LinearIssueRef::Id { id } => id.0.clone(),
    }
}

pub fn kapellmeister_final_pr_gate_runtime_plan(
    started_at: DateTime<Utc>,
    input: FinalPrGateRuntimePlanInput,
) -> FinalPrGateRuntimePlan {
    let trigger_mode = input.start_request.trigger_mode;
    let merge_authorized = input.merge_authorized_by_kapellmeister;
    let result = aggregate_final_pr_gate(FinalPrGateInput {
        issue: input.start_request.issue,
        config: input.config,
        reviewed_pr: input.start_request.pr,
        current_pr: input.current_pr,
        verdicts: input.verdicts,
        ci: input.ci,
        merge_authorized,
        mergeable: input.mergeable,
        extra_blockers: input.extra_blockers,
        idempotency_key: input.idempotency_key,
    });
    let projection =
        final_pr_gate_projection_plan(started_at, trigger_mode, &result, merge_authorized);
    FinalPrGateRuntimePlan { result, projection }
}

pub fn final_pr_gate_projection_plan(
    started_at: DateTime<Utc>,
    trigger_mode: FinalReviewMode,
    result: &FinalPrGateResult,
    merge_authorized_by_kapellmeister: bool,
) -> FinalPrGateProjectionPlan {
    let body_markdown = final_gate_workpad_evidence(started_at, trigger_mode, result);
    FinalPrGateProjectionPlan {
        workpad: UpsertWorkpadInput {
            issue: result.issue.clone(),
            heading: "## Fugue Workpad".to_string(),
            body_markdown,
            create_if_missing: true,
            idempotency_key: IdempotencyKey(format!(
                "final-pr-gate-workpad:{}:{}",
                result.pr.number.0, result.reviewed_head_sha.0
            )),
        },
        move_issue: MoveIssueInput {
            issue: result.issue.clone(),
            state: LinearStateRef::Name {
                name: result.target_lane.clone(),
            },
            expected_updated_at: None,
            idempotency_key: IdempotencyKey(format!(
                "final-pr-gate-project:{}:{}:{}",
                result.pr.number.0, result.reviewed_head_sha.0, result.target_lane
            )),
        },
        merge: result
            .merge
            .clone()
            .filter(|_| merge_authorized_by_kapellmeister),
    }
}

pub fn final_pr_gate_completion_plan(
    started_at: DateTime<Utc>,
    input: FinalPrGateCompletionInput,
) -> FinalPrGateCompletionPlan {
    let verdicts = collect_final_review_verdicts_from_events(&input.events);
    let required_verdicts_seen = input
        .config
        .reviewers
        .iter()
        .filter(|reviewer| reviewer.required)
        .all(|reviewer| {
            verdicts
                .iter()
                .any(|verdict| verdict.reviewer == reviewer.id)
        });
    let run_completed = input.events.iter().any(|event| {
        matches!(
            event.kind,
            MobEventKind::RunCompleted {
                status: crate::tools::MobRunStatus::Succeeded,
                ..
            }
        )
    });
    if !run_completed && !required_verdicts_seen {
        return FinalPrGateCompletionPlan::Waiting {
            reason: "final PR gate reviewers have not all emitted typed verdicts yet".to_string(),
        };
    }
    FinalPrGateCompletionPlan::Ready(kapellmeister_final_pr_gate_runtime_plan(
        started_at,
        FinalPrGateRuntimePlanInput {
            config: input.config,
            start_request: input.start_request,
            current_pr: input.current_pr,
            verdicts,
            ci: input.ci,
            merge_authorized_by_kapellmeister: input.merge_authorized_by_kapellmeister,
            mergeable: input.mergeable,
            extra_blockers: input.extra_blockers,
            idempotency_key: input.idempotency_key,
        },
    ))
}

pub fn collect_final_review_verdicts_from_events(
    events: &[MobEventSnapshot],
) -> Vec<FinalReviewVerdict> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            MobEventKind::RunCompleted {
                structured_output: Some(value),
                ..
            } => final_review_verdicts_from_value(value),
            _ => None,
        })
        .flatten()
        .collect()
}

fn final_review_verdicts_from_value(value: &Value) -> Option<Vec<FinalReviewVerdict>> {
    if let Ok(verdict) = serde_json::from_value::<FinalReviewVerdict>(value.clone()) {
        return Some(vec![verdict]);
    }
    if let Some(verdicts) = value.get("verdicts") {
        return serde_json::from_value::<Vec<FinalReviewVerdict>>(verdicts.clone()).ok();
    }
    if let Ok(verdicts) = serde_json::from_value::<Vec<FinalReviewVerdict>>(value.clone()) {
        return Some(verdicts);
    }
    None
}

pub fn final_reviewer_member_specs(config: &FinalReviewConfig) -> Vec<FinalReviewerMemberSpec> {
    config
        .reviewers
        .iter()
        .map(|reviewer| FinalReviewerMemberSpec {
            id: reviewer.id.clone(),
            profile: reviewer.profile.clone(),
            required: reviewer.required,
            merge_tool_allowed: false,
        })
        .collect()
}

pub fn final_reviewer_merge_tool_violations(
    config: &FinalReviewConfig,
    profile_tools: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let merge_tool_names = [
        FugueToolOperation::GitHubMergePr.as_str(),
        FugueToolOperation::GitHubMergePr.provider_callback_name(),
    ];
    config
        .reviewers
        .iter()
        .filter_map(|reviewer| {
            let tools = profile_tools.get(&reviewer.profile)?;
            tools
                .iter()
                .any(|tool| merge_tool_names.iter().any(|merge| tool == merge))
                .then_some(reviewer.profile.clone())
        })
        .collect()
}

pub fn aggregate_final_pr_gate(input: FinalPrGateInput) -> FinalPrGateResult {
    let ci_status = final_pr_gate_ci_status(input.ci.as_ref());
    let reviewed_head_sha = input.reviewed_pr.head_sha.clone();
    let mut findings = Vec::new();
    let mut blockers = input.extra_blockers.clone();
    let verdicts_by_reviewer = input
        .verdicts
        .iter()
        .map(|v| (v.reviewer.as_str(), v))
        .collect::<BTreeMap<_, _>>();

    if !input.config.enabled {
        blockers.push("final_review.enabled is false".to_string());
    }
    let required_reviewers = input
        .config
        .reviewers
        .iter()
        .filter(|r| r.required)
        .map(|r| r.id.as_str())
        .collect::<BTreeSet<_>>();

    for reviewer in &input.config.reviewers {
        match verdicts_by_reviewer.get(reviewer.id.as_str()) {
            Some(verdict) => {
                findings.extend(verdict.findings.clone());
                if verdict.head_sha != reviewed_head_sha {
                    blockers.push(format!(
                        "reviewer {} returned verdict for SHA {} but reviewed SHA is {}",
                        reviewer.id, verdict.head_sha.0, reviewed_head_sha.0
                    ));
                }
                if verdict.pr.number != input.reviewed_pr.number
                    || verdict.pr.repo != input.reviewed_pr.repo
                {
                    blockers.push(format!(
                        "reviewer {} returned verdict for a different PR",
                        reviewer.id
                    ));
                }
            }
            None if reviewer.required => blockers.push(format!(
                "missing required final reviewer verdict from {}",
                reviewer.id
            )),
            None => {}
        }
    }

    let required_request_changes = input.verdicts.iter().any(|v| {
        required_reviewers.contains(v.reviewer.as_str())
            && v.status == FinalReviewVerdictStatus::RequestChanges
    });
    let required_blocked = input.verdicts.iter().any(|v| {
        required_reviewers.contains(v.reviewer.as_str())
            && v.status == FinalReviewVerdictStatus::Blocked
    });
    let all_required_approved = required_reviewers.iter().all(|reviewer| {
        verdicts_by_reviewer
            .get(reviewer)
            .is_some_and(|v| v.status == FinalReviewVerdictStatus::Approve)
    });

    if required_blocked {
        blockers.push("a required final reviewer returned Blocked".to_string());
    }
    if input.config.merge.require_fresh_head && input.current_pr.head_sha != reviewed_head_sha {
        blockers.push(format!(
            "PR head changed from reviewed SHA {} to current SHA {}",
            reviewed_head_sha.0, input.current_pr.head_sha.0
        ));
    }
    if input.config.merge.require_ci_success && ci_status != FinalPrGateCiStatus::Success {
        blockers.push(format!(
            "CI/check status for reviewed SHA is {:?}, not Success",
            ci_status
        ));
    }
    if !input.mergeable {
        blockers.push("GitHub reports the PR is not mergeable".to_string());
    }

    let decision = if required_request_changes {
        FinalPrGateDecision::Rework
    } else if blockers.is_empty() && all_required_approved {
        FinalPrGateDecision::Merge
    } else {
        FinalPrGateDecision::Blocked
    };
    let target_lane = match decision {
        FinalPrGateDecision::Merge => input.config.success_lane.clone(),
        FinalPrGateDecision::Rework => input.config.rework_lane.clone(),
        FinalPrGateDecision::Blocked => input.config.blocked_lane.clone(),
    };
    let merge =
        (decision == FinalPrGateDecision::Merge && input.merge_authorized).then(|| MergePrInput {
            repo: input.reviewed_pr.repo.clone(),
            pull_request: GitHubPullRequestRef::Number {
                number: input.reviewed_pr.number,
            },
            method: input.config.merge.strategy.into(),
            expected_head_sha: Some(reviewed_head_sha.clone()),
            idempotency_key: input.idempotency_key,
        });

    FinalPrGateResult {
        issue: input.issue,
        pr: input.current_pr,
        reviewed_head_sha,
        reviewers: input.verdicts,
        ci_status,
        decision,
        target_lane,
        findings,
        blockers,
        merge,
    }
}

pub fn final_pr_gate_ci_status(ci: Option<&CheckCiOutput>) -> FinalPrGateCiStatus {
    let Some(ci) = ci else {
        return FinalPrGateCiStatus::Unknown;
    };
    if ci.checks.is_empty() {
        return FinalPrGateCiStatus::Pending;
    }
    match ci.status {
        CiStatus::Success | CiStatus::Skipped => FinalPrGateCiStatus::Success,
        CiStatus::Pending => FinalPrGateCiStatus::Pending,
        CiStatus::Failure | CiStatus::Error | CiStatus::Cancelled => FinalPrGateCiStatus::Failed,
    }
}

pub fn final_gate_workpad_evidence(
    started_at: DateTime<Utc>,
    trigger_mode: FinalReviewMode,
    result: &FinalPrGateResult,
) -> String {
    let mut lines = vec![
        "### Final PR Review Gate".to_string(),
        format!(
            "- [x] Final gate started at {} via {:?} trigger.",
            started_at.to_rfc3339(),
            trigger_mode
        ),
        format!(
            "- [x] PR #{} under review: {}; reviewed head {}.",
            result.pr.number.0, result.pr.url, result.reviewed_head_sha.0
        ),
        format!(
            "- [x] CI/check evidence for reviewed head: {:?}.",
            result.ci_status
        ),
        format!("- [x] Typed gate decision: {:?}.", result.decision),
    ];
    for verdict in &result.reviewers {
        lines.push(format!(
            "- [x] Reviewer {} verdict: {:?} for {} with {} finding(s).",
            verdict.reviewer,
            verdict.status,
            verdict.head_sha.0,
            verdict.findings.len()
        ));
    }
    for finding in &result.findings {
        lines.push(format!(
            "- [ ] Finding {} / {}: {}",
            finding.severity, finding.title, finding.detail
        ));
    }
    for blocker in &result.blockers {
        lines.push(format!("- [ ] Blocker: {blocker}."));
    }
    if result.merge.is_some() {
        lines.push("- [x] Merge authorized for the reviewed SHA; merge evidence must be recorded after GitHub merge returns.".to_string());
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FinalReviewMergeConfig, FinalReviewReviewerConfig};
    use crate::tools::{
        BranchName, GitHubCheckSnapshot, GitHubOwner, GitHubPullRequestNumber, GitHubRepoName,
        GitHubRepoRef, LinearIssueIdentifier, PullRequestState,
    };

    fn repo() -> GitHubRepoRef {
        GitHubRepoRef {
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }
    }
    fn pr(sha: &str) -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: repo(),
            number: GitHubPullRequestNumber(623),
            title: "LUC-623".to_string(),
            url: "https://github.example/pr/623".to_string(),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName("luka/luc-623".to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha(sha.to_string()),
        }
    }
    fn issue() -> LinearIssueRef {
        LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier("LUC-623".to_string()),
        }
    }
    fn config() -> FinalReviewConfig {
        FinalReviewConfig {
            enabled: true,
            mode: FinalReviewMode::Manual,
            lane: "Custom Final Gate".to_string(),
            rework_lane: "Custom Rework".to_string(),
            blocked_lane: "Custom Blocked".to_string(),
            success_lane: "Custom Done".to_string(),
            merge: FinalReviewMergeConfig::default(),
            reviewers: vec![
                FinalReviewReviewerConfig {
                    id: "architecture".to_string(),
                    profile: "final_architecture_reviewer".to_string(),
                    required: true,
                },
                FinalReviewReviewerConfig {
                    id: "tests".to_string(),
                    profile: "final_test_reviewer".to_string(),
                    required: true,
                },
                FinalReviewReviewerConfig {
                    id: "security".to_string(),
                    profile: "final_security_reviewer".to_string(),
                    required: false,
                },
            ],
        }
    }
    fn verdict(reviewer: &str, status: FinalReviewVerdictStatus, sha: &str) -> FinalReviewVerdict {
        FinalReviewVerdict {
            reviewer: reviewer.to_string(),
            status,
            pr: pr(sha),
            head_sha: GitSha(sha.to_string()),
            findings: Vec::new(),
            required_checks: Vec::new(),
        }
    }
    fn ci(status: CiStatus) -> CheckCiOutput {
        CheckCiOutput {
            status,
            checks: vec![GitHubCheckSnapshot {
                name: "required-ci".to_string(),
                status,
                details_url: Some("https://github.example/checks/required-ci".to_string()),
                completed_at: (status != CiStatus::Pending).then(Utc::now),
            }],
        }
    }
    fn gate(verdicts: Vec<FinalReviewVerdict>) -> FinalPrGateInput {
        FinalPrGateInput {
            issue: issue(),
            config: config(),
            reviewed_pr: pr("sha-a"),
            current_pr: pr("sha-a"),
            verdicts,
            ci: Some(ci(CiStatus::Success)),
            merge_authorized: true,
            mergeable: true,
            idempotency_key: IdempotencyKey("final-gate:LUC-623:623:sha-a".to_string()),
            extra_blockers: Vec::new(),
        }
    }

    #[test]
    fn manual_trigger_starts_only_on_configured_lane_move() {
        let cfg = config();
        assert_eq!(
            final_pr_gate_trigger_decision(FinalPrGateTriggerInput {
                config: cfg.clone(),
                issue_state: "Custom Final Gate".to_string(),
                review_ready_lane: Some("Custom Review Ready".to_string()),
                typed_pr_handoff_present: false,
                external_lane_move: true
            }),
            FinalPrGateTriggerDecision::Start {
                trigger_mode: FinalReviewMode::Manual
            }
        );
        assert!(matches!(
            final_pr_gate_trigger_decision(FinalPrGateTriggerInput {
                config: cfg,
                issue_state: "Custom Review Ready".to_string(),
                review_ready_lane: Some("Custom Review Ready".to_string()),
                typed_pr_handoff_present: true,
                external_lane_move: false
            }),
            FinalPrGateTriggerDecision::Wait { .. }
        ));
    }

    #[test]
    fn auto_trigger_uses_same_start_decision_after_handoff() {
        let mut cfg = config();
        cfg.mode = FinalReviewMode::Auto;
        assert_eq!(
            final_pr_gate_trigger_decision(FinalPrGateTriggerInput {
                config: cfg,
                issue_state: "Custom Review Ready".to_string(),
                review_ready_lane: Some("Custom Review Ready".to_string()),
                typed_pr_handoff_present: true,
                external_lane_move: false
            }),
            FinalPrGateTriggerDecision::Start {
                trigger_mode: FinalReviewMode::Auto
            }
        );
    }

    #[test]
    fn approvals_with_empty_checks_and_required_ci_block_without_merge() {
        let mut input = gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]);
        input.ci = Some(CheckCiOutput {
            status: CiStatus::Pending,
            checks: Vec::new(),
        });
        let result = aggregate_final_pr_gate(input);

        assert_eq!(result.decision, FinalPrGateDecision::Blocked);
        assert!(result.merge.is_none());
        assert!(
            result
                .blockers
                .iter()
                .any(|blocker| blocker.contains("CI/check status"))
        );
    }

    #[test]
    fn final_gate_never_treats_empty_check_evidence_as_green_ci() {
        let empty_success_ci = CheckCiOutput {
            status: CiStatus::Success,
            checks: Vec::new(),
        };
        assert_eq!(
            final_pr_gate_ci_status(Some(&empty_success_ci)),
            FinalPrGateCiStatus::Pending
        );

        let mut input = gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]);
        input.ci = Some(empty_success_ci);

        let result = aggregate_final_pr_gate(input);

        assert_eq!(result.ci_status, FinalPrGateCiStatus::Pending);
        assert_eq!(result.decision, FinalPrGateDecision::Blocked);
        assert!(result.merge.is_none());
        assert!(
            result
                .blockers
                .iter()
                .any(|blocker| blocker.contains("CI/check status"))
        );
    }

    #[test]
    fn approvals_with_empty_checks_can_merge_when_required_ci_disabled() {
        let mut cfg = config();
        cfg.merge.require_ci_success = false;
        let result = aggregate_final_pr_gate(FinalPrGateInput {
            issue: issue(),
            config: cfg,
            reviewed_pr: pr("sha-a"),
            current_pr: pr("sha-a"),
            verdicts: vec![
                verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
                verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
            ],
            ci: Some(CheckCiOutput {
                status: CiStatus::Pending,
                checks: Vec::new(),
            }),
            merge_authorized: true,
            mergeable: true,
            extra_blockers: Vec::new(),
            idempotency_key: IdempotencyKey("final-gate:LUC-623:623:sha-a".to_string()),
        });

        assert_eq!(result.decision, FinalPrGateDecision::Merge);
        assert!(result.merge.is_some());
    }

    #[test]
    fn approve_green_ci_fresh_sha_builds_kapellmeister_merge_plan() {
        let result = aggregate_final_pr_gate(gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]));
        assert_eq!(result.decision, FinalPrGateDecision::Merge);
        assert_eq!(result.target_lane, "Custom Done");
        let merge = result.merge.expect("merge input");
        assert_eq!(merge.method, MergeMethod::Squash);
        assert_eq!(merge.expected_head_sha, Some(GitSha("sha-a".to_string())));
    }

    #[test]
    fn required_request_changes_routes_to_configured_rework_lane() {
        let mut change = verdict("tests", FinalReviewVerdictStatus::RequestChanges, "sha-a");
        change.findings.push(FinalReviewFinding {
            severity: "major".to_string(),
            title: "Missing regression".to_string(),
            detail: "Add SHA invariant test".to_string(),
        });
        let result = aggregate_final_pr_gate(gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            change,
        ]));
        assert_eq!(result.decision, FinalPrGateDecision::Rework);
        assert_eq!(result.target_lane, "Custom Rework");
        assert_eq!(result.findings.len(), 1);
        assert!(result.merge.is_none());
    }

    #[test]
    fn changed_head_invalidates_approvals_and_blocks() {
        let mut input = gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]);
        input.current_pr = pr("sha-b");
        let result = aggregate_final_pr_gate(input);
        assert_eq!(result.decision, FinalPrGateDecision::Blocked);
        assert_eq!(result.target_lane, "Custom Blocked");
        assert!(
            result
                .blockers
                .iter()
                .any(|b| b.contains("PR head changed"))
        );
    }

    #[test]
    fn missing_required_verdict_or_ci_or_authority_blocks_but_optional_absence_does_not() {
        let result = aggregate_final_pr_gate(gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]));
        assert_eq!(result.decision, FinalPrGateDecision::Merge);

        let mut missing = gate(vec![verdict(
            "architecture",
            FinalReviewVerdictStatus::Approve,
            "sha-a",
        )]);
        missing.ci = Some(ci(CiStatus::Pending));
        missing.merge_authorized = false;
        let blocked = aggregate_final_pr_gate(missing);
        assert_eq!(blocked.decision, FinalPrGateDecision::Blocked);
        assert!(
            blocked
                .blockers
                .iter()
                .any(|blocker| blocker.contains("missing required"))
        );

        let mut no_merge_authority = gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]);
        no_merge_authority.merge_authorized = false;
        let done = aggregate_final_pr_gate(no_merge_authority);
        assert_eq!(done.decision, FinalPrGateDecision::Merge);
        assert_eq!(done.target_lane, "Custom Done");
        assert!(done.merge.is_none());
    }

    #[test]
    fn runtime_start_request_materializes_meta_visible_reviewers_from_manual_and_auto_triggers() {
        let cfg = config();
        let manual = final_pr_gate_start_request(FinalPrGateRuntimeTriggerInput {
            config: cfg.clone(),
            issue: issue(),
            issue_state: "Custom Final Gate".to_string(),
            review_ready_lane: Some("Custom Review Ready".to_string()),
            typed_pr_handoff_present: false,
            external_lane_move: true,
            pr: pr("sha-a"),
            workspace_key: WorkspaceKey("LUC-623".to_string()),
            worktree_path: Some("/tmp/LUC-623".to_string()),
        })
        .expect("manual start");
        assert_eq!(manual.trigger_mode, FinalReviewMode::Manual);
        assert_eq!(manual.reviewers.len(), 3);
        assert!(
            manual
                .reviewers
                .iter()
                .all(|reviewer| !reviewer.merge_tool_allowed)
        );
        assert!(
            manual
                .spawn_input
                .instructions
                .as_deref()
                .unwrap()
                .contains("FinalReviewVerdict")
        );
        assert!(
            manual
                .mob_definition
                .to_string()
                .contains("final_architecture_reviewer")
        );

        let mut auto_cfg = cfg;
        auto_cfg.mode = FinalReviewMode::Auto;
        let auto = final_pr_gate_start_request(FinalPrGateRuntimeTriggerInput {
            config: auto_cfg,
            issue: issue(),
            issue_state: "Custom Review Ready".to_string(),
            review_ready_lane: Some("Custom Review Ready".to_string()),
            typed_pr_handoff_present: true,
            external_lane_move: false,
            pr: pr("sha-a"),
            workspace_key: WorkspaceKey("LUC-623".to_string()),
            worktree_path: None,
        })
        .expect("auto start");
        assert_eq!(auto.trigger_mode, FinalReviewMode::Auto);
        assert_eq!(
            auto.spawn_input.idempotency_key,
            manual.spawn_input.idempotency_key
        );
    }

    fn start_request() -> FinalPrGateStartRequest {
        final_pr_gate_start_request(FinalPrGateRuntimeTriggerInput {
            config: config(),
            issue: issue(),
            issue_state: "Custom Final Gate".to_string(),
            review_ready_lane: Some("Custom Review Ready".to_string()),
            typed_pr_handoff_present: false,
            external_lane_move: true,
            pr: pr("sha-a"),
            workspace_key: WorkspaceKey("LUC-623".to_string()),
            worktree_path: Some("/tmp/LUC-623".to_string()),
        })
        .expect("start request")
    }

    fn completed_event(value: Value) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc::now(),
            kind: MobEventKind::RunCompleted {
                status: crate::tools::MobRunStatus::Succeeded,
                structured_output: Some(value),
            },
            message: None,
        }
    }

    fn completion_plan(verdicts: Vec<FinalReviewVerdict>) -> FinalPrGateCompletionPlan {
        final_pr_gate_completion_plan(
            Utc::now(),
            FinalPrGateCompletionInput {
                config: config(),
                start_request: start_request(),
                current_pr: pr("sha-a"),
                ci: Some(ci(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate:LUC-623:623:sha-a".to_string()),
                extra_blockers: Vec::new(),
                events: vec![completed_event(json!({ "verdicts": verdicts }))],
            },
        )
    }

    #[test]
    fn completion_plan_routes_required_request_changes_to_rework_from_typed_verdicts() {
        let mut changes = verdict("tests", FinalReviewVerdictStatus::RequestChanges, "sha-a");
        changes.findings.push(FinalReviewFinding {
            severity: "major".to_string(),
            title: "Fix tests".to_string(),
            detail: "Add missing coverage".to_string(),
        });

        let FinalPrGateCompletionPlan::Ready(plan) = completion_plan(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            changes,
        ]) else {
            panic!("completion plan should be ready");
        };

        assert_eq!(plan.result.decision, FinalPrGateDecision::Rework);
        assert_eq!(
            plan.projection.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Rework".to_string()
            }
        );
        assert!(plan.projection.merge.is_none());
        assert!(plan.projection.workpad.body_markdown.contains("Fix tests"));
    }

    #[test]
    fn completion_plan_routes_missing_or_stale_evidence_to_blocked() {
        let FinalPrGateCompletionPlan::Ready(missing) = completion_plan(vec![verdict(
            "architecture",
            FinalReviewVerdictStatus::Approve,
            "sha-a",
        )]) else {
            panic!("completion plan should be ready after run completion");
        };
        assert_eq!(missing.result.decision, FinalPrGateDecision::Blocked);
        assert_eq!(
            missing.projection.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Blocked".to_string()
            }
        );
        assert!(
            missing
                .result
                .blockers
                .iter()
                .any(|blocker| blocker.contains("missing required"))
        );

        let stale = final_pr_gate_completion_plan(
            Utc::now(),
            FinalPrGateCompletionInput {
                config: config(),
                start_request: start_request(),
                current_pr: pr("sha-b"),
                ci: Some(ci(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate:LUC-623:623:sha-a".to_string()),
                extra_blockers: Vec::new(),
                events: vec![completed_event(json!({ "verdicts": vec![
                    verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
                    verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
                ] }))],
            },
        );
        let FinalPrGateCompletionPlan::Ready(stale) = stale else {
            panic!("stale completion plan should be ready");
        };
        assert_eq!(stale.result.decision, FinalPrGateDecision::Blocked);
        assert!(
            stale
                .result
                .blockers
                .iter()
                .any(|blocker| blocker.contains("PR head changed"))
        );
    }

    #[test]
    fn completion_plan_all_required_approvals_produces_guarded_merge_and_success_projection() {
        let FinalPrGateCompletionPlan::Ready(plan) = completion_plan(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]) else {
            panic!("completion plan should be ready");
        };

        assert_eq!(plan.result.decision, FinalPrGateDecision::Merge);
        assert_eq!(
            plan.projection.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Done".to_string()
            }
        );
        let merge = plan.projection.merge.expect("guarded merge input");
        assert_eq!(merge.expected_head_sha, Some(GitSha("sha-a".to_string())));
    }

    #[test]
    fn completion_plan_optional_absence_and_workpad_prose_do_not_drive_authority() {
        let FinalPrGateCompletionPlan::Ready(plan) = completion_plan(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]) else {
            panic!("completion plan should be ready without optional reviewer");
        };
        assert_eq!(plan.result.decision, FinalPrGateDecision::Merge);

        let prose_only = final_pr_gate_completion_plan(
            Utc::now(),
            FinalPrGateCompletionInput {
                config: config(),
                start_request: start_request(),
                current_pr: pr("sha-a"),
                ci: Some(ci(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate:LUC-623:623:sha-a".to_string()),
                extra_blockers: Vec::new(),
                events: vec![completed_event(
                    json!({ "workpad": "architecture approve; tests approve" }),
                )],
            },
        );
        let FinalPrGateCompletionPlan::Ready(prose_only) = prose_only else {
            panic!("completed prose-only run should aggregate to blocker, not wait forever");
        };
        assert_eq!(prose_only.result.decision, FinalPrGateDecision::Blocked);
        assert!(prose_only.result.reviewers.is_empty());
    }

    #[test]
    fn projection_plan_routes_typed_result_to_configured_lane_and_evidence_without_unowned_merge() {
        let result = aggregate_final_pr_gate(gate(vec![
            verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
            verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
        ]));
        let unauthorized =
            final_pr_gate_projection_plan(Utc::now(), FinalReviewMode::Manual, &result, false);
        assert_eq!(
            unauthorized.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Done".to_string()
            }
        );
        assert!(
            unauthorized
                .workpad
                .body_markdown
                .contains("Typed gate decision: Merge")
        );
        assert!(unauthorized.merge.is_none());

        let authorized =
            final_pr_gate_projection_plan(Utc::now(), FinalReviewMode::Manual, &result, true);
        assert!(authorized.merge.is_some());
    }

    #[test]
    fn kapellmeister_runtime_plan_consumes_start_request_verdicts_and_projects_non_default_lanes() {
        let start = final_pr_gate_start_request(FinalPrGateRuntimeTriggerInput {
            config: config(),
            issue: issue(),
            issue_state: "Custom Final Gate".to_string(),
            review_ready_lane: Some("Custom Review Ready".to_string()),
            typed_pr_handoff_present: false,
            external_lane_move: true,
            pr: pr("sha-a"),
            workspace_key: WorkspaceKey("LUC-623".to_string()),
            worktree_path: None,
        })
        .expect("start request");
        let plan = kapellmeister_final_pr_gate_runtime_plan(
            Utc::now(),
            FinalPrGateRuntimePlanInput {
                config: config(),
                start_request: start,
                current_pr: pr("sha-a"),
                verdicts: vec![
                    verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
                    verdict("tests", FinalReviewVerdictStatus::Approve, "sha-a"),
                ],
                ci: Some(ci(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate-runtime:LUC-623:sha-a".to_string()),
                extra_blockers: Vec::new(),
            },
        );

        assert_eq!(plan.result.decision, FinalPrGateDecision::Merge);
        assert_eq!(plan.result.reviewed_head_sha, GitSha("sha-a".to_string()));
        assert_eq!(
            plan.projection.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Done".to_string()
            }
        );
        assert!(plan.projection.merge.is_some());
        assert!(
            plan.projection
                .workpad
                .body_markdown
                .contains("Typed gate decision: Merge")
        );
        assert!(
            plan.projection
                .workpad
                .body_markdown
                .contains("Reviewer architecture verdict: Approve")
        );
    }

    #[test]
    fn kapellmeister_runtime_plan_routes_request_changes_to_non_default_rework_without_merge() {
        let start = final_pr_gate_start_request(FinalPrGateRuntimeTriggerInput {
            config: config(),
            issue: issue(),
            issue_state: "Custom Final Gate".to_string(),
            review_ready_lane: Some("Custom Review Ready".to_string()),
            typed_pr_handoff_present: false,
            external_lane_move: true,
            pr: pr("sha-a"),
            workspace_key: WorkspaceKey("LUC-623".to_string()),
            worktree_path: None,
        })
        .expect("start request");
        let mut tests = verdict("tests", FinalReviewVerdictStatus::RequestChanges, "sha-a");
        tests.findings.push(FinalReviewFinding {
            severity: "major".to_string(),
            title: "Runtime gap".to_string(),
            detail: "Consume typed verdicts before projection".to_string(),
        });
        let plan = kapellmeister_final_pr_gate_runtime_plan(
            Utc::now(),
            FinalPrGateRuntimePlanInput {
                config: config(),
                start_request: start,
                current_pr: pr("sha-a"),
                verdicts: vec![
                    verdict("architecture", FinalReviewVerdictStatus::Approve, "sha-a"),
                    tests,
                ],
                ci: Some(ci(CiStatus::Success)),
                merge_authorized_by_kapellmeister: true,
                mergeable: true,
                idempotency_key: IdempotencyKey("final-gate-runtime:LUC-623:sha-a".to_string()),
                extra_blockers: Vec::new(),
            },
        );

        assert_eq!(plan.result.decision, FinalPrGateDecision::Rework);
        assert_eq!(
            plan.projection.move_issue.state,
            LinearStateRef::Name {
                name: "Custom Rework".to_string()
            }
        );
        assert!(plan.projection.merge.is_none());
        assert!(
            plan.projection
                .workpad
                .body_markdown
                .contains("Runtime gap")
        );
    }

    #[test]
    fn final_reviewers_are_meta_members_without_merge_tool() {
        let cfg = config();
        let specs = final_reviewer_member_specs(&cfg);
        assert_eq!(specs.len(), 3);
        assert!(specs.iter().all(|spec| !spec.merge_tool_allowed));
        let profiles = BTreeMap::from([
            (
                "final_architecture_reviewer".to_string(),
                vec![
                    "github_inspect_pr".to_string(),
                    "github_merge_pr".to_string(),
                ],
            ),
            (
                "final_test_reviewer".to_string(),
                vec!["github_check_ci".to_string()],
            ),
        ]);
        assert_eq!(
            final_reviewer_merge_tool_violations(&cfg, &profiles),
            vec!["final_architecture_reviewer".to_string()]
        );
    }
}
