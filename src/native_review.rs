//! Review evidence read from native flow custody. This module does not accept
//! HTTP verdicts, transient turn results or WorkGraph prose as approval proof.

use std::collections::{BTreeMap, BTreeSet};

use indexmap::IndexMap;
use meerkat_mob::AgentIdentity;
use meerkat_mob::definition::{
    CollectionPolicy, DependencyMode, DispatchMode, FlowNodeFailurePolicy, FlowSpec, FlowStepSpec,
    MobDefinition, StepOutputFormat,
};
use meerkat_mob::{
    FlowId, FlowRoleTargets, FlowRunConfig, MobEvent, MobEventKind, MobRun, MobRunStatus,
    ProfileName, RunId, StepId, StepRunStatus,
};
use serde::{Deserialize, Serialize};

use crate::independent_review::{IndependentReviewVerdict, IndependentReviewVerdictStatus};
use crate::tools::{BranchName, GitHubPullRequestNumber, GitHubRepoRef, GitSha, LinearIssueId};

pub(crate) const SCHEMA: &str = "fugue.native-review.v1";
const STEP: &str = "review";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewCandidate {
    pub tracker_context: String,
    pub issue_id: LinearIssueId,
    pub issue_identifier: String,
    pub repo: GitHubRepoRef,
    pub pull_request: GitHubPullRequestNumber,
    pub base_branch: BranchName,
    pub head_sha: GitSha,
}

/// Persisted unchanged in every flow participating in one review round.
/// The repository authority chooses the reviewers; the host only verifies them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewIntent {
    pub schema_version: String,
    pub round: String,
    pub candidate: ReviewCandidate,
    pub reviewers: FlowRoleTargets,
    pub task: String,
}

/// Both entry points use the same native review protocol. Final-review profile
/// requirements select existing specialists; they never create replacement roles.
pub(crate) fn final_review_count(
    config: &crate::config::FinalReviewConfig,
    reviewers: &FlowRoleTargets,
) -> Result<usize, ReviewProofError> {
    if !config.enabled {
        return Err(ReviewProofError::Unavailable);
    }
    let count = reviewers
        .bound_roles()
        .map(|(_, members)| members.len())
        .sum::<usize>();
    if config.minimum_reviewers == 0
        || !(usize::from(config.minimum_reviewers)..=usize::from(u8::MAX)).contains(&count)
    {
        return Err(ReviewProofError::ReviewerCount);
    }
    let mut required = BTreeMap::<&str, usize>::new();
    for reviewer in config.reviewers.iter().filter(|reviewer| reviewer.required) {
        *required.entry(&reviewer.profile).or_default() += 1;
    }
    if required.is_empty()
        || required.into_iter().any(|(profile, minimum)| {
            reviewers
                .bound_roles()
                .find(|(role, _)| role.as_str() == profile)
                .is_none_or(|(_, members)| members.len() < minimum)
        })
    {
        return Err(ReviewProofError::InvalidIntent);
    }
    Ok(count)
}

pub(crate) fn configured_review_count(
    independent: &crate::config::IndependentReviewConfig,
    final_review: &crate::config::FinalReviewConfig,
    reviewers: &FlowRoleTargets,
) -> Result<usize, ReviewProofError> {
    let count = reviewers
        .bound_roles()
        .map(|(_, members)| members.len())
        .sum::<usize>();
    if independent.enabled && count == usize::from(independent.reviewer_count) {
        return Ok(count);
    }
    if independent.enabled && !final_review.enabled {
        return Err(ReviewProofError::ReviewerCount);
    }
    final_review_count(final_review, reviewers)
}

impl ReviewIntent {
    pub fn validate(&self, required_reviewers: usize) -> Result<(), ReviewProofError> {
        if self.schema_version != SCHEMA
            || self.round.trim().is_empty()
            || self.task.trim().is_empty()
            || self.candidate.tracker_context.trim().is_empty()
            || self.candidate.issue_identifier.trim().is_empty()
            || self.candidate.issue_id.0.trim().is_empty()
            || self.candidate.repo.owner.0.trim().is_empty()
            || self.candidate.repo.name.0.trim().is_empty()
            || self.candidate.pull_request.0 == 0
            || self.candidate.base_branch.0.trim().is_empty()
            || !matches!(self.candidate.head_sha.0.len(), 40 | 64)
            || !self
                .candidate
                .head_sha
                .0
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(ReviewProofError::InvalidIntent);
        }
        let mut identities = BTreeSet::new();
        for (_, members) in self.reviewers.bound_roles() {
            for identity in members {
                if !identities.insert(identity) {
                    return Err(ReviewProofError::DuplicateReviewer);
                }
            }
        }
        if required_reviewers == 0 || identities.len() != required_reviewers {
            return Err(ReviewProofError::ReviewerCount);
        }
        Ok(())
    }
}

pub fn flow_id(role: &ProfileName) -> FlowId {
    format!("fugue-review-{role}").into()
}

/// One bounded native flow per existing profile. All selected identities of the
/// profile must finish. Different profiles retain their original capabilities.
pub fn flow(role: &ProfileName) -> FlowSpec {
    FlowSpec::new(
        Some("Independent review by explicitly selected repository members".into()),
        IndexMap::from([(
            STEP.into(),
            FlowStepSpec {
                role: role.clone(),
                message: format!(
                    "Review role: {role}\n{}",
                    concat!(
                        "[INDEPENDENT REVIEW]\n",
                        "Repository: {{ params.candidate.repo }} (an omitted host means github.com)\n",
                        "Pull request: {{ params.candidate.pull_request }}\n",
                        "Immutable head: {{ params.candidate.head_sha }}\n",
                        "Required base branch: {{ params.candidate.base_branch }}\n",
                        "Issue: {{ params.candidate.issue_identifier }} ({{ params.candidate.issue_id }})\n",
                        "Tracker scope: {{ params.candidate.tracker_context }}\n",
                        "Review round: {{ params.round }}\n",
                        "Additional focus: {{ params.task }}\n",
                        "Review only the immutable candidate in this assignment. Do not modify it, ",
                        "merge, deploy, or recruit another reviewer. Return a JSON object with ",
                        "schema_version 'fugue.review.v1', reviewer_id equal to your native agent ",
                        "identity, focus, verdict ('green', 'red', or 'blocked'), summary, findings ",
                        "and evidence {base, head_sha, commands, checked_paths}. Set evidence.base ",
                        "to the exact required base branch name. Green requires no ",
                        "findings. Red requires findings with severity, title and detail. If evidence ",
                        "is incomplete, return blocked. Your native flow records the final result; ",
                        "do not rely on a peer message as a completion receipt."
                    )
                ).into(),
                depends_on: Vec::new(),
                dispatch_mode: DispatchMode::FanOut,
                collection_policy: CollectionPolicy::All,
                condition: None,
                timeout_ms: Some(2 * 60 * 60 * 1000),
                expected_schema_ref: None,
                branch: None,
                depends_on_mode: DependencyMode::All,
                allowed_tools: None,
                blocked_tools: None,
                failure_policy: FlowNodeFailurePolicy::Escalate,
                output_format: Some(StepOutputFormat::Json),
            },
        )]),
        None,
    )
}

/// Obtained by reading a native handle, never deserialized from a tool request.
struct NativeRunEvidence<'a> {
    pub run: &'a MobRun,
    pub events: &'a [MobEvent],
}

/// References locate native records. They carry no verdict or approval claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewReference {
    pub runs: Vec<RunId>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReviewAdmissionError {
    #[error(transparent)]
    Invalid(#[from] ReviewProofError),
    #[error("review round already names a different assignment for flow {flow_id} (run {run_id})")]
    Conflict { run_id: RunId, flow_id: FlowId },
    #[error("native review admission failed: {0}")]
    Runtime(#[source] meerkat_mob::MobError),
}

/// Admit the repository authority's selected reviewers without provisioning
/// members or sequencing their work. Each profile's admission is atomic and
/// keyed by the issue and round. A partial response can be retried with the
/// unchanged intent; native custody returns the already admitted runs.
///
/// A terminal or recovery-canceled run is never restarted by replay. The
/// repository authority must choose a new round for another review attempt.
pub async fn admit(
    handle: &meerkat_mob::MobHandle,
    intent: &ReviewIntent,
    required_reviewers: usize,
) -> Result<ReviewReference, ReviewAdmissionError> {
    admit_with_observer(handle, intent, required_reviewers, None).await
}

pub(crate) async fn admit_observed(
    handle: &meerkat_mob::MobHandle,
    intent: &ReviewIntent,
    required_reviewers: usize,
    observer: meerkat_mob::run::FlowRunObserver,
) -> Result<ReviewReference, ReviewAdmissionError> {
    admit_with_observer(handle, intent, required_reviewers, Some(observer)).await
}

async fn admit_with_observer(
    handle: &meerkat_mob::MobHandle,
    intent: &ReviewIntent,
    required_reviewers: usize,
    observer: Option<meerkat_mob::run::FlowRunObserver>,
) -> Result<ReviewReference, ReviewAdmissionError> {
    intent.validate(required_reviewers)?;
    // Validate every installed definition before admitting any profile.
    for (role, _) in intent.reviewers.bound_roles() {
        if handle.definition().flows.get(&flow_id(role)) != Some(&flow(role)) {
            return Err(ReviewProofError::DefinitionMismatch.into());
        }
    }
    // The key excludes mutable assignment fields so a previously admitted
    // profile refuses a changed candidate, task or selection for this round.
    // This is per-profile admission, not a transaction across profile flows.
    let key = admission_key(intent)?;
    let identity =
        meerkat_mob::MobExternalDeliveryIdentity::new(&key, uuid::Uuid::new_v4().to_string())
            .map_err(|_| ReviewProofError::InvalidIntent)?;
    let params = serde_json::to_value(intent).map_err(|_| ReviewProofError::InvalidIntent)?;
    let mut runs = Vec::with_capacity(intent.reviewers.bound_roles().count());
    for (role, members) in intent.reviewers.bound_roles() {
        let targets = FlowRoleTargets::new([(role.clone(), members.iter().cloned())])
            .map_err(|_| ReviewProofError::InvalidIntent)?;
        let admission = match &observer {
            Some(observer) => {
                handle
                    .run_flow_with_role_targets_and_external_identity_observed(
                        flow_id(role),
                        params.clone(),
                        targets,
                        &identity,
                        observer.clone(),
                    )
                    .await
            }
            None => {
                handle
                    .run_flow_with_role_targets_and_external_identity(
                        flow_id(role),
                        params.clone(),
                        targets,
                        &identity,
                    )
                    .await
            }
        };
        let run = admission.map_err(|error| match error {
            meerkat_mob::MobError::FlowRunIdempotencyConflict { run_id, flow_id } => {
                ReviewAdmissionError::Conflict { run_id, flow_id }
            }
            error => ReviewAdmissionError::Runtime(error),
        })?;
        if run != RunId::for_work_execution(&handle.definition().id, &flow_id(role), &key) {
            return Err(ReviewProofError::ScopeMismatch.into());
        }
        runs.push(run);
    }
    Ok(ReviewReference { runs })
}

fn admission_key(intent: &ReviewIntent) -> Result<String, ReviewProofError> {
    use sha2::{Digest, Sha256};
    let scope = serde_json::to_vec(&(
        &intent.candidate.tracker_context,
        &intent.candidate.issue_id,
        &intent.round,
    ))
    .map_err(|_| ReviewProofError::InvalidIntent)?;
    Ok(format!("fugue-review:{:x}", Sha256::digest(scope)))
}

/// Run identities come from the native keyed-admission contract. Persisted
/// activation parameters alone cannot prove that a run used that contract.
pub fn reference_for(
    mob: &meerkat_mob::MobId,
    intent: &ReviewIntent,
) -> Result<ReviewReference, ReviewProofError> {
    let key = admission_key(intent)?;
    Ok(ReviewReference {
        runs: intent
            .reviewers
            .bound_roles()
            .map(|(role, _)| RunId::for_work_execution(mob, &flow_id(role), &key))
            .collect(),
    })
}

/// Resolve an assignment through the configured tracker and forge. A submitted
/// PR snapshot is only a selector, never evidence of the current candidate.
pub async fn resolve_candidate(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError> {
    resolve_candidate_for(
        config,
        issue,
        requested_repo,
        pull_request,
        CandidateUse::Review,
    )
    .await
}

pub(crate) async fn resolve_merge_candidate_with_host(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
    host: crate::github_tools::GitHubToolHost,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError> {
    resolve_candidate_with_host(
        config,
        issue,
        requested_repo,
        pull_request,
        CandidateUse::Merge,
        host,
    )
    .await
}

#[derive(Clone, Copy)]
pub(crate) enum CandidateUse {
    Review,
    Merge,
}

async fn resolve_candidate_for(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
    purpose: CandidateUse,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError> {
    let host = crate::github_tools::GitHubToolHost::with_connections(
        config.meerkat.runtime_connections.clone(),
    );
    resolve_candidate_with_host(config, issue, requested_repo, pull_request, purpose, host).await
}

pub(crate) async fn resolve_candidate_with_host(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
    purpose: CandidateUse,
    host: crate::github_tools::GitHubToolHost,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError> {
    resolve_candidate_with_purpose(
        config,
        issue,
        requested_repo,
        pull_request,
        purpose,
        |input| async move {
            let result = tokio::task::spawn_blocking(move || host.inspect_pr(input))
                .await
                .map_err(|_| ReviewProofError::Unavailable)?;
            match result {
                crate::tools::FugueToolResult::Ok {
                    output: crate::tools::FugueToolOutput::GitHubInspectPr(output),
                    ..
                } => Ok(output.pull_request),
                _ => Err(ReviewProofError::Unavailable),
            }
        },
    )
    .await
}

#[cfg(test)]
async fn resolve_candidate_with<F, Fut>(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
    inspect_pr: F,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError>
where
    F: FnOnce(crate::tools::InspectPrInput) -> Fut,
    Fut: std::future::Future<
            Output = Result<crate::tools::GitHubPullRequestSnapshot, ReviewProofError>,
        >,
{
    resolve_candidate_with_purpose(
        config,
        issue,
        requested_repo,
        pull_request,
        CandidateUse::Review,
        inspect_pr,
    )
    .await
}

async fn resolve_candidate_with_purpose<F, Fut>(
    config: &crate::config::ServiceConfig,
    issue: crate::tools::LinearIssueRef,
    requested_repo: &GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestRef,
    purpose: CandidateUse,
    inspect_pr: F,
) -> Result<(ReviewCandidate, crate::tools::GitHubPullRequestSnapshot), ReviewProofError>
where
    F: FnOnce(crate::tools::InspectPrInput) -> Fut,
    Fut: std::future::Future<
            Output = Result<crate::tools::GitHubPullRequestSnapshot, ReviewProofError>,
        >,
{
    let repo = config
        .deploy
        .allowed_repositories
        .iter()
        .find(|repo| same_repo(repo, requested_repo))
        .cloned()
        .ok_or(ReviewProofError::ScopeMismatch)?;
    let issue = crate::linear_adapter::IssueAdapter::from_config(&config.tracker)
        .map_err(|_| ReviewProofError::Unavailable)?
        .inspect_issue(crate::tools::InspectIssueInput { issue })
        .await
        .map_err(|_| ReviewProofError::Unavailable)?
        .issue;
    let expected_number = match &pull_request {
        crate::tools::GitHubPullRequestRef::Number { number } => Some(*number),
        _ => None,
    };
    let pr = inspect_pr(crate::tools::InspectPrInput {
        repo: repo.clone(),
        pull_request,
    })
    .await?;
    if !same_repo(&pr.repo, &repo)
        || expected_number.is_some_and(|number| number != pr.number)
        || pr.number.0 == 0
        || !(pr.state == crate::tools::PullRequestState::Open
            || (matches!(purpose, CandidateUse::Merge)
                && pr.state == crate::tools::PullRequestState::Merged))
        || pr.draft
        || pr.base_branch.0 != config.deploy.base_branch
    {
        return Err(ReviewProofError::ScopeMismatch);
    }
    Ok((
        ReviewCandidate {
            tracker_context: config.tracker_context_id(),
            issue_id: issue.id,
            issue_identifier: issue.identifier.0,
            repo,
            pull_request: pr.number,
            base_branch: pr.base_branch.clone(),
            head_sha: pr.head_sha.clone(),
        },
        pr,
    ))
}

pub(crate) fn same_repo(left: &GitHubRepoRef, right: &GitHubRepoRef) -> bool {
    left.hostname() == right.hostname()
        && left.owner.0.eq_ignore_ascii_case(&right.owner.0)
        && left.name.0.eq_ignore_ascii_case(&right.name.0)
}

/// Deliberately not deserializable. Only the verifier can construct this value.
#[derive(Debug)]
pub struct VerifiedReview {
    intent: ReviewIntent,
    verdicts: BTreeMap<AgentIdentity, IndependentReviewVerdict>,
    runs: BTreeSet<RunId>,
}

impl VerifiedReview {
    pub fn candidate(&self) -> &ReviewCandidate {
        &self.intent.candidate
    }

    pub fn intent(&self) -> &ReviewIntent {
        &self.intent
    }

    pub fn verdicts(&self) -> impl Iterator<Item = &IndependentReviewVerdict> {
        self.verdicts.values()
    }

    pub fn runs(&self) -> impl Iterator<Item = &RunId> {
        self.runs.iter()
    }

    /// Successful execution may return a red or blocked review. Projection can
    /// consume that verified result, but merge additionally requires approval.
    pub fn require_approval(&self) -> Result<(), ReviewProofError> {
        if self
            .verdicts
            .values()
            .all(|verdict| verdict.verdict == IndependentReviewVerdictStatus::Green)
        {
            Ok(())
        } else {
            Err(ReviewProofError::NotApproved)
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReviewProofError {
    #[error("native review intent is invalid")]
    InvalidIntent,
    #[error("one identity cannot count as multiple independent reviewers")]
    DuplicateReviewer,
    #[error("native review selection does not meet the configured reviewer count")]
    ReviewerCount,
    #[error("native review run does not match the repository, candidate or selected reviewer set")]
    ScopeMismatch,
    #[error("native review flow definition is missing or differs from the admitted definition")]
    DefinitionMismatch,
    #[error("native review requires explicit identity bindings")]
    MissingBinding,
    #[error("native review run or its review step has not completed successfully")]
    NotCompleted,
    #[error("native review has incomplete or inconsistent producer evidence")]
    ProducerEvidence,
    #[error("native reviewer did not approve this exact candidate")]
    NotApproved,
    #[error("native review ledger could not be decoded")]
    InvalidLedger,
    #[error("native review records could not be read; merge remains blocked")]
    Unavailable,
    #[error("native review is not the current admitted request for this issue")]
    Superseded,
}

/// Read the native run ledger and one bounded event snapshot. Neither a caller
/// verdict nor a separate approval database participates in this operation.
pub async fn read(
    handle: &meerkat_mob::MobHandle,
    candidate: &ReviewCandidate,
    required_reviewers: usize,
    reference: &ReviewReference,
) -> Result<VerifiedReview, ReviewProofError> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        if reference.runs.is_empty() || reference.runs.len() > required_reviewers {
            return Err(ReviewProofError::ReviewerCount);
        }
        let mut records = BTreeMap::new();
        for id in &reference.runs {
            let run = handle
                .flow_status(id.clone())
                .await
                .map_err(|_| ReviewProofError::Unavailable)?
                .ok_or(ReviewProofError::Unavailable)?;
            if records.insert(id.clone(), (run, Vec::new())).is_some() {
                return Err(ReviewProofError::ScopeMismatch);
            }
        }
        let first = records
            .values()
            .next()
            .ok_or(ReviewProofError::ReviewerCount)?;
        let intent: ReviewIntent = serde_json::from_value(first.0.activation_params.clone())
            .map_err(|_| ReviewProofError::InvalidLedger)?;
        intent.validate(required_reviewers)?;
        if &intent.candidate != candidate {
            return Err(ReviewProofError::ScopeMismatch);
        }
        let events = handle.events();
        let frontier = events
            .latest_cursor()
            .await
            .map_err(|_| ReviewProofError::Unavailable)?;
        let mut cursor = 0;
        while cursor < frontier {
            let page = events
                .poll_strict(cursor, 128)
                .await
                .map_err(|_| ReviewProofError::Unavailable)?;
            if page.is_empty() {
                return Err(ReviewProofError::InvalidLedger);
            }
            for event in page {
                if event.cursor > frontier {
                    break;
                }
                if event.cursor <= cursor {
                    return Err(ReviewProofError::InvalidLedger);
                }
                cursor = event.cursor;
                let id = match &event.kind {
                    MobEventKind::StepDispatched { run_id, .. }
                    | MobEventKind::StepTargetCompleted { run_id, .. } => run_id,
                    _ => continue,
                };
                if let Some((_, events)) = records.get_mut(id) {
                    if events.len() >= 4096 {
                        return Err(ReviewProofError::InvalidLedger);
                    }
                    events.push(event);
                }
            }
        }
        let evidence: Vec<_> = records
            .values()
            .map(|(run, events)| NativeRunEvidence { run, events })
            .collect();
        verify(
            handle.definition(),
            candidate,
            &intent,
            required_reviewers,
            &evidence,
        )
    })
    .await
    .map_err(|_| ReviewProofError::Unavailable)?
}

/// Verify against the current server-resolved candidate and its native intent.
/// The caller must load runs/events from the repository runtime. This is a proof
/// check, not merge authorization: current PR, CI and WorkGraph checks still run
/// immediately before the GitHub operation.
fn verify(
    definition: &MobDefinition,
    candidate: &ReviewCandidate,
    intent: &ReviewIntent,
    required_reviewers: usize,
    evidence: &[NativeRunEvidence<'_>],
) -> Result<VerifiedReview, ReviewProofError> {
    intent.validate(required_reviewers)?;
    if &intent.candidate != candidate || evidence.len() != intent.reviewers.bound_roles().count() {
        return Err(ReviewProofError::ScopeMismatch);
    }
    let mut verified = VerifiedReview {
        intent: intent.clone(),
        verdicts: BTreeMap::new(),
        runs: BTreeSet::new(),
    };
    let mut roles_seen = BTreeSet::new();
    let expected = reference_for(&definition.id, intent)?;
    for NativeRunEvidence { run, events } in evidence {
        if run.mob_id != definition.id
            || !expected.runs.contains(&run.run_id)
            || !verified.runs.insert(run.run_id.clone())
        {
            return Err(ReviewProofError::ScopeMismatch);
        }
        let persisted: ReviewIntent = serde_json::from_value(run.activation_params.clone())
            .map_err(|_| ReviewProofError::InvalidLedger)?;
        if &persisted != intent {
            return Err(ReviewProofError::ScopeMismatch);
        }
        let binding = run
            .role_targets
            .as_ref()
            .ok_or(ReviewProofError::MissingBinding)?;
        let mut bound = binding.bound_roles();
        let (role, members) = bound.next().ok_or(ReviewProofError::MissingBinding)?;
        if bound.next().is_some()
            || intent.reviewers.members(role) != Some(members)
            || !roles_seen.insert(role)
            || run.flow_id != flow_id(role)
            || run.run_id
                != RunId::for_work_execution(
                    &definition.id,
                    &flow_id(role),
                    &admission_key(intent)?,
                )
        {
            return Err(ReviewProofError::ScopeMismatch);
        }
        if definition.flows.get(&run.flow_id) != Some(&flow(role)) {
            return Err(ReviewProofError::DefinitionMismatch);
        }
        let digest = FlowRunConfig::from_definition(run.flow_id.clone(), definition)
            .and_then(|config| config.definition_digest())
            .map_err(|_| ReviewProofError::DefinitionMismatch)?;
        if run.flow_definition_digest.as_deref() != Some(digest.as_str()) {
            return Err(ReviewProofError::DefinitionMismatch);
        }
        let step_id = StepId::from(STEP);
        if run.status != MobRunStatus::Completed
            || run
                .step_status_snapshot()
                .map_err(|_| ReviewProofError::InvalidLedger)?
                .get(&step_id)
                != Some(&StepRunStatus::Completed)
        {
            return Err(ReviewProofError::NotCompleted);
        }
        let aggregate: BTreeMap<AgentIdentity, IndependentReviewVerdict> = serde_json::from_value(
            run.root_step_outputs
                .get(&step_id)
                .cloned()
                .ok_or(ReviewProofError::ProducerEvidence)?,
        )
        .map_err(|_| ReviewProofError::InvalidLedger)?;
        if !aggregate.keys().eq(members.iter()) {
            return Err(ReviewProofError::ProducerEvidence);
        }
        let mut dispatched = BTreeSet::new();
        let mut accepted = BTreeSet::new();
        let mut previous_cursor = None;
        for event in *events {
            let (event_run, event_step, target, output) = match &event.kind {
                MobEventKind::StepDispatched {
                    run_id,
                    step_id,
                    target,
                } => (run_id, step_id, target, None),
                MobEventKind::StepTargetCompleted {
                    run_id,
                    step_id,
                    target,
                    output,
                    ..
                } => (run_id, step_id, target, Some(output)),
                _ => continue,
            };
            if event_run != &run.run_id || event_step != &step_id {
                continue;
            }
            if event.mob_id != definition.id
                || previous_cursor.is_some_and(|cursor| event.cursor <= cursor)
                || !members.contains(&target.identity)
            {
                return Err(ReviewProofError::ProducerEvidence);
            }
            previous_cursor = Some(event.cursor);
            let Some(output) = output else {
                dispatched.insert(target);
                continue;
            };
            if !dispatched.contains(target) || !accepted.insert(&target.identity) {
                return Err(ReviewProofError::ProducerEvidence);
            }
            let verdict: IndependentReviewVerdict =
                serde_json::from_value(output.clone().ok_or(ReviewProofError::ProducerEvidence)?)
                    .map_err(|_| ReviewProofError::InvalidLedger)?;
            if aggregate.get(&target.identity) != Some(&verdict) {
                return Err(ReviewProofError::ProducerEvidence);
            }
            if verdict.validate().is_err()
                || (verdict.reviewer_id != target.identity.as_str()
                    && verdict.reviewer_id
                        != meerkat_meta::member_comms_id::runtime_alias_str(
                            target.identity.as_str(),
                        ))
                || verdict.evidence.head_sha != candidate.head_sha
                || verdict.evidence.base != candidate.base_branch.0
            {
                return Err(ReviewProofError::NotApproved);
            }
            if verified
                .verdicts
                .insert(target.identity.clone(), verdict)
                .is_some()
            {
                return Err(ReviewProofError::DuplicateReviewer);
            }
        }
        if !accepted.into_iter().eq(members.iter()) {
            return Err(ReviewProofError::ProducerEvidence);
        }
    }
    Ok(verified)
}

#[cfg(test)]
mod tests;
