//! The only constructor of authority for an irreversible forge merge.
//! Review intent arbitrates replacement; native runs own reviewer verdicts.

use crate::config::{FinalReviewMergeStrategy, ServiceConfig};
use crate::github_tools::GitHubToolHost;
use crate::meta_runtime::MetaRuntimeRegistry;
use crate::native_review::ReviewCandidate;
use crate::tools::{
    CiStatus, FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolOutput,
    FugueToolResult, GitHubPullRequestRef, GitHubPullRequestSnapshot, InspectPrInput,
    LinearIssueRef, MergeMethod, MergePrInput, PullRequestState, RetryAdvice,
    ToolExecutionMetadata,
};

/// Host-selected policy, never decoded from a tool argument.
#[derive(Clone, Copy)]
pub(crate) enum MergePurpose {
    Deploy,
    FinalReview,
}

/// Constructed only while the current-request lock covers the final checks and
/// forge operation. No serialized boolean or caller input can mint this value.
pub(crate) struct AuthorizedMerge {
    input: MergePrInput,
    expected_base: crate::tools::BranchName,
}

impl AuthorizedMerge {
    pub(crate) fn into_parts(self) -> (MergePrInput, crate::tools::BranchName) {
        (self.input, self.expected_base)
    }

    #[cfg(test)]
    pub(crate) fn for_adapter_fixture(input: MergePrInput) -> Self {
        Self {
            input,
            expected_base: crate::tools::BranchName("main".into()),
        }
    }
}

#[derive(Clone, Copy)]
struct MergePolicy {
    method: MergeMethod,
    require_ci: bool,
    allow_no_checks: bool,
}

impl MergePolicy {
    fn configured(config: &ServiceConfig, purpose: MergePurpose) -> Result<Self, &'static str> {
        match purpose {
            MergePurpose::Deploy if config.deploy.enabled => Ok(Self {
                method: config.deploy.merge_method,
                require_ci: config.deploy.require_ci_success,
                allow_no_checks: config.deploy.allow_no_ci_checks,
            }),
            MergePurpose::FinalReview if config.final_review.enabled => Ok(Self {
                method: match config.final_review.merge.strategy {
                    FinalReviewMergeStrategy::Merge => MergeMethod::Merge,
                    FinalReviewMergeStrategy::Squash => MergeMethod::Squash,
                    FinalReviewMergeStrategy::Rebase => MergeMethod::Rebase,
                },
                require_ci: config.final_review.merge.require_ci_success,
                allow_no_checks: config.deploy.allow_no_ci_checks,
            }),
            _ => Err("The requested merge procedure is disabled for this repository."),
        }
    }

    fn check_ci(self, status: CiStatus) -> Result<(), &'static str> {
        if !self.require_ci
            || status == CiStatus::Success
            || (self.allow_no_checks && status == CiStatus::NoChecks)
        {
            Ok(())
        } else {
            Err("Required CI is not green for the current PR head.")
        }
    }
}

pub(crate) fn missing_context(input: MergePrInput) -> FugueToolResult {
    refused(
        &input,
        "Merge requires the configured Fugue host, current native review, CI and WorkGraph checks. Use the repository's guarded merge action.",
    )
}

fn refused(input: &MergePrInput, message: impl Into<String>) -> FugueToolResult {
    FugueToolResult::Err {
        error: FugueToolError {
            operation: FugueToolOperation::GitHubMergePr,
            code: FugueToolErrorCode::PreconditionFailed,
            message: message.into(),
            retry: RetryAdvice::RefreshStateThenRetry,
            idempotency_key: Some(input.idempotency_key.clone()),
        },
        metadata: ToolExecutionMetadata::default(),
    }
}

fn verify_candidate(
    config: &ServiceConfig,
    policy: MergePolicy,
    input: &MergePrInput,
    candidate: &ReviewCandidate,
    current: &GitHubPullRequestSnapshot,
) -> Result<(), &'static str> {
    if input.expected_head_sha.as_ref() != Some(&candidate.head_sha)
        || current.head_sha != candidate.head_sha
        || current.base_branch != candidate.base_branch
        || current.number != candidate.pull_request
        || !crate::native_review::same_repo(&current.repo, &candidate.repo)
        || !crate::native_review::same_repo(&input.repo, &candidate.repo)
        || !config.deploy.allows_repository(&current.repo)
    {
        return Err("The PR no longer matches the configured repository and reviewed head.");
    }
    if current.draft
        || !matches!(
            current.state,
            PullRequestState::Open | PullRequestState::Merged
        )
        || current.base_branch.0 != config.deploy.base_branch
        || input.method != policy.method
    {
        return Err("The PR state, base branch or merge method does not match repository policy.");
    }
    Ok(())
}

pub(crate) async fn merge(
    config: &ServiceConfig,
    registry: &MetaRuntimeRegistry,
    issue: LinearIssueRef,
    input: MergePrInput,
    purpose: MergePurpose,
) -> FugueToolResult {
    merge_with_host(
        config,
        registry,
        issue,
        input,
        purpose,
        GitHubToolHost::with_connections(config.meerkat.runtime_connections.clone()),
    )
    .await
}

pub(crate) async fn merge_with_host(
    config: &ServiceConfig,
    registry: &MetaRuntimeRegistry,
    issue: LinearIssueRef,
    input: MergePrInput,
    purpose: MergePurpose,
    host: GitHubToolHost,
) -> FugueToolResult {
    merge_review_with_host(config, registry, issue, input, purpose, host, None).await
}

pub(crate) async fn merge_review_with_host(
    config: &ServiceConfig,
    registry: &MetaRuntimeRegistry,
    issue: LinearIssueRef,
    mut input: MergePrInput,
    purpose: MergePurpose,
    host: GitHubToolHost,
    expected_review: Option<&crate::native_review::ReviewIntent>,
) -> FugueToolResult {
    let policy = match MergePolicy::configured(config, purpose) {
        Ok(policy) => policy,
        Err(message) => return refused(&input, message),
    };
    if input
        .expected_head_sha
        .as_ref()
        .is_none_or(|sha| sha.0.trim().is_empty())
    {
        return refused(&input, "Merge requires the exact reviewed head SHA.");
    }
    let (candidate, current) = match crate::native_review::resolve_merge_candidate_with_host(
        config,
        issue,
        &input.repo,
        input.pull_request.clone(),
        host.clone(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return refused(&input, error.to_string()),
    };
    if let Err(message) = verify_candidate(config, policy, &input, &candidate, &current) {
        return refused(&input, message);
    }
    // Resolve branch selectors once, before the final protected operation.
    input.repo = candidate.repo.clone();
    input.pull_request = GitHubPullRequestRef::Number {
        number: candidate.pull_request,
    };
    let proof = match registry.read_current_native_review(&candidate).await {
        Ok(proof) => proof,
        Err(error) => return refused(&input, error.to_string()),
    };
    if let Err(error) = proof.require_approval() {
        return refused(&input, error.to_string());
    }
    if expected_review.is_some_and(|expected| expected != proof.intent()) {
        return refused(&input, "The completed review request is no longer current.");
    }
    if matches!(purpose, MergePurpose::FinalReview)
        && crate::native_review::final_review_count(&config.final_review, &proof.intent().reviewers)
            .is_err()
    {
        return refused(
            &input,
            "The native reviewers do not cover the configured final-review profiles.",
        );
    }
    let config = config.clone();
    let registry_for_check = registry.clone();
    let error_input = input.clone();
    let runtime = tokio::runtime::Handle::current();
    registry.with_current_review(proof.intent().clone(), move || runtime.block_on(async move {
        let current = match host.inspect_pr(InspectPrInput {
            repo: input.repo.clone(), pull_request: input.pull_request.clone(),
        }) {
            FugueToolResult::Ok { output: FugueToolOutput::GitHubInspectPr(output), .. } => output.pull_request,
            _ => return refused(&input, "The current PR could not be verified before merge."),
        };
        if let Err(message) = verify_candidate(&config, policy, &input, &candidate, &current) {
            return refused(&input, message);
        }
        let status = match host.check_pr_ci(&current) {
            FugueToolResult::Ok { output: FugueToolOutput::GitHubCheckCi(output), .. } => output.status,
            _ => return refused(&input, "The complete CI results for this PR head could not be verified."),
        };
        if let Err(message) = policy.check_ci(status) {
            return refused(&input, message);
        }
        if registry_for_check.verify_review_independence(proof.intent()).is_err() {
            return refused(&input, "A selected reviewer belongs to this issue's execution team or its membership could not be verified.");
        }
        let canonical_issue = LinearIssueRef::Id { id: candidate.issue_id };
        let graph = crate::workgraph_gate::WorkGraphTerminalGate::for_service(&config);
        // A deleted native database must not be recreated as an empty green gate.
        if !graph.state_path.is_file() {
            return refused(&input, "The repository's native WorkGraph database is unavailable.");
        }
        graph.merge_pr_with(&config.tracker, &canonical_issue, input, |input| {
            host.merge_authorized(AuthorizedMerge { input, expected_base: candidate.base_branch })
        }).await
    })).await.unwrap_or_else(|_| refused(&error_input, "The review request changed or its authority could not be verified before merge."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_checks_exception_never_accepts_pending_failed_or_canceled_checks() {
        let policy = MergePolicy {
            method: MergeMethod::Squash,
            require_ci: true,
            allow_no_checks: true,
        };
        for status in [CiStatus::Pending, CiStatus::Failure, CiStatus::Cancelled] {
            assert!(policy.check_ci(status).is_err());
        }
        assert!(policy.check_ci(CiStatus::NoChecks).is_ok());
        assert!(policy.check_ci(CiStatus::Success).is_ok());
        assert!(
            MergePolicy {
                allow_no_checks: false,
                ..policy
            }
            .check_ci(CiStatus::NoChecks)
            .is_err()
        );
    }
}
