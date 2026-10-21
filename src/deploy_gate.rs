use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::DeployLaneConfig;
use crate::tools::{
    BranchName, CiStatus, GitHubPullRequestNumber, GitHubPullRequestRef, GitHubPullRequestSnapshot,
    GitHubRepoRef, GitSha, IdempotencyKey, LinearIssueRef, MergeMethod, MergePrInput,
    PullRequestState,
};

pub const DEPLOY_OUTCOME_SCHEMA_VERSION: &str = "fugue.deploy.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployOutcome {
    pub schema_version: String,
    pub issue_identifier: String,
    pub status: DeployOutcomeStatus,
    pub failure_category: Option<DeployFailureCategory>,
    pub pull_request: DeployPullRequestEvidence,
    pub merge: Option<DeployMergeEvidence>,
    pub ota: Option<DeployOtaEvidence>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployStartRequest {
    pub issue: LinearIssueRef,
    pub pull_request: GitHubPullRequestSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_head_sha: Option<GitSha>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_status: Option<CiStatus>,
    #[serde(default)]
    pub unresolved_workgraph_blockers: usize,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployOutcomeStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployFailureCategory {
    MissingPrEvidence,
    WrongRepository,
    StaleHead,
    CiNotGreen,
    UnresolvedWorkGraph,
    MergeFailed,
    OtaFailed,
    MissingAuth,
    UnsafeDeploy,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployPullRequestEvidence {
    pub repo: GitHubRepoRef,
    pub number: GitHubPullRequestNumber,
    pub url: String,
    pub base_branch: BranchName,
    pub head_branch: BranchName,
    pub head_sha: GitSha,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployMergeEvidence {
    pub method: MergeMethod,
    pub merged_sha: GitSha,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployOtaEvidence {
    pub service_name: String,
    pub deployed_checkout_sha: GitSha,
    #[serde(default)]
    pub installed_binary_hashes: Vec<DeployBinaryHash>,
    pub health_url: String,
    pub health_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployBinaryHash {
    pub path: String,
    pub sha256: String,
}

impl DeployOutcome {
    pub fn validate(&self) -> Result<(), DeployOutcomeError> {
        if self.schema_version != DEPLOY_OUTCOME_SCHEMA_VERSION {
            return Err(DeployOutcomeError::UnsupportedSchema {
                schema_version: self.schema_version.clone(),
            });
        }
        if self.issue_identifier.trim().is_empty() {
            return Err(DeployOutcomeError::MissingField("issue_identifier"));
        }
        if self.pull_request.url.trim().is_empty() {
            return Err(DeployOutcomeError::MissingField("pull_request.url"));
        }
        if self.pull_request.base_branch.0.trim().is_empty() {
            return Err(DeployOutcomeError::MissingField("pull_request.base_branch"));
        }
        if self.pull_request.head_sha.0.trim().is_empty() {
            return Err(DeployOutcomeError::MissingField("pull_request.head_sha"));
        }
        match self.status {
            DeployOutcomeStatus::Succeeded => {
                if self.failure_category.is_some() {
                    return Err(DeployOutcomeError::SucceededWithFailureCategory);
                }
                if self.merge.is_none() {
                    return Err(DeployOutcomeError::MissingField("merge"));
                }
                if self.ota.is_none() {
                    return Err(DeployOutcomeError::MissingField("ota"));
                }
                let merge = self
                    .merge
                    .as_ref()
                    .ok_or(DeployOutcomeError::MissingField("merge"))?;
                let ota = self
                    .ota
                    .as_ref()
                    .ok_or(DeployOutcomeError::MissingField("ota"))?;
                if merge.merged_sha != ota.deployed_checkout_sha {
                    return Err(DeployOutcomeError::SuccessfulDeployShaMismatch {
                        merged_sha: merge.merged_sha.clone(),
                        deployed_checkout_sha: ota.deployed_checkout_sha.clone(),
                    });
                }
            }
            DeployOutcomeStatus::Failed => {
                if self.failure_category.is_none() {
                    return Err(DeployOutcomeError::FailedWithoutFailureCategory);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployOutcomeError {
    UnsupportedSchema {
        schema_version: String,
    },
    MissingField(&'static str),
    SucceededWithFailureCategory,
    FailedWithoutFailureCategory,
    SuccessfulDeployShaMismatch {
        merged_sha: GitSha,
        deployed_checkout_sha: GitSha,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployGateInput {
    pub config: DeployLaneConfig,
    pub issue_identifier: String,
    pub pull_request: Option<GitHubPullRequestSnapshot>,
    pub recorded_head_sha: Option<GitSha>,
    pub ci_status: Option<CiStatus>,
    pub unresolved_workgraph_blockers: usize,
    pub ota_update_enabled: bool,
    pub deploy_auth_available: bool,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeployGatePlan {
    pub decision: DeployGateDecision,
    pub target_lane: String,
    pub procedure: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub procedure_identity: Option<DeployProcedureIdentity>,
    pub failure_category: Option<DeployFailureCategory>,
    pub reasons: Vec<String>,
    pub merge: Option<MergePrInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeployProcedureIdentity {
    MeerkatFugueMergeOta { path: String },
    Unsupported { path: String },
}

impl DeployProcedureIdentity {
    fn from_config(path: &str, repo: &GitHubRepoRef) -> Result<Self, String> {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Err("deploy procedure is empty".to_string());
        }
        if trimmed != ".fugue/deploy.md" {
            return Err(format!("unsupported deploy procedure `{trimmed}`"));
        }
        if repo.owner.0 != "lukacf" || repo.name.0 != "meerkat-fugue" {
            return Err(format!(
                "deploy procedure `{trimmed}` is currently supported only for lukacf/meerkat-fugue"
            ));
        }
        Ok(Self::MeerkatFugueMergeOta {
            path: trimmed.to_string(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployGateDecision {
    MergeAndDeploy,
    ReadyForReview,
    Rework,
    Blocked,
}

pub fn plan_deploy_gate(input: DeployGateInput) -> DeployGatePlan {
    let config = input.config;
    let Some(pull_request) = input.pull_request else {
        return deploy_failure_plan(
            DeployGateDecision::ReadyForReview,
            config.ready_lane,
            DeployFailureCategory::MissingPrEvidence,
            "missing durable PR handoff evidence",
        );
    };

    if !config.allows_repository(&pull_request.repo) {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::WrongRepository,
            "PR repository is not allowed for this deploy lane",
        );
    }
    if !pull_request
        .base_branch
        .0
        .eq_ignore_ascii_case(&config.base_branch)
    {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::UnsafeDeploy,
            "PR base branch does not match deploy policy",
        );
    }
    if pull_request.state != PullRequestState::Open {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::MergeFailed,
            "PR is not open",
        );
    }
    if config.require_fresh_head {
        let Some(recorded_head_sha) = input.recorded_head_sha else {
            return deploy_failure_plan(
                DeployGateDecision::ReadyForReview,
                config.ready_lane,
                DeployFailureCategory::MissingPrEvidence,
                "missing recorded PR head SHA",
            );
        };
        if recorded_head_sha != pull_request.head_sha {
            return deploy_failure_plan(
                DeployGateDecision::Blocked,
                config.blocked_lane,
                DeployFailureCategory::StaleHead,
                "recorded PR head SHA is stale",
            );
        }
    }
    if config.require_ci_success && !ci_is_green(input.ci_status) {
        return deploy_failure_plan(
            DeployGateDecision::Rework,
            config.rework_lane,
            DeployFailureCategory::CiNotGreen,
            "required CI is not green for the deploy head",
        );
    }
    if config.require_no_blocking_workgraph && input.unresolved_workgraph_blockers > 0 {
        return deploy_failure_plan(
            DeployGateDecision::Rework,
            config.rework_lane,
            DeployFailureCategory::UnresolvedWorkGraph,
            "unresolved blocking WorkGraph items remain",
        );
    }
    if config.require_ota_update && !input.ota_update_enabled {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::UnsafeDeploy,
            "OTA update is not enabled",
        );
    }
    let procedure_identity =
        match DeployProcedureIdentity::from_config(&config.procedure, &pull_request.repo) {
            Ok(identity) => identity,
            Err(reason) => {
                return DeployGatePlan {
                    decision: DeployGateDecision::Blocked,
                    target_lane: config.blocked_lane,
                    procedure: config.procedure.clone(),
                    procedure_identity: Some(DeployProcedureIdentity::Unsupported {
                        path: config.procedure,
                    }),
                    failure_category: Some(DeployFailureCategory::UnsafeDeploy),
                    reasons: vec![reason],
                    merge: None,
                };
            }
        };

    if !input.deploy_auth_available {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::MissingAuth,
            "deploy authorization is unavailable",
        );
    }

    DeployGatePlan {
        decision: DeployGateDecision::MergeAndDeploy,
        target_lane: config.success_lane,
        procedure: config.procedure,
        procedure_identity: Some(procedure_identity),
        failure_category: None,
        reasons: Vec::new(),
        merge: Some(MergePrInput {
            repo: pull_request.repo.clone(),
            pull_request: GitHubPullRequestRef::Number {
                number: pull_request.number,
            },
            method: config.merge_method,
            expected_head_sha: Some(pull_request.head_sha),
            idempotency_key: input.idempotency_key,
        }),
    }
}

fn deploy_failure_plan(
    decision: DeployGateDecision,
    target_lane: String,
    failure_category: DeployFailureCategory,
    reason: &str,
) -> DeployGatePlan {
    DeployGatePlan {
        decision,
        target_lane,
        procedure: String::new(),
        procedure_identity: None,
        failure_category: Some(failure_category),
        reasons: vec![reason.to_string()],
        merge: None,
    }
}

fn ci_is_green(status: Option<CiStatus>) -> bool {
    matches!(status, Some(CiStatus::Success | CiStatus::Skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{GitHubOwner, GitHubRepoName};

    fn pr_evidence() -> DeployPullRequestEvidence {
        DeployPullRequestEvidence {
            repo: GitHubRepoRef {
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            },
            number: GitHubPullRequestNumber(201),
            url: "https://github.com/lukacf/meerkat-fugue/pull/201".to_string(),
            base_branch: BranchName("main".to_string()),
            head_branch: BranchName("codex/feature".to_string()),
            head_sha: GitSha("abc123".to_string()),
        }
    }

    fn pr_snapshot() -> GitHubPullRequestSnapshot {
        GitHubPullRequestSnapshot {
            repo: GitHubRepoRef {
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            },
            number: GitHubPullRequestNumber(201),
            title: "Feature".to_string(),
            url: "https://github.com/lukacf/meerkat-fugue/pull/201".to_string(),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName("codex/feature".to_string()),
            base_branch: BranchName("main".to_string()),
            head_sha: GitSha("abc123".to_string()),
        }
    }

    fn deploy_config() -> DeployLaneConfig {
        DeployLaneConfig {
            enabled: true,
            allowed_repositories: vec![GitHubRepoRef {
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            }],
            ..DeployLaneConfig::default()
        }
    }

    fn gate_input() -> DeployGateInput {
        DeployGateInput {
            config: deploy_config(),
            issue_identifier: "LUC-631".to_string(),
            pull_request: Some(pr_snapshot()),
            recorded_head_sha: Some(GitSha("abc123".to_string())),
            ci_status: Some(CiStatus::Success),
            unresolved_workgraph_blockers: 0,
            ota_update_enabled: true,
            deploy_auth_available: true,
            idempotency_key: IdempotencyKey("deploy:LUC-631:201:abc123".to_string()),
        }
    }

    #[test]
    fn deploy_outcome_round_trips_success_evidence() {
        let now = Utc::now();
        let outcome = DeployOutcome {
            schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
            issue_identifier: "LUC-631".to_string(),
            status: DeployOutcomeStatus::Succeeded,
            failure_category: None,
            pull_request: pr_evidence(),
            merge: Some(DeployMergeEvidence {
                method: MergeMethod::Squash,
                merged_sha: GitSha("def456".to_string()),
            }),
            ota: Some(DeployOtaEvidence {
                service_name: "meerkat-fugue".to_string(),
                deployed_checkout_sha: GitSha("def456".to_string()),
                installed_binary_hashes: vec![DeployBinaryHash {
                    path: "/opt/fugue/bin/meerkat-fugue".to_string(),
                    sha256: "sha256:abc".to_string(),
                }],
                health_url: "http://127.0.0.1:4001/healthz".to_string(),
                health_result: "ok".to_string(),
            }),
            started_at: now,
            completed_at: now,
        };

        let encoded = serde_json::to_string(&outcome).unwrap();
        let decoded: DeployOutcome = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, outcome);
        decoded.validate().unwrap();
    }

    #[test]
    fn deploy_outcome_rejects_failed_without_failure_category() {
        let now = Utc::now();
        let outcome = DeployOutcome {
            schema_version: DEPLOY_OUTCOME_SCHEMA_VERSION.to_string(),
            issue_identifier: "LUC-631".to_string(),
            status: DeployOutcomeStatus::Failed,
            failure_category: None,
            pull_request: pr_evidence(),
            merge: None,
            ota: None,
            started_at: now,
            completed_at: now,
        };

        assert_eq!(
            outcome.validate(),
            Err(DeployOutcomeError::FailedWithoutFailureCategory)
        );
    }

    #[test]
    fn deploy_gate_allows_exact_allowed_repo_green_ci_and_ota() {
        let plan = plan_deploy_gate(gate_input());

        assert_eq!(plan.decision, DeployGateDecision::MergeAndDeploy);
        assert_eq!(plan.target_lane, "Done");
        assert_eq!(plan.procedure, ".fugue/deploy.md");
        assert_eq!(
            plan.procedure_identity,
            Some(DeployProcedureIdentity::MeerkatFugueMergeOta {
                path: ".fugue/deploy.md".to_string()
            })
        );
        let merge = plan.merge.expect("merge plan");
        assert_eq!(merge.expected_head_sha, Some(GitSha("abc123".to_string())));
        assert_eq!(merge.method, MergeMethod::Squash);
    }

    #[test]
    fn deploy_gate_rejects_unsupported_procedure_deterministically() {
        let mut input = gate_input();
        input.config.procedure = ".fugue/custom-deploy.md".to_string();

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::Blocked);
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::UnsafeDeploy)
        );
        assert_eq!(
            plan.procedure_identity,
            Some(DeployProcedureIdentity::Unsupported {
                path: ".fugue/custom-deploy.md".to_string()
            })
        );
        assert!(plan.reasons[0].contains("unsupported deploy procedure"));
        assert!(plan.merge.is_none());
    }

    #[test]
    fn deploy_gate_requires_supported_repo_for_meerkat_procedure() {
        let mut input = gate_input();
        let repo = GitHubRepoRef {
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("future-repo".to_string()),
        };
        input.config.allowed_repositories = vec![repo.clone()];
        input.pull_request.as_mut().unwrap().repo = repo;

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::Blocked);
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::UnsafeDeploy)
        );
        assert!(plan.reasons[0].contains("supported only for lukacf/meerkat-fugue"));
        assert!(plan.merge.is_none());
    }

    #[test]
    fn deploy_gate_rejects_wrong_repo_to_blocked() {
        let mut input = gate_input();
        input.pull_request.as_mut().unwrap().repo.name = GitHubRepoName("other".to_string());

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::Blocked);
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::WrongRepository)
        );
    }

    #[test]
    fn deploy_gate_rejects_stale_head_to_blocked() {
        let mut input = gate_input();
        input.recorded_head_sha = Some(GitSha("old".to_string()));

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::Blocked);
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::StaleHead)
        );
    }

    #[test]
    fn deploy_gate_routes_failed_ci_to_rework() {
        let mut input = gate_input();
        input.ci_status = Some(CiStatus::Failure);

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::Rework);
        assert_eq!(plan.target_lane, "Rework");
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::CiNotGreen)
        );
    }

    #[test]
    fn deploy_gate_routes_missing_pr_to_ready_for_review() {
        let mut input = gate_input();
        input.pull_request = None;

        let plan = plan_deploy_gate(input);

        assert_eq!(plan.decision, DeployGateDecision::ReadyForReview);
        assert_eq!(
            plan.failure_category,
            Some(DeployFailureCategory::MissingPrEvidence)
        );
    }
}
