use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A late plan/validation update must not erase the host's merge receipt.
/// An explicit replacement section is still accepted for a subsequent action.
pub(crate) fn merge_preserved_deploy_evidence(rendered: &str, existing: &str) -> String {
    fn section(body: &str) -> Option<&str> {
        let mut offset = 0;
        let mut start = None;
        for line in body.split_inclusive('\n') {
            let trimmed = line.trim();
            if trimmed == "### Deployment" {
                start = Some(offset);
            } else if let Some(start) = start
                && (trimmed.starts_with("## ") || trimmed.starts_with("### "))
            {
                return Some(body[start..offset].trim_end());
            }
            offset += line.len();
        }
        start.map(|start| body[start..].trim_end())
    }
    if section(rendered).is_none()
        && let Some(evidence) = section(existing)
    {
        format!("{}\n\n{evidence}", rendered.trim_end())
    } else {
        rendered.to_string()
    }
}

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

/// Evidence for the configured guarded merge-only or merge-and-deploy action.
/// Inspect the current issue, PR, CI and repository Work Graph before submitting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployStartRequest {
    /// Issue in this repository's configured tracker, using its inspected ID or identifier.
    pub issue: LinearIssueRef,
    /// Full current snapshot returned by github_inspect_pr. Preserve the repository
    /// host, PR number, title, URL, state, draft flag, branches and exact head SHA.
    pub pull_request: GitHubPullRequestSnapshot,
    /// Head SHA recorded by the issue's durable PR handoff/review evidence. Supply
    /// it when fresh-head policy is enabled; compare it with the inspected PR head.
    /// A mismatch requires refreshed review evidence, not overwriting this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_head_sha: Option<GitSha>,
    /// Status returned by github_check_ci for that exact PR head. Supply it when
    /// CI is required. no_checks means no checks exist, not success; only an
    /// explicit repository policy permits it. Never infer CI success from prose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_status: Option<CiStatus>,
    /// Count unresolved blocking prerequisites from the issue's native Work Graph
    /// in this repository. Supply the observed count; do not assume zero.
    #[serde(default)]
    pub unresolved_workgraph_blockers: usize,
    /// Stable key for this exact issue, PR head and action. Replay a lost response
    /// with the identical request and key. Do not change keys to bypass a refusal.
    pub idempotency_key: IdempotencyKey,
    /// Omit for the initial action or unchanged replay. After a failed or uncertain
    /// deployment, only explicit operator inspection and authorization permits a
    /// retry with the prior attempt ID, reason and a new idempotency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_deployment: Option<crate::DeploymentRetry>,
}

pub(crate) const DEPLOY_TOOL_DESCRIPTION: &str = "Run the configured guarded merge-only or merge-and-OTA action as repository Kapellmeister after explicit action-lane/operator intent. First inspect the issue and its durable PR handoff/review, use github_inspect_pr for the full current PR snapshot and github_check_ci for that exact head, and inspect the issue's native Work Graph blocking prerequisites. Supply the recorded head, observed CI status and blocker count; never invent evidence to satisfy a guard. The host rechecks current evidence and selects the configured procedure and merge method, so do not supply policy overrides. Replay lost responses with the identical request and key. For a failed or uncertain deployment, report the attempt ID and wait for operator inspection and authorization before supplying retry_deployment with a new key.";

pub(crate) fn deploy_start_input_schema() -> serde_json::Value {
    // Keep both native and MCP tool contracts aligned with the wire type. Inline
    // references because this schema is embedded in each provider's tool catalog.
    schemars::generate::SchemaSettings::draft2020_12()
        .with(|settings| {
            settings.inline_subschemas = true;
            settings.meta_schema = None;
        })
        .into_generator()
        .into_root_schema_for::<DeployStartRequest>()
        .into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployOutcomeStatus {
    /// The exact PR head was merged; no deployment was requested or performed.
    Merged,
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
            DeployOutcomeStatus::Merged => {
                if self.failure_category.is_some() {
                    return Err(DeployOutcomeError::SucceededWithFailureCategory);
                }
                if self.merge.is_none() {
                    return Err(DeployOutcomeError::MissingField("merge"));
                }
                if self.ota.is_some() {
                    return Err(DeployOutcomeError::MergeOnlyWithDeployment);
                }
            }
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
    MergeOnlyWithDeployment,
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
    MergeOnly,
    MeerkatFugueMergeOta { path: String },
    Unsupported { path: String },
}

impl DeployProcedureIdentity {
    fn from_config(path: &str, repo: &GitHubRepoRef, require_ota: bool) -> Result<Self, String> {
        let trimmed = path.trim();
        if trimmed == "merge-only" {
            return if require_ota {
                Err("merge-only requires deploy.require_ota_update=false".to_string())
            } else {
                Ok(Self::MergeOnly)
            };
        }
        if trimmed.is_empty() {
            return Err("deploy procedure is empty".to_string());
        }
        if trimmed != ".fugue/deploy.md" {
            return Err(format!("unsupported deploy procedure `{trimmed}`"));
        }
        if repo.hostname() != "github.com"
            || repo.owner.0 != "lukacf"
            || repo.name.0 != "meerkat-fugue"
        {
            return Err(format!(
                "deploy procedure `{trimmed}` is currently supported only for github.com/lukacf/meerkat-fugue"
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
    MergeOnly,
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
    // A merged snapshot may resume an uncertain transaction. The execution
    // gate still verifies the actual merge commit and every current prerequisite.
    if !matches!(
        pull_request.state,
        PullRequestState::Open | PullRequestState::Merged
    ) {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::MergeFailed,
            "PR is neither open nor already merged",
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
    if config.require_ci_success
        && !ci_is_green(input.ci_status)
        && !(config.allow_no_ci_checks && input.ci_status == Some(CiStatus::NoChecks))
    {
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
    let procedure_identity = match DeployProcedureIdentity::from_config(
        &config.procedure,
        &pull_request.repo,
        config.require_ota_update,
    ) {
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

    let merge_only = procedure_identity == DeployProcedureIdentity::MergeOnly;
    if !merge_only && !input.deploy_auth_available {
        return deploy_failure_plan(
            DeployGateDecision::Blocked,
            config.blocked_lane,
            DeployFailureCategory::MissingAuth,
            "deploy authorization is unavailable",
        );
    }

    DeployGatePlan {
        decision: if merge_only {
            DeployGateDecision::MergeOnly
        } else {
            DeployGateDecision::MergeAndDeploy
        },
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
                host: None,
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
                host: None,
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
                host: None,
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
    fn deploy_tool_schema_exposes_the_complete_typed_request() {
        let schema = deploy_start_input_schema();
        let request = DeployStartRequest {
            issue: LinearIssueRef::Identifier {
                identifier: crate::tools::LinearIssueIdentifier("LUC-631".to_string()),
            },
            pull_request: pr_snapshot(),
            recorded_head_sha: Some(GitSha("abc123".to_string())),
            ci_status: Some(CiStatus::Success),
            unresolved_workgraph_blockers: 0,
            idempotency_key: IdempotencyKey("deploy:LUC-631:201:abc123".to_string()),
            retry_deployment: Some(crate::DeploymentRetry {
                attempt_id: "previous-attempt".to_string(),
                reason: "Operator inspected the failed attempt and authorized retry".to_string(),
            }),
        };
        let wire = serde_json::to_value(&request).unwrap();
        let properties = schema["properties"].as_object().unwrap();
        for field in wire.as_object().unwrap().keys() {
            assert!(
                properties.contains_key(field),
                "missing advertised field: {field}"
            );
        }
        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(validator.is_valid(&wire));
        assert_eq!(
            serde_json::from_value::<DeployStartRequest>(wire.clone()).unwrap(),
            request
        );
        // The model must discover the actual nested reference and snapshot shapes.
        for pointer in [
            "/issue/kind",
            "/pull_request/repo/owner",
            "/pull_request/head_sha",
            "/pull_request/title",
            "/idempotency_key",
        ] {
            let mut invalid = wire.clone();
            *invalid.pointer_mut(pointer).unwrap() = serde_json::json!(false);
            assert!(
                !validator.is_valid(&invalid),
                "schema accepted wrong shape at {pointer}"
            );
        }
        for field in ["issue", "pull_request", "idempotency_key"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                !validator.is_valid(&missing),
                "schema omitted required field: {field}"
            );
        }
        let mut unknown = wire;
        unknown["operator_approved"] = serde_json::json!(true);
        assert!(!validator.is_valid(&unknown));
    }

    #[test]
    fn deploy_tool_schema_preserves_policy_evidence_and_retry_shapes() {
        let schema = deploy_start_input_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut wire = serde_json::json!({
            "issue": {"kind": "id", "id": "issue-631"},
            "pull_request": pr_snapshot(),
            "idempotency_key": "deploy:issue-631:201:abc123"
        });
        // Existing HTTP callers may omit policy-dependent evidence. The host still
        // rejects missing evidence when that repository policy requires it.
        assert!(validator.is_valid(&wire));
        for status in [
            CiStatus::NoChecks,
            CiStatus::Pending,
            CiStatus::Success,
            CiStatus::Failure,
            CiStatus::Error,
            CiStatus::Skipped,
            CiStatus::Cancelled,
        ] {
            wire["ci_status"] = serde_json::to_value(status).unwrap();
            assert!(validator.is_valid(&wire));
            assert_eq!(
                serde_json::from_value::<DeployStartRequest>(wire.clone())
                    .unwrap()
                    .ci_status,
                Some(status)
            );
        }
        wire["ci_status"] = serde_json::json!("green");
        assert!(!validator.is_valid(&wire));
        wire["ci_status"] = serde_json::Value::Null;
        wire["unresolved_workgraph_blockers"] = serde_json::json!(-1);
        assert!(!validator.is_valid(&wire));
        wire["unresolved_workgraph_blockers"] = serde_json::json!(0);
        wire["retry_deployment"] = serde_json::json!({"attempt_id":"a"});
        assert!(!validator.is_valid(&wire));
        wire["retry_deployment"] = serde_json::json!({"attempt_id":"a", "reason":""});
        assert!(!validator.is_valid(&wire));
    }

    #[test]
    fn absent_ci_requires_explicit_policy_and_does_not_allow_pending_or_failed_checks() {
        let mut input = gate_input();
        input.ci_status = Some(CiStatus::NoChecks);
        assert_eq!(
            plan_deploy_gate(input.clone()).failure_category,
            Some(DeployFailureCategory::CiNotGreen)
        );
        input.config.allow_no_ci_checks = true;
        assert_eq!(plan_deploy_gate(input.clone()).failure_category, None);
        for status in [
            None,
            Some(CiStatus::Pending),
            Some(CiStatus::Failure),
            Some(CiStatus::Error),
            Some(CiStatus::Cancelled),
        ] {
            input.ci_status = status;
            assert_eq!(
                plan_deploy_gate(input.clone()).failure_category,
                Some(DeployFailureCategory::CiNotGreen)
            );
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
    fn merge_only_accepts_an_allowed_enterprise_repo_without_ota() {
        let mut input = gate_input();
        let mut repo = input.pull_request.as_ref().unwrap().repo.clone();
        repo.host = Some(crate::tools::GitHubHost::try_from("git.example.com".to_owned()).unwrap());
        repo.owner = GitHubOwner("developer".into());
        repo.name = GitHubRepoName("sandbox".into());
        input.pull_request.as_mut().unwrap().repo = repo.clone();
        input.config.allowed_repositories = vec![repo];
        input.config.procedure = "merge-only".into();
        input.config.require_ota_update = false;
        input.ota_update_enabled = false;
        input.deploy_auth_available = false;
        let plan = plan_deploy_gate(input);
        assert!(plan.merge.is_some(), "{plan:?}");
        assert_eq!(serde_json::to_value(plan.decision).unwrap(), "merge_only");
    }

    #[test]
    fn merge_only_retains_ci_and_head_guards() {
        for (head, ci, expected) in [
            ("stale", CiStatus::Success, DeployFailureCategory::StaleHead),
            (
                "abc123",
                CiStatus::Failure,
                DeployFailureCategory::CiNotGreen,
            ),
        ] {
            let mut input = gate_input();
            input.config.procedure = "merge-only".into();
            input.config.require_ota_update = false;
            input.ota_update_enabled = false;
            input.deploy_auth_available = false;
            input.recorded_head_sha = Some(GitSha(head.into()));
            input.ci_status = Some(ci);
            let plan = plan_deploy_gate(input);
            assert!(plan.merge.is_none());
            assert_eq!(plan.failure_category, Some(expected));
        }
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
            host: None,
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
        assert!(plan.reasons[0].contains("supported only for github.com/lukacf/meerkat-fugue"));
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
