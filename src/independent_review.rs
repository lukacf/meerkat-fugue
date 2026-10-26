use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::IndependentReviewConfig;
use crate::tools::{
    GitHubPullRequestSnapshot, GitSha, IdempotencyKey, LinearIssueRef, SpawnOrResumeExecutionInput,
};

pub const INDEPENDENT_REVIEW_SCHEMA_VERSION: &str = "fugue.review.v1";

/// HTTP success alone does not establish that the host accepted a completion.
/// This decodes only the acknowledgement, not the host's review evidence.
pub(crate) fn completion_acknowledged(body: &str) -> bool {
    #[derive(Deserialize)]
    struct Response {
        result: Acceptance,
    }
    #[derive(Deserialize)]
    struct Acceptance {
        accepted: bool,
    }
    serde_json::from_str::<Response>(body).is_ok_and(|response| response.result.accepted)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewVerdict {
    pub schema_version: String,
    pub reviewer_id: String,
    pub focus: String,
    pub verdict: IndependentReviewVerdictStatus,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<IndependentReviewFinding>,
    pub evidence: IndependentReviewEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewStartRequest {
    pub issue: LinearIssueRef,
    pub spawn_input: SpawnOrResumeExecutionInput,
    pub pull_request: GitHubPullRequestSnapshot,
    pub orchestrator_profile: String,
}

/// Public launch intent. Runtime profiles, templates and workspace paths belong
/// to the configured host, not the agent requesting review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewLaunchRequest {
    pub issue: LinearIssueRef,
    pub repo: crate::tools::GitHubRepoRef,
    pub pull_request: crate::tools::GitHubPullRequestNumber,
    pub head_sha: GitSha,
    pub idempotency_key: IdempotencyKey,
}

pub fn independent_review_launch_input_schema() -> Value {
    json!({
        "type":"object",
        "properties": {
            "issue":{"oneOf":[
                {"type":"object","properties":{"kind":{"const":"id"},"id":{"type":"string","minLength":1}},"required":["kind","id"],"additionalProperties":false},
                {"type":"object","properties":{"kind":{"const":"identifier"},"identifier":{"type":"string","minLength":1}},"required":["kind","identifier"],"additionalProperties":false}
            ]},
            "repo":{"type":"object","properties":{"host":{"type":"string"},"owner":{"type":"string","minLength":1},"name":{"type":"string","minLength":1}},"required":["owner","name"],"additionalProperties":false},
            "pull_request":{"type":"integer","minimum":1},
            "head_sha":{"type":"string","description":"Expected immutable head of the open, ready-for-review PR. The host rechecks it."},
            "idempotency_key":{"type":"string","minLength":1,"description":"Reuse this key and identical candidate after an uncertain response."}
        },
        "required":["issue","repo","pull_request","head_sha","idempotency_key"],
        "additionalProperties":false
    })
}

pub(crate) fn configured_review_launch(
    config: &IndependentReviewConfig,
    request: IndependentReviewLaunchRequest,
    candidate: crate::native_review::ReviewCandidate,
    pull_request: GitHubPullRequestSnapshot,
) -> Result<IndependentReviewStartRequest, crate::native_review::ReviewProofError> {
    use crate::native_review::{ReviewProofError, same_repo};
    if !config.enabled
        || request.idempotency_key.0.trim().is_empty()
        || request.head_sha != candidate.head_sha
        || request.pull_request != candidate.pull_request
        || !same_repo(&request.repo, &candidate.repo)
        || pull_request.head_sha != candidate.head_sha
        || pull_request.number != candidate.pull_request
        || !same_repo(&pull_request.repo, &candidate.repo)
    {
        return Err(ReviewProofError::InvalidIntent);
    }
    let issue = LinearIssueRef::Identifier {
        identifier: crate::tools::LinearIssueIdentifier(candidate.issue_identifier.clone()),
    };
    let instructions = format!(
        "Review the verified candidate for issue {} (tracker ID {}) in {}/{}/{}: PR #{} ({}), base {}, exact head {}. Select exactly {} distinct independent existing repository specialists. The configured count must not be reduced when capacity is insufficient; report the shortage and keep review blocked. Exclude issue implementers and execution-team members. Use native admission and native run references for completion. The configured review timeout is {} ms and require_all_green is {}. Review requests do not authorize a merge. Keep the candidate and request key unchanged when retrying an uncertain launch.",
        candidate.issue_identifier,
        candidate.issue_id.0,
        candidate.repo.hostname(),
        candidate.repo.owner.0,
        candidate.repo.name.0,
        candidate.pull_request.0,
        pull_request.url,
        candidate.base_branch.0,
        candidate.head_sha.0,
        config.reviewer_count,
        config.timeout_ms,
        config.require_all_green
    );
    Ok(IndependentReviewStartRequest {
        issue: issue.clone(),
        spawn_input: SpawnOrResumeExecutionInput {
            issue,
            workspace_key: crate::tools::WorkspaceKey(candidate.issue_identifier),
            template_id: crate::tools::MobTemplateId("independent_review".into()),
            worktree_path: None,
            instructions: Some(instructions),
            idempotency_key: request.idempotency_key,
        },
        pull_request,
        orchestrator_profile: config.orchestrator_profile.clone(),
    })
}

/// The caller selects members, not a review request identity or verdict. The
/// host resolves the candidate and atomically creates the current request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewAdmissionRequest {
    pub issue: LinearIssueRef,
    pub repo: crate::tools::GitHubRepoRef,
    pub pull_request: crate::tools::GitHubPullRequestNumber,
    pub head_sha: GitSha,
    pub reviewers: meerkat_mob::FlowRoleTargets,
    pub task: String,
    pub idempotency_key: IdempotencyKey,
    #[serde(default)]
    pub supersedes: Option<String>,
}

pub fn independent_review_admission_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "issue": {"type":"object", "description":"Configured tracker issue reference: kind id with id, or kind identifier with identifier."},
            "repo": {"type":"object", "properties":{"host":{"type":"string"}, "owner":{"type":"string"}, "name":{"type":"string"}}, "required":["owner","name"], "additionalProperties":false},
            "pull_request": {"type":"integer", "minimum":1},
            "head_sha": {"type":"string", "description":"Expected immutable PR head; host rechecks it."},
            "reviewers": {"type":"object", "minProperties":1,
                "description":"Map existing native profile names to selected existing specialist identities. Use the repository specialist directory and native roster. Never create replacement reviewers.",
                "additionalProperties":{"type":"array", "minItems":1, "uniqueItems":true, "items":{"type":"string", "minLength":1}}},
            "task": {"type":"string", "minLength":1},
            "idempotency_key": {"type":"string", "minLength":1, "description":"Reuse the same key and unchanged assignment after an uncertain response."},
            "supersedes": {"type":["string","null"], "description":"Current request ID to replace explicitly. Omit only when no request exists."}
        },
        "required":["issue","repo","pull_request","head_sha","reviewers","task","idempotency_key"],
        "additionalProperties":false
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewCompletionRequest {
    pub issue: LinearIssueRef,
    pub pull_request: GitHubPullRequestSnapshot,
    pub head_sha: GitSha,
    pub workgraph_scope: IndependentReviewWorkGraphScope,
    pub native_review: crate::native_review::ReviewReference,
    pub idempotency_key: IdempotencyKey,
}

impl IndependentReviewCompletionRequest {
    pub fn validate(&self) -> Result<(), IndependentReviewVerdictError> {
        self.workgraph_scope.validate()?;
        if self.head_sha.0.trim().is_empty() || self.pull_request.head_sha != self.head_sha {
            return Err(IndependentReviewVerdictError::MismatchedHead);
        }
        if self.native_review.runs.is_empty() {
            return Err(IndependentReviewVerdictError::MissingField(
                "native_review.runs",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewWorkGraphScope {
    pub realm_id: String,
    pub namespace: String,
}

impl IndependentReviewWorkGraphScope {
    pub fn validate_for_mob(&self, mob_id: &str) -> Result<(), IndependentReviewVerdictError> {
        self.validate()?;
        let realm = meerkat_core::mob_realm_id(mob_id)
            .map_err(|error| IndependentReviewVerdictError::InvalidScope(error.to_string()))?;
        if self.realm_id != realm.as_str()
            || self.namespace != meerkat::WorkNamespace::default().as_str()
        {
            return Err(IndependentReviewVerdictError::InvalidScope(
                "review obligations must belong to the repository's native mob realm and default namespace".into(),
            ));
        }
        meerkat::WorkNamespace::new(self.namespace.clone())
            .map_err(|error| IndependentReviewVerdictError::InvalidScope(error.to_string()))?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), IndependentReviewVerdictError> {
        if self.realm_id.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField(
                "workgraph_scope.realm_id",
            ));
        }
        if self.namespace.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField(
                "workgraph_scope.namespace",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndependentReviewVerdictStatus {
    Green,
    Red,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewFinding {
    pub severity: IndependentReviewFindingSeverity,
    pub title: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndependentReviewFindingSeverity {
    Blocker,
    Major,
    Minor,
    Note,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewEvidence {
    pub base: String,
    pub head_sha: GitSha,
    #[serde(default)]
    pub commands: Vec<IndependentReviewCommand>,
    #[serde(default)]
    pub checked_paths: Vec<String>,
}

/// Native reviewers can retain a command alone or its observed result. These
/// are supporting evidence, not an alternative verdict or approval source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndependentReviewCommand {
    Command(String),
    Observation(IndependentReviewCommandObservation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReviewCommandObservation {
    pub command: String,
    pub result: String,
}

impl IndependentReviewVerdict {
    pub fn validate(&self) -> Result<(), IndependentReviewVerdictError> {
        if self.schema_version != INDEPENDENT_REVIEW_SCHEMA_VERSION {
            return Err(IndependentReviewVerdictError::UnsupportedSchema {
                schema_version: self.schema_version.clone(),
            });
        }
        if self.reviewer_id.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField("reviewer_id"));
        }
        if self.focus.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField("focus"));
        }
        if self.summary.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField("summary"));
        }
        if self.evidence.base.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField("evidence.base"));
        }
        if self.evidence.head_sha.0.trim().is_empty() {
            return Err(IndependentReviewVerdictError::MissingField(
                "evidence.head_sha",
            ));
        }
        if self.verdict == IndependentReviewVerdictStatus::Green && !self.findings.is_empty() {
            return Err(IndependentReviewVerdictError::GreenWithFindings);
        }
        if self.verdict == IndependentReviewVerdictStatus::Red && self.findings.is_empty() {
            return Err(IndependentReviewVerdictError::RedWithoutFindings);
        }
        for finding in &self.findings {
            if finding.title.trim().is_empty() {
                return Err(IndependentReviewVerdictError::MissingField("finding.title"));
            }
            if finding.detail.trim().is_empty() {
                return Err(IndependentReviewVerdictError::MissingField(
                    "finding.detail",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndependentReviewVerdictError {
    UnsupportedSchema { schema_version: String },
    MissingField(&'static str),
    GreenWithFindings,
    RedWithoutFindings,
    MismatchedHead,
    InvalidScope(String),
}

impl std::fmt::Display for IndependentReviewVerdictError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidScope(message) => write!(formatter, "invalid WorkGraph scope: {message}"),
            Self::UnsupportedSchema { schema_version } => write!(
                formatter,
                "unsupported schema_version {schema_version:?}; expected {INDEPENDENT_REVIEW_SCHEMA_VERSION:?}"
            ),
            Self::MissingField(field) => write!(formatter, "missing required field {field}"),
            Self::GreenWithFindings => {
                formatter.write_str("green verdict must not include findings")
            }
            Self::RedWithoutFindings => formatter.write_str("red verdict must include findings"),
            Self::MismatchedHead => formatter
                .write_str("PR snapshot and every verdict must match the reviewed head SHA"),
        }
    }
}

impl std::error::Error for IndependentReviewVerdictError {}

pub fn independent_review_completion_input_schema() -> Value {
    json!({
        "type": "object",
        "description": "Complete independent review using native flow references. The host reads producer results from durable native records and rechecks the current repository, issue and PR head. Do not submit verdicts or timeout claims.",
        "properties": {
            "issue": {"type": "object", "description": "The repository's tracker issue reference."},
            "pull_request": {"type": "object", "description": "PR selector and expected snapshot; the host fetches current forge state."},
            "head_sha": {"type": "string", "description": "Exact reviewed PR head SHA."},
            "workgraph_scope": {
                "type": "object",
                "description": "The repository's native mob realm and default WorkGraph namespace.",
                "properties": {"realm_id": {"type": "string", "minLength": 1}, "namespace": {"type": "string", "minLength": 1}},
                "required": ["realm_id", "namespace"], "additionalProperties": false
            },
            "native_review": {
                "type": "object", "properties": {
                    "runs": {"type": "array", "minItems": 1, "maxItems": 255, "uniqueItems": true,
                        "items": {"type": "string", "format": "uuid"}}
                },
                "required": ["runs"], "additionalProperties": false
            },
            "idempotency_key": {"type": "string"}
        },
        "required": ["issue", "pull_request", "head_sha", "workgraph_scope", "native_review", "idempotency_key"],
        "additionalProperties": false
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndependentReviewAggregationInput {
    pub config: IndependentReviewConfig,
    pub issue_identifier: String,
    pub head_sha: GitSha,
    pub verdicts: Vec<IndependentReviewVerdict>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndependentReviewAggregationPlan {
    pub decision: IndependentReviewDecision,
    pub target_lane: Option<String>,
    pub consolidated_comment: Option<String>,
    pub workgraph_items: Vec<IndependentReviewWorkGraphItem>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndependentReviewDecision {
    Waiting,
    Success,
    Rework,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndependentReviewWorkGraphItem {
    pub title: String,
    pub detail: String,
    pub labels: Vec<String>,
}

pub fn aggregate_independent_review(
    input: IndependentReviewAggregationInput,
) -> IndependentReviewAggregationPlan {
    let required = usize::from(input.config.reviewer_count);
    if let Some(reason) = input.verdicts.iter().find_map(|verdict| {
        verdict
            .validate()
            .err()
            .map(|error| error.to_string())
            .or_else(|| {
                (verdict.evidence.head_sha != input.head_sha)
                    .then(|| IndependentReviewVerdictError::MismatchedHead.to_string())
            })
    }) {
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Blocked,
            target_lane: Some(input.config.blocked_lane),
            consolidated_comment: None,
            workgraph_items: Vec::new(),
            reasons: vec![reason],
        };
    }
    let distinct_reviewer_ids = distinct_reviewer_ids(&input.verdicts);
    if distinct_reviewer_ids.len() < input.verdicts.len() {
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Blocked,
            target_lane: Some(input.config.blocked_lane),
            consolidated_comment: None,
            workgraph_items: Vec::new(),
            reasons: vec!["duplicate reviewer verdict ids".to_string()],
        };
    }
    if input.verdicts.len() < required && !input.timed_out {
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Waiting,
            target_lane: None,
            consolidated_comment: None,
            workgraph_items: Vec::new(),
            reasons: vec![format!(
                "waiting for {}/{} independent review verdicts",
                input.verdicts.len(),
                required
            )],
        };
    }
    if input.verdicts.len() < required && input.timed_out {
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Blocked,
            target_lane: Some(input.config.blocked_lane),
            consolidated_comment: None,
            workgraph_items: Vec::new(),
            reasons: vec![format!(
                "timed out with {}/{} independent review verdicts",
                input.verdicts.len(),
                required
            )],
        };
    }
    if let Some(blocked) = input
        .verdicts
        .iter()
        .find(|verdict| verdict.verdict == IndependentReviewVerdictStatus::Blocked)
    {
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Blocked,
            target_lane: Some(input.config.blocked_lane),
            consolidated_comment: None,
            workgraph_items: Vec::new(),
            reasons: vec![format!("reviewer {} was blocked", blocked.reviewer_id)],
        };
    }

    let red_findings = input
        .verdicts
        .iter()
        .filter(|verdict| verdict.verdict == IndependentReviewVerdictStatus::Red)
        .flat_map(|verdict| {
            verdict
                .findings
                .iter()
                .map(move |finding| (verdict, finding))
        })
        .collect::<Vec<_>>();
    if !red_findings.is_empty() {
        let labels = vec![
            "fugue".to_string(),
            format!("issue:{}", input.issue_identifier),
            "review".to_string(),
            "blocking".to_string(),
        ];
        let workgraph_items = red_findings
            .iter()
            .map(|(verdict, finding)| IndependentReviewWorkGraphItem {
                title: format!("{}: {}", verdict.reviewer_id, finding.title),
                detail: finding.detail.clone(),
                labels: labels.clone(),
            })
            .collect::<Vec<_>>();
        return IndependentReviewAggregationPlan {
            decision: IndependentReviewDecision::Rework,
            target_lane: Some(input.config.rework_lane.clone()),
            consolidated_comment: Some(independent_review_findings_comment(
                &input.config.findings_comment_marker,
                &input.issue_identifier,
                &input.head_sha,
                &red_findings,
            )),
            workgraph_items,
            reasons: vec!["one or more independent reviewers requested changes".to_string()],
        };
    }

    IndependentReviewAggregationPlan {
        decision: IndependentReviewDecision::Success,
        target_lane: Some(input.config.success_lane),
        consolidated_comment: None,
        workgraph_items: Vec::new(),
        reasons: vec!["all independent reviewers are green".to_string()],
    }
}

fn distinct_reviewer_ids(verdicts: &[IndependentReviewVerdict]) -> Vec<&str> {
    let mut ids = Vec::new();
    for verdict in verdicts {
        if !ids.contains(&verdict.reviewer_id.as_str()) {
            ids.push(verdict.reviewer_id.as_str());
        }
    }
    ids
}

fn independent_review_findings_comment(
    marker: &str,
    issue_identifier: &str,
    head_sha: &GitSha,
    findings: &[(&IndependentReviewVerdict, &IndependentReviewFinding)],
) -> String {
    struct Findings<'a>(&'a [(&'a IndependentReviewVerdict, &'a IndependentReviewFinding)]);

    impl std::fmt::Display for Findings<'_> {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for (index, (verdict, finding)) in self.0.iter().enumerate() {
                if index > 0 {
                    formatter.write_str("\n")?;
                }
                write!(
                    formatter,
                    "- [{}] {}: {} -- {}",
                    verdict.reviewer_id,
                    format!("{:?}", finding.severity).to_lowercase(),
                    finding.title,
                    finding.detail
                )?;
            }
            Ok(())
        }
    }

    format!(
        "{marker}\n\nIssue: {issue_identifier}\nHead SHA: {}\n\n{}",
        head_sha.0,
        Findings(findings)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_review_launch_uses_canonical_issue_and_host_profile() {
        use crate::tools::*;
        let config = IndependentReviewConfig {
            enabled: true,
            orchestrator_profile: "configured_review_coordinator".into(),
            ..Default::default()
        };
        let request: IndependentReviewLaunchRequest = serde_json::from_value(json!({
            "issue":{"kind":"id","id":"tracker-row-17"},
            "repo":{"owner":"example","name":"app"},
            "pull_request":7,"head_sha":"a".repeat(40),
            "idempotency_key":"review-start-1"
        }))
        .unwrap();
        let candidate = crate::native_review::ReviewCandidate {
            tracker_context: "private-tracker-context".into(),
            issue_id: LinearIssueId("tracker-row-17".into()),
            issue_identifier: "TEST-17".into(),
            repo: request.repo.clone(),
            pull_request: request.pull_request,
            base_branch: BranchName("main".into()),
            head_sha: request.head_sha.clone(),
        };
        let pr = GitHubPullRequestSnapshot {
            repo: candidate.repo.clone(),
            number: candidate.pull_request,
            title: "Candidate".into(),
            url: "https://github.com/example/app/pull/7".into(),
            state: PullRequestState::Open,
            draft: false,
            head_branch: BranchName("feature/test-17".into()),
            base_branch: candidate.base_branch.clone(),
            head_sha: candidate.head_sha.clone(),
        };
        let launch =
            configured_review_launch(&config, request.clone(), candidate.clone(), pr.clone())
                .unwrap();
        assert_eq!(launch.spawn_input.workspace_key.0, "TEST-17");
        assert_eq!(launch.spawn_input.issue, launch.issue);
        assert_eq!(launch.orchestrator_profile, "configured_review_coordinator");
        assert!(launch.spawn_input.worktree_path.is_none());
        let instructions = launch.spawn_input.instructions.as_deref().unwrap();
        assert!(instructions.contains("issue TEST-17"));
        assert!(instructions.contains("github.com/example/app: PR #7"));
        assert!(instructions.contains(&format!("exact head {}", candidate.head_sha.0)));
        assert!(
            instructions.contains("exactly 3 distinct independent existing repository specialists")
        );
        assert_eq!(launch.spawn_input.idempotency_key, request.idempotency_key);
        // The orchestrator resolves the workspace before this real constructor.
        let mut input = launch.spawn_input.clone();
        input.worktree_path = Some("/tmp/fugue-review/TEST-17".into());
        let template =
            crate::kapellmeister_host::independent_review_native_launch_template(&launch, &[]);
        crate::mob_tools::NativeMobLaunchPlan::new(
            input,
            template,
            Default::default(),
            Default::default(),
            None,
            Some(candidate.repo.clone()),
        )
        .unwrap();

        let replay =
            configured_review_launch(&config, request.clone(), candidate.clone(), pr.clone())
                .unwrap();
        assert_eq!(launch, replay);
        let mut changed = request.clone();
        changed.head_sha = GitSha("b".repeat(40));
        assert!(configured_review_launch(&config, changed, candidate.clone(), pr.clone()).is_err());
        let mut empty_key = request.clone();
        empty_key.idempotency_key = IdempotencyKey(" ".into());
        assert!(
            configured_review_launch(&config, empty_key, candidate.clone(), pr.clone()).is_err()
        );
        assert!(
            configured_review_launch(&IndependentReviewConfig::default(), request, candidate, pr)
                .is_err()
        );
    }

    fn green_verdict() -> IndependentReviewVerdict {
        IndependentReviewVerdict {
            schema_version: INDEPENDENT_REVIEW_SCHEMA_VERSION.to_string(),
            reviewer_id: "runtime-boundary".to_string(),
            focus: "Meta runtime boundary".to_string(),
            verdict: IndependentReviewVerdictStatus::Green,
            summary: "No blocking findings.".to_string(),
            findings: Vec::new(),
            evidence: IndependentReviewEvidence {
                base: "main".to_string(),
                head_sha: GitSha("abc123".to_string()),
                commands: vec![IndependentReviewCommand::Command(
                    "git diff origin/main...HEAD".into(),
                )],
                checked_paths: vec!["src/meta_runtime.rs".to_string()],
            },
        }
    }

    fn red_verdict() -> IndependentReviewVerdict {
        let mut verdict = green_verdict();
        verdict.reviewer_id = "tests".to_string();
        verdict.focus = "Test coverage".to_string();
        verdict.verdict = IndependentReviewVerdictStatus::Red;
        verdict.summary = "Missing regression coverage.".to_string();
        verdict.findings = vec![IndependentReviewFinding {
            severity: IndependentReviewFindingSeverity::Major,
            title: "Missing regression test".to_string(),
            detail: "The PR changes lane classification without a focused test.".to_string(),
            file: Some("src/linear_lane_state.rs".to_string()),
            line: Some(120),
        }];
        verdict
    }

    fn aggregation_config() -> IndependentReviewConfig {
        IndependentReviewConfig {
            enabled: true,
            ..IndependentReviewConfig::default()
        }
    }

    fn aggregation_input(
        verdicts: Vec<IndependentReviewVerdict>,
    ) -> IndependentReviewAggregationInput {
        IndependentReviewAggregationInput {
            config: aggregation_config(),
            issue_identifier: "LUC-631".to_string(),
            head_sha: GitSha("abc123".to_string()),
            verdicts,
            timed_out: false,
        }
    }

    #[test]
    fn independent_review_verdict_round_trips_with_typed_status() {
        let verdict = green_verdict();

        let encoded = serde_json::to_string(&verdict).unwrap();
        let decoded: IndependentReviewVerdict = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, verdict);
        decoded.validate().unwrap();
    }

    #[test]
    fn independent_review_verdict_rejects_red_without_findings() {
        let mut verdict = green_verdict();
        verdict.verdict = IndependentReviewVerdictStatus::Red;

        assert_eq!(
            verdict.validate(),
            Err(IndependentReviewVerdictError::RedWithoutFindings)
        );
    }

    #[test]
    fn independent_review_verdict_rejects_unknown_schema() {
        let mut verdict = green_verdict();
        verdict.schema_version = "fugue.review.v0".to_string();

        assert_eq!(
            verdict.validate(),
            Err(IndependentReviewVerdictError::UnsupportedSchema {
                schema_version: "fugue.review.v0".to_string()
            })
        );
    }

    #[test]
    fn independent_review_completion_schema_accepts_references_not_verdicts() {
        let schema = independent_review_completion_input_schema();
        assert!(schema.pointer("/properties/verdicts").is_none());
        assert!(schema.pointer("/properties/timed_out").is_none());
        assert_eq!(schema["additionalProperties"], false);
        assert!(
            schema
                .pointer("/required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|value| value == "native_review"))
        );
        assert_eq!(
            schema
                .pointer("/properties/workgraph_scope/required/0")
                .and_then(Value::as_str),
            Some("realm_id")
        );
        assert!(
            schema
                .pointer("/required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|value| value == "workgraph_scope"))
        );
    }

    #[test]
    fn completion_schema_accepts_four_distinct_profile_runs() {
        let schema = independent_review_completion_input_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let input = json!({
            "issue": {"kind":"id", "id":"10001"}, "pull_request": {},
            "head_sha":"a".repeat(40), "idempotency_key":"four-profiles",
            "workgraph_scope":{"realm_id":"repository", "namespace":"default"},
            "native_review":{"runs":(1..=4).map(uuid::Uuid::from_u128).collect::<Vec<_>>()}
        });
        assert!(
            validator.is_valid(&input),
            "four configured profiles need four native run references"
        );
    }

    #[test]
    fn independent_review_aggregation_waits_for_three_verdicts() {
        let plan = aggregate_independent_review(aggregation_input(vec![green_verdict()]));

        assert_eq!(plan.decision, IndependentReviewDecision::Waiting);
        assert!(plan.target_lane.is_none());
    }

    #[test]
    fn independent_review_aggregation_all_green_moves_to_success_lane() {
        let mut second = green_verdict();
        second.reviewer_id = "product".to_string();
        let mut third = green_verdict();
        third.reviewer_id = "ops".to_string();

        let plan =
            aggregate_independent_review(aggregation_input(vec![green_verdict(), second, third]));

        assert_eq!(plan.decision, IndependentReviewDecision::Success);
        assert_eq!(plan.target_lane.as_deref(), Some("Ready for review"));
    }

    #[test]
    fn independent_review_cannot_approve_a_different_commit() {
        let mut second = green_verdict();
        second.reviewer_id = "product".into();
        second.evidence.head_sha = GitSha("previous-head".into());
        let mut third = green_verdict();
        third.reviewer_id = "ops".into();
        let plan =
            aggregate_independent_review(aggregation_input(vec![green_verdict(), second, third]));
        assert_eq!(plan.decision, IndependentReviewDecision::Blocked);
        assert!(plan.reasons.iter().any(|reason| reason.contains("head")));
    }

    #[test]
    fn independent_review_aggregation_red_creates_comment_and_workgraph_items() {
        let mut third = green_verdict();
        third.reviewer_id = "ops".to_string();

        let plan = aggregate_independent_review(aggregation_input(vec![
            green_verdict(),
            red_verdict(),
            third,
        ]));

        assert_eq!(plan.decision, IndependentReviewDecision::Rework);
        assert_eq!(plan.target_lane.as_deref(), Some("Rework"));
        assert_eq!(plan.workgraph_items.len(), 1);
        assert!(plan.consolidated_comment.unwrap().contains("LUC-631"));
    }

    #[test]
    fn independent_review_aggregation_timeout_blocks() {
        let mut input = aggregation_input(vec![green_verdict()]);
        input.timed_out = true;

        let plan = aggregate_independent_review(input);

        assert_eq!(plan.decision, IndependentReviewDecision::Blocked);
        assert_eq!(plan.target_lane.as_deref(), Some("Blocked"));
    }
}
