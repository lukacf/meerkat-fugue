//! Native review completion shared by manual review and the final merge gate.
//! Runtime producer records supply the evidence; this module applies configured
//! projection and merge policy without scheduling reviewer turns.

use crate::config::ServiceConfig;
use crate::independent_review::{
    IndependentReviewAggregationInput, IndependentReviewAggregationPlan,
    IndependentReviewCompletionRequest, IndependentReviewDecision, IndependentReviewWorkGraphScope,
    aggregate_independent_review,
};
use crate::linear_adapter::IssueAdapter;
use crate::meta_runtime::{MetaRuntimeRegistry, meta_runtime_state_path};
use crate::tools::{
    AddCommentInput, FugueToolOperation, FugueToolOutput, FugueToolResult, GitHubPullRequestRef,
    IdempotencyKey, InspectIssueInput, LinearIssueRef, LinearStateRef, MergePrInput, MergePrOutput,
    MoveIssueInput, UpsertWorkpadInput,
};
use meerkat::{
    CreateWorkItemRequest, SqliteWorkGraphStore, WorkCompletionPolicy, WorkGraphService,
    WorkItemFilter, WorkNamespace, WorkPriority, WorkStatus,
};
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReviewCompletionKind {
    Independent,
    Final,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct CompletionError {
    pub code: &'static str,
    pub message: String,
}
fn failure(code: &'static str, error: impl std::fmt::Display) -> CompletionError {
    CompletionError {
        code,
        message: error.to_string(),
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CompletedReview {
    pub accepted: bool,
    pub issue_identifier: String,
    pub plan: IndependentReviewAggregationPlan,
    pub projection: IndependentReviewProjectionResult,
    pub merge: Option<MergePrOutput>,
}

pub(crate) async fn complete(
    config: &ServiceConfig,
    registry: &MetaRuntimeRegistry,
    mut request: IndependentReviewCompletionRequest,
    kind: ReviewCompletionKind,
    host: crate::github_tools::GitHubToolHost,
) -> Result<CompletedReview, CompletionError> {
    let enabled = match kind {
        ReviewCompletionKind::Independent => config.independent_review.enabled,
        ReviewCompletionKind::Final => config.final_review.enabled,
    };
    if !enabled {
        return Err(failure(
            "review_disabled",
            "This review procedure is disabled for the repository.",
        ));
    }
    request
        .validate()
        .and_then(|()| {
            request
                .workgraph_scope
                .validate_for_mob(&config.meerkat.mob_id())
        })
        .map_err(|error| failure("invalid_review_completion", error))?;
    let purpose = match kind {
        ReviewCompletionKind::Independent => crate::native_review::CandidateUse::Review,
        ReviewCompletionKind::Final => crate::native_review::CandidateUse::Merge,
    };
    let (candidate, pull_request) = crate::native_review::resolve_candidate_with_host(
        config,
        request.issue.clone(),
        &request.pull_request.repo,
        GitHubPullRequestRef::Number {
            number: request.pull_request.number,
        },
        purpose,
        host.clone(),
    )
    .await
    .map_err(|error| failure("review_candidate_unavailable", error))?;
    if candidate.head_sha != request.head_sha {
        return Err(failure(
            "review_candidate_changed",
            "The current PR head differs from the reviewed candidate.",
        ));
    }
    let proof = registry
        .read_native_review(&candidate, &request.native_review)
        .await
        .map_err(|error| failure("native_review_not_verified", error))?;
    let mut aggregation = config.independent_review.clone();
    if kind == ReviewCompletionKind::Final {
        let count = crate::native_review::final_review_count(
            &config.final_review,
            &proof.intent().reviewers,
        )
        .map_err(|error| failure("final_review_profiles_not_satisfied", error))?;
        aggregation.reviewer_count =
            u8::try_from(count).map_err(|error| failure("invalid_review_count", error))?;
        aggregation.success_lane = config.final_review.success_lane.clone();
        aggregation.rework_lane = config.final_review.rework_lane.clone();
        aggregation.blocked_lane = config.final_review.blocked_lane.clone();
        aggregation.enabled = true;
        aggregation.require_all_green = true;
    }
    request.issue = LinearIssueRef::Id {
        id: candidate.issue_id,
    };
    request.pull_request = pull_request;
    let verdicts: Vec<_> = proof.verdicts().cloned().collect();
    let mut plan = aggregate_independent_review(IndependentReviewAggregationInput {
        config: aggregation,
        issue_identifier: candidate.issue_identifier.clone(),
        head_sha: request.head_sha.clone(),
        verdicts: verdicts.clone(),
        timed_out: false,
    });
    if plan.decision == IndependentReviewDecision::Waiting {
        return Err(failure(
            "native_review_incomplete",
            "Native review does not yet have every required result.",
        ));
    }
    let mut merged = None;
    if kind == ReviewCompletionKind::Final {
        let issue = IssueAdapter::from_config(&config.tracker)
            .map_err(|error| failure("review_candidate_unavailable", error))?
            .inspect_issue(InspectIssueInput {
                issue: request.issue.clone(),
            })
            .await
            .map_err(|error| failure("review_candidate_unavailable", error))?
            .issue;
        let context = crate::kapellmeister::KapellmeisterDecisionContext {
            issue: Some(request.issue.clone()),
            repository: Some(candidate.repo),
            linear_labels: issue.labels,
            workspace_key: None,
            observations: Vec::new(),
        };
        let mut operations = vec![
            FugueToolOperation::LinearUpsertWorkpad,
            FugueToolOperation::LinearMoveIssue,
        ];
        if plan.decision == IndependentReviewDecision::Success {
            operations.push(FugueToolOperation::GitHubMergePr);
        }
        crate::kapellmeister_host::ensure_guarded_operations(
            &config.kapellmeister.shadow_mode.authority,
            &context,
            &operations,
        )
        .map_err(|error| failure("final_review_not_authorized", error))?;
        if plan.decision == IndependentReviewDecision::Success {
            // No current-request transaction is held here. The common merge gate
            // owns it and also binds the operation to this exact completed intent.
            let input = MergePrInput {
                repo: request.pull_request.repo.clone(),
                pull_request: GitHubPullRequestRef::Number {
                    number: request.pull_request.number,
                },
                expected_head_sha: Some(request.head_sha.clone()),
                method: config.final_review.merge.strategy.into(),
                idempotency_key: IdempotencyKey(format!(
                    "final-review-merge:{}",
                    request.idempotency_key.0
                )),
            };
            match crate::merge_gate::merge_review_with_host(
                config,
                registry,
                request.issue.clone(),
                input,
                crate::merge_gate::MergePurpose::FinalReview,
                host,
                Some(proof.intent()),
            )
            .await
            {
                FugueToolResult::Ok {
                    output: FugueToolOutput::GitHubMergePr(output),
                    ..
                } => merged = Some(output),
                result => {
                    let reason = match result {
                        FugueToolResult::Err { error, .. } => error.message,
                        _ => "The merge operation did not return confirmed merge evidence.".into(),
                    };
                    plan.decision = IndependentReviewDecision::Blocked;
                    plan.target_lane = Some(config.final_review.blocked_lane.clone());
                    plan.reasons
                        .push(format!("Merge could not be confirmed: {reason}"));
                }
            }
        }
    }
    let runtime = tokio::runtime::Handle::current();
    let projection_config = config.clone();
    let projection_issue = candidate.issue_identifier.clone();
    let projection_plan = plan.clone();
    let projection_merge = merged.clone();
    let projection_registry = registry.clone();
    let intent = proof.intent().clone();
    let projection = registry
        .with_current_review(intent.clone(), move || {
            projection_registry
                .verify_review_independence(&intent)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            runtime.block_on(record_review_projection(
                &projection_config,
                &request,
                &projection_issue,
                &projection_plan,
                &verdicts,
                projection_merge.as_ref(),
                kind,
            ))
        })
        .await
        .map_err(|error| failure("review_request_changed", error))?
        .map_err(|error| failure("review_projection_failed", error))?;
    Ok(CompletedReview {
        accepted: true,
        issue_identifier: candidate.issue_identifier,
        plan,
        projection,
        merge: merged,
    })
}

#[derive(Debug, Serialize)]
pub(crate) struct IndependentReviewProjectionResult {
    pub(crate) target_lane: String,
    pub(crate) workpad_comment_id: String,
    pub(crate) findings_comment_id: Option<String>,
    pub(crate) workgraph_items: Vec<IndependentReviewWorkGraphItemRef>,
}

#[derive(Debug, Serialize)]
pub(crate) struct IndependentReviewWorkGraphItemRef {
    pub(crate) id: String,
    pub(crate) realm_id: String,
    pub(crate) namespace: String,
}

pub(crate) async fn record_review_projection(
    config: &ServiceConfig,
    request: &IndependentReviewCompletionRequest,
    issue_identifier: &str,
    plan: &crate::independent_review::IndependentReviewAggregationPlan,
    verdicts: &[crate::independent_review::IndependentReviewVerdict],
    merged: Option<&MergePrOutput>,
    kind: ReviewCompletionKind,
) -> anyhow::Result<IndependentReviewProjectionResult> {
    request.validate()?;
    for verdict in verdicts {
        verdict.validate()?;
        if verdict.evidence.head_sha != request.head_sha {
            anyhow::bail!("native review verdict and PR must name the same head");
        }
    }
    request
        .workgraph_scope
        .validate_for_mob(&config.meerkat.mob_id())?;
    let target_lane = plan
        .target_lane
        .clone()
        .ok_or_else(|| anyhow::anyhow!("independent review projection has no target lane"))?;
    let adapter = IssueAdapter::from_config(&config.tracker)?;
    let canonical = adapter
        .inspect_issue(InspectIssueInput {
            issue: request.issue.clone(),
        })
        .await?;
    if canonical.issue.identifier.0 != issue_identifier {
        anyhow::bail!("review obligation labels must use the tracker's canonical issue identifier");
    }
    let label = match kind {
        ReviewCompletionKind::Independent => "Independent Review",
        ReviewCompletionKind::Final => "Final Review",
    };
    let review_summary = serde_json::to_string_pretty(&json!({
        "schema_version": crate::independent_review::INDEPENDENT_REVIEW_SCHEMA_VERSION,
        "issue_identifier": issue_identifier,
        "pull_request": request.pull_request,
        "head_sha": request.head_sha,
        "decision": plan.decision,
        "target_lane": target_lane,
        "reasons": plan.reasons,
        "native_review": request.native_review,
        "verdicts": verdicts,
        "timed_out": false,
        "merge": merged,
    }))?;
    let workpad = adapter
        .upsert_workpad(UpsertWorkpadInput {
            issue: request.issue.clone(),
            heading: "## Fugue Workpad".to_string(),
            body_markdown: format!("### {label}\n\n```json\n{review_summary}\n```"),
            create_if_missing: true,
            idempotency_key: IdempotencyKey(format!(
                "independent-review-workpad:{}:{}",
                issue_identifier, request.idempotency_key.0
            )),
        })
        .await?;

    let workgraph_projection =
        create_independent_review_workgraph_items(config, &request.workgraph_scope, plan).await?;

    let mut findings_comment_id = None;
    if let Some(comment) = &plan.consolidated_comment
        && (workgraph_projection.created_any || workgraph_projection.item_refs.is_empty())
    {
        let comment = adapter
            .add_comment(AddCommentInput {
                issue: request.issue.clone(),
                body_markdown: format!(
                    "{}\n\nIdempotency-Key: `{}`",
                    comment, request.idempotency_key.0
                ),
                attachments: Vec::new(),
            })
            .await?;
        findings_comment_id = Some(comment.comment_id.0);
    }

    let movement = MoveIssueInput {
        issue: request.issue.clone(),
        state: LinearStateRef::Name {
            name: target_lane.clone(),
        },
        expected_updated_at: None,
        idempotency_key: IdempotencyKey(format!(
            "independent-review-move:{}:{}",
            issue_identifier, request.idempotency_key.0
        )),
    };
    // Work can arrive after merge. Recheck before publishing terminal success,
    // preserving the recorded merge evidence if this projection must be retried.
    let mut graph_gate = crate::workgraph_gate::WorkGraphTerminalGate::for_service(config);
    if kind == ReviewCompletionKind::Final {
        graph_gate
            .terminal_states
            .push(config.final_review.success_lane.clone());
        graph_gate
            .allowed_terminal_states
            .push(config.final_review.blocked_lane.clone());
    }
    graph_gate
        .guard_terminal_move(&config.tracker, &movement)
        .await
        .map_err(anyhow::Error::msg)?;
    crate::linear_adapter::move_issue_with_observation(config, movement).await?;

    Ok(IndependentReviewProjectionResult {
        target_lane,
        workpad_comment_id: workpad.comment_id.0,
        findings_comment_id,
        workgraph_items: workgraph_projection.item_refs,
    })
}

struct IndependentReviewWorkGraphProjection {
    item_refs: Vec<IndependentReviewWorkGraphItemRef>,
    created_any: bool,
}

async fn create_independent_review_workgraph_items(
    config: &ServiceConfig,
    scope: &IndependentReviewWorkGraphScope,
    plan: &crate::independent_review::IndependentReviewAggregationPlan,
) -> anyhow::Result<IndependentReviewWorkGraphProjection> {
    if plan.workgraph_items.is_empty() {
        return Ok(IndependentReviewWorkGraphProjection {
            item_refs: Vec::new(),
            created_any: false,
        });
    }
    scope.validate()?;
    let workgraph_path = meta_runtime_state_path(&config.meerkat).join("workgraph.sqlite3");
    if let Some(parent) = workgraph_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let store = SqliteWorkGraphStore::open(&workgraph_path)?;
    let namespace = WorkNamespace::new(scope.namespace.clone())?;
    let service = WorkGraphService::with_scope(Arc::new(store), &scope.realm_id, namespace.clone());
    let mut item_refs = Vec::new();
    let mut created_any = false;
    for item in &plan.workgraph_items {
        let labels = item.labels.iter().cloned().collect::<BTreeSet<_>>();
        let existing = service
            .list(WorkItemFilter {
                realm_id: Some(scope.realm_id.clone()),
                namespace: Some(namespace.clone()),
                all_namespaces: false,
                statuses: Vec::new(),
                labels: item.labels.clone(),
                include_terminal: false,
                limit: None,
            })
            .await?
            .into_iter()
            .find(|existing| {
                existing.title == item.title
                    && existing.description.as_deref() == Some(item.detail.as_str())
            });
        let work_item = match existing {
            Some(existing) => existing,
            None => {
                created_any = true;
                service
                    .create(CreateWorkItemRequest {
                        realm_id: Some(scope.realm_id.clone()),
                        namespace: Some(namespace.clone()),
                        title: item.title.clone(),
                        description: Some(item.detail.clone()),
                        priority: WorkPriority::High,
                        completion_policy: WorkCompletionPolicy::SelfAttest,
                        failed_child_join_policy: Default::default(),
                        cancelled_child_join_policy: Default::default(),
                        labels,
                        due_at: None,
                        not_before: None,
                        snoozed_until: None,
                        external_refs: Vec::new(),
                        evidence_refs: Vec::new(),
                        status: Some(WorkStatus::Open),
                    })
                    .await?
            }
        };
        item_refs.push(IndependentReviewWorkGraphItemRef {
            id: work_item.id.to_string(),
            realm_id: work_item.realm_id,
            namespace: work_item.namespace.to_string(),
        });
    }
    Ok(IndependentReviewWorkGraphProjection {
        item_refs,
        created_any,
    })
}
