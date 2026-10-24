use super::*;
use chrono::Utc;
use meerkat_mob::ids::AgentRuntimeId;
use serde_json::json;

fn candidate_config(root: &std::path::Path) -> crate::config::ServiceConfig {
    crate::config::ServiceConfig::from_definition(
        root.join("fugue.yaml"),
        &crate::config::WorkflowDefinition {
            config: json!({
                "tracker": {"kind":"fugue-ims", "endpoint":root.join("issues.sqlite"), "project_slug":"example/app"},
                "deploy": {"allowed_repositories":[{"owner":"example", "name":"app"}]}
            }).as_object().unwrap().clone(),
            prompt_template: "test".into(),
        },
    ).unwrap()
}

fn candidate_pr() -> crate::tools::GitHubPullRequestSnapshot {
    serde_json::from_value(json!({
        "repo":{"owner":"example", "name":"app"}, "number":1,
        "title":"Test candidate", "url":"https://github.com/example/app/pull/1",
        "state":"open", "draft":false, "head_branch":"feature", "base_branch":"main", "head_sha":"a".repeat(40)
    })).unwrap()
}

#[tokio::test]
async fn native_review_candidate_uses_canonical_issue_and_configured_repository() {
    use crate::tools::{GitHubPullRequestRef, LinearIssueIdentifier, LinearIssueRef};
    let temp = tempfile::tempdir().unwrap();
    let mut config = candidate_config(temp.path());
    crate::fugue_ims::create_issue_for_config(
        &config.tracker,
        crate::fugue_ims::CreateFugueIssue {
            key: crate::ims::ImsIssueKey("TEST-1".into()),
            repo: "example/app".into(),
            title: "Review candidate".into(),
            description: None,
            stage: crate::ims::ImsStageName("Todo".into()),
            labels: vec![],
        },
    )
    .unwrap();
    let issue = LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier("TEST-1".into()),
    };
    let pr = candidate_pr();
    let selector = GitHubPullRequestRef::Number { number: pr.number };
    let (candidate, snapshot) = resolve_candidate_with(
        &config,
        issue.clone(),
        &pr.repo,
        selector.clone(),
        |input| {
            assert_eq!(input.repo, pr.repo);
            std::future::ready(Ok(pr.clone()))
        },
    )
    .await
    .unwrap();
    assert_eq!(candidate.issue_identifier, "TEST-1");
    let canonical = crate::linear_adapter::IssueAdapter::from_config(&config.tracker)
        .unwrap()
        .inspect_issue(crate::tools::InspectIssueInput {
            issue: issue.clone(),
        })
        .await
        .unwrap();
    assert_eq!(candidate.issue_id, canonical.issue.id);
    assert_eq!(candidate.tracker_context, config.tracker_context_id());
    assert_eq!(candidate.head_sha, snapshot.head_sha);

    for change in [
        "host",
        "owner",
        "repository",
        "number",
        "closed",
        "draft",
        "base",
    ] {
        let mut different = pr.clone();
        match change {
            "host" => different.repo.host = Some("ghe.example.test".to_owned().try_into().unwrap()),
            "owner" => different.repo.owner.0 = "other".into(),
            "repository" => different.repo.name.0 = "other".into(),
            "number" => different.number.0 += 1,
            "closed" => different.state = crate::tools::PullRequestState::Closed,
            "base" => different.base_branch.0 = "release".into(),
            _ => different.draft = true,
        }
        let result = resolve_candidate_with(
            &config,
            issue.clone(),
            &pr.repo,
            selector.clone(),
            |_| async { Ok(different) },
        )
        .await;
        assert_eq!(
            result.unwrap_err(),
            ReviewProofError::ScopeMismatch,
            "{change}"
        );
    }
    config.tracker.project_slug = Some("example/other".into());
    assert!(
        resolve_candidate_with(
            &config,
            issue.clone(),
            &pr.repo,
            selector.clone(),
            |_| async { panic!("foreign tracker issue must be refused before calling the forge") }
        )
        .await
        .is_err()
    );
    config.deploy.allowed_repositories.clear();
    assert_eq!(
        resolve_candidate_with(&config, issue, &pr.repo, selector, |_| async {
            panic!("an unconfigured repository must never reach the forge")
        })
        .await
        .unwrap_err(),
        ReviewProofError::ScopeMismatch
    );
}

#[tokio::test]
async fn native_review_candidate_resolves_jira_key_to_immutable_id() {
    use crate::jira::tests::{issue, mock_jira, reply, tracker};
    let mock = mock_jira(vec![
        reply("GET", "/rest/api/3/issue/TEST-1", issue("To Do")),
        reply(
            "GET",
            "/rest/api/3/issue/TEST-1/comment",
            json!({"comments":[], "total":0}),
        ),
    ])
    .await;
    let temp = tempfile::tempdir().unwrap();
    let mut config = candidate_config(temp.path());
    config.tracker = tracker(&mock.endpoint);
    let pr = candidate_pr();
    let (candidate, _) = resolve_candidate_with(
        &config,
        crate::tools::LinearIssueRef::Identifier {
            identifier: crate::tools::LinearIssueIdentifier("TEST-1".into()),
        },
        &pr.repo,
        crate::tools::GitHubPullRequestRef::Number { number: pr.number },
        |_| std::future::ready(Ok(pr.clone())),
    )
    .await
    .unwrap();
    assert_eq!(candidate.issue_id.0, "10001");
    assert_eq!(candidate.issue_identifier, "TEST-1");
    assert_eq!(candidate.tracker_context, config.tracker_context_id());
    assert_eq!(mock.requests.lock().unwrap().len(), 2);
}

struct Fixture {
    definition: MobDefinition,
    intent: ReviewIntent,
    runs: Vec<(MobRun, Vec<MobEvent>)>,
}

impl Fixture {
    fn new() -> Self {
        let reviewers = FlowRoleTargets::new([
            ("code", vec!["review-a".into(), "review-b".into()]),
            ("release", vec!["review-c".into()]),
        ])
        .unwrap();
        let intent = ReviewIntent {
            schema_version: SCHEMA.into(),
            round: "work-1-review-1".into(),
            candidate: ReviewCandidate {
                tracker_context: "site-and-project-fingerprint".into(),
                issue_id: LinearIssueId("10001".into()),
                issue_identifier: "TEST-1".into(),
                repo: GitHubRepoRef {
                    host: None,
                    owner: crate::tools::GitHubOwner("example".into()),
                    name: crate::tools::GitHubRepoName("app".into()),
                },
                pull_request: GitHubPullRequestNumber(1),
                base_branch: BranchName("main".into()),
                head_sha: GitSha("a".repeat(40)),
            },
            reviewers,
            task: "Review the immutable assignment".into(),
        };
        let mut definition: MobDefinition =
            serde_json::from_value(json!({"id":"repository-mob"})).unwrap();
        let mut runs = Vec::new();
        for (role, members) in intent.reviewers.bound_roles() {
            let id = flow_id(role);
            definition.flows.insert(id.clone(), flow(role));
            let mut run = MobRun::pending(
                definition.id.clone(),
                id.clone(),
                MobRun::flow_state_for_steps([StepId::from(STEP)]).unwrap(),
                serde_json::to_value(&intent).unwrap(),
            );
            run.run_id =
                RunId::for_work_execution(&definition.id, &id, &admission_key(&intent).unwrap());
            run.status = MobRunStatus::Completed;
            run.flow_state.step_status.insert(
                STEP.into(),
                Some(serde_json::from_value(json!("Completed")).unwrap()),
            );
            run.role_targets =
                Some(FlowRoleTargets::new([(role.clone(), members.iter().cloned())]).unwrap());
            run.flow_definition_digest = Some(
                FlowRunConfig::from_definition(id, &definition)
                    .unwrap()
                    .definition_digest()
                    .unwrap(),
            );
            let mut aggregate = BTreeMap::new();
            let mut events = Vec::new();
            for identity in members {
                let output = json!({
                    "schema_version":"fugue.review.v1", "reviewer_id":identity,
                    "focus":"regressions", "verdict":"green", "summary":"Checks pass",
                    "findings":[], "evidence":{"base":"main", "head_sha":intent.candidate.head_sha,
                    "commands":["cargo test"], "checked_paths":["src"]}
                });
                let target = AgentRuntimeId::initial(identity.clone());
                events.push(MobEvent {
                    cursor: events.len() as u64 + 1,
                    timestamp: Utc::now(),
                    mob_id: run.mob_id.clone(),
                    kind: MobEventKind::StepDispatched {
                        run_id: run.run_id.clone(),
                        step_id: STEP.into(),
                        target: target.clone(),
                    },
                });
                events.push(MobEvent {
                    cursor: events.len() as u64 + 1,
                    timestamp: Utc::now(),
                    mob_id: run.mob_id.clone(),
                    kind: MobEventKind::StepTargetCompleted {
                        run_id: run.run_id.clone(),
                        step_id: STEP.into(),
                        target,
                        output: Some(output.clone()),
                        remote_turn_obligation: None,
                    },
                });
                aggregate.insert(identity.clone(), output);
            }
            run.root_step_outputs
                .insert(STEP.into(), serde_json::to_value(aggregate).unwrap());
            runs.push((run, events));
        }
        Self {
            definition,
            intent,
            runs,
        }
    }

    fn check(&self) -> Result<VerifiedReview, ReviewProofError> {
        let evidence: Vec<_> = self
            .runs
            .iter()
            .map(|(run, events)| NativeRunEvidence { run, events })
            .collect();
        verify(
            &self.definition,
            &self.intent.candidate,
            &self.intent,
            3,
            &evidence,
        )
    }
}

#[test]
fn native_review_accepts_distinct_producers_across_existing_profiles_after_reload() {
    let mut fixture = Fixture::new();
    for (run, events) in &mut fixture.runs {
        *run = serde_json::from_value(serde_json::to_value(&*run).unwrap()).unwrap();
        *events = serde_json::from_value(serde_json::to_value(&*events).unwrap()).unwrap();
        // Aggregate ledger identity is not a producer identity. Neither that
        // ledger nor a transient turn result supplies a missing acceptance.
        assert!(run.step_ledger.is_empty());
    }
    let verified = fixture.check().unwrap();
    verified.require_approval().unwrap();
    assert_eq!(verified.verdicts().count(), 3);
    assert_eq!(verified.runs().count(), 2);
}

#[test]
fn native_review_rejects_unkeyed_run_even_with_copied_intent_and_producer_events() {
    let mut fixture = Fixture::new();
    let (run, events) = &mut fixture.runs[0];
    run.run_id = RunId::new();
    for event in events {
        match &mut event.kind {
            MobEventKind::StepDispatched { run_id, .. }
            | MobEventKind::StepTargetCompleted { run_id, .. } => *run_id = run.run_id.clone(),
            _ => {}
        }
    }
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ScopeMismatch
    );
}

#[test]
fn native_review_rejects_completed_run_with_failed_or_skipped_step() {
    for status in ["Failed", "Skipped", "Canceled", "Dispatched"] {
        let mut fixture = Fixture::new();
        fixture.runs[0].0.flow_state.step_status.insert(
            STEP.into(),
            Some(serde_json::from_value(json!(status)).unwrap()),
        );
        assert_eq!(fixture.check().unwrap_err(), ReviewProofError::NotCompleted);
    }
}

#[test]
fn native_review_rejects_custody_loss_and_active_runs() {
    for status in [
        MobRunStatus::Pending,
        MobRunStatus::Running,
        MobRunStatus::Failed,
        MobRunStatus::Canceled,
    ] {
        let mut fixture = Fixture::new();
        fixture.runs[0].0.status = status;
        assert_eq!(fixture.check().unwrap_err(), ReviewProofError::NotCompleted);
    }
}

#[test]
fn native_review_rejects_legacy_role_wide_and_drifted_definition() {
    let mut fixture = Fixture::new();
    fixture.runs[0].0.role_targets = None;
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::MissingBinding
    );
    let mut fixture = Fixture::new();
    fixture.runs[0].0.flow_definition_digest = None;
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::DefinitionMismatch
    );
}

#[test]
fn native_review_rejects_candidate_scope_changes() {
    for field in [
        "tracker_context",
        "issue_id",
        "issue_identifier",
        "head_sha",
        "base_branch",
        "pull_request",
        "repo",
    ] {
        let mut fixture = Fixture::new();
        fixture.runs[0].0.activation_params["candidate"][field] = match field {
            "pull_request" => json!(2),
            "repo" => json!({"owner":"different", "name":"app"}),
            _ => json!("changed"),
        };
        assert_eq!(
            fixture.check().unwrap_err(),
            ReviewProofError::ScopeMismatch
        );
    }
}

#[test]
fn native_review_rejects_repeated_identity_in_different_profiles() {
    let mut fixture = Fixture::new();
    fixture.intent.reviewers = FlowRoleTargets::new([
        ("code", vec!["review-a".into(), "review-b".into()]),
        ("release", vec!["review-a".into()]),
    ])
    .unwrap();
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::DuplicateReviewer
    );
}

#[test]
fn native_review_aggregate_without_native_acceptance_is_not_proof() {
    let mut fixture = Fixture::new();
    fixture.runs[0].1.pop();
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ProducerEvidence
    );
}

#[test]
fn native_review_rejects_missing_or_mismatched_completion_payload() {
    for output in [None, Some(json!({"arbitrary":"green"}))] {
        let mut fixture = Fixture::new();
        if let MobEventKind::StepTargetCompleted { output: stored, .. } =
            &mut fixture.runs[0].1[1].kind
        {
            *stored = output;
        }
        assert!(fixture.check().is_err());
    }
}

#[test]
fn native_review_rejects_completion_without_its_dispatch() {
    let mut fixture = Fixture::new();
    fixture.runs[0].1.remove(0);
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ProducerEvidence
    );
}

#[test]
fn native_review_rejects_duplicate_acceptance_and_out_of_order_events() {
    let mut fixture = Fixture::new();
    let duplicate = fixture.runs[0].1[1].clone();
    fixture.runs[0].1.insert(2, duplicate);
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ProducerEvidence
    );
    let mut fixture = Fixture::new();
    fixture.runs[0].1.swap(0, 1);
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ProducerEvidence
    );
}

#[test]
fn native_review_rejects_foreign_native_mob_even_with_matching_sha() {
    let mut fixture = Fixture::new();
    fixture.runs[0].1[1].mob_id = "other-mob".into();
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ProducerEvidence
    );
    let mut fixture = Fixture::new();
    fixture.runs[0].0.mob_id = "other-mob".into();
    assert_eq!(
        fixture.check().unwrap_err(),
        ReviewProofError::ScopeMismatch
    );
}

#[test]
fn native_review_rejects_forged_reviewer_name_or_stale_head_in_accepted_output() {
    for (field, value) in [
        ("reviewer_id", json!("different-reviewer")),
        ("head_sha", json!("b".repeat(40))),
        ("base", json!("release")),
    ] {
        let mut fixture = Fixture::new();
        let (run, events) = &mut fixture.runs[0];
        if let MobEventKind::StepTargetCompleted {
            output: Some(output),
            target,
            ..
        } = &mut events[1].kind
        {
            if field == "head_sha" || field == "base" {
                output["evidence"][field] = value;
            } else {
                output[field] = value;
            }
            run.root_step_outputs.get_mut(&StepId::from(STEP)).unwrap()[target.identity.as_str()] =
                output.clone();
        }
        assert_eq!(fixture.check().unwrap_err(), ReviewProofError::NotApproved);
    }
}

#[test]
fn native_review_distinguishes_verified_blocked_output_from_approval() {
    let mut fixture = Fixture::new();
    let (run, events) = &mut fixture.runs[0];
    if let MobEventKind::StepTargetCompleted {
        output: Some(output),
        target,
        ..
    } = &mut events[1].kind
    {
        output["verdict"] = json!("blocked");
        run.root_step_outputs.get_mut(&StepId::from(STEP)).unwrap()[target.identity.as_str()] =
            output.clone();
    }
    let verified = fixture.check().unwrap();
    assert_eq!(verified.verdicts().count(), 3);
    assert_eq!(
        verified.require_approval(),
        Err(ReviewProofError::NotApproved)
    );
}

#[test]
fn final_review_policy_requires_distinct_profile_quota_and_at_least_three_members() {
    use crate::config::{FinalReviewConfig, FinalReviewReviewerConfig, IndependentReviewConfig};
    let mut config = FinalReviewConfig {
        enabled: true,
        reviewers: vec![
            FinalReviewReviewerConfig {
                id: "architecture".into(),
                profile: "domain".into(),
                required: true,
            },
            FinalReviewReviewerConfig {
                id: "tests".into(),
                profile: "domain".into(),
                required: true,
            },
            FinalReviewReviewerConfig {
                id: "optional".into(),
                profile: "security".into(),
                required: false,
            },
        ],
        ..Default::default()
    };
    let sufficient = FlowRoleTargets::new([
        (
            "domain",
            ["repo:a", "repo:b"].map(meerkat_meta::member_comms_id::roster_member_id_for_identity),
        ),
        (
            "other",
            ["repo:c", "repo:d"].map(meerkat_meta::member_comms_id::roster_member_id_for_identity),
        ),
    ])
    .unwrap();
    assert_eq!(final_review_count(&config, &sufficient).unwrap(), 4);
    assert_eq!(
        configured_review_count(&IndependentReviewConfig::default(), &config, &sufficient).unwrap(),
        4
    );
    let too_few = FlowRoleTargets::new([(
        "domain",
        ["repo:a", "repo:b"].map(meerkat_meta::member_comms_id::roster_member_id_for_identity),
    )])
    .unwrap();
    assert!(matches!(
        final_review_count(&config, &too_few),
        Err(ReviewProofError::ReviewerCount)
    ));
    let missing_profile = FlowRoleTargets::new([(
        "other",
        ["repo:a", "repo:b", "repo:c"]
            .map(meerkat_meta::member_comms_id::roster_member_id_for_identity),
    )])
    .unwrap();
    assert!(matches!(
        final_review_count(&config, &missing_profile),
        Err(ReviewProofError::InvalidIntent)
    ));
    let insufficient_quota = FlowRoleTargets::new([
        (
            "domain",
            vec![meerkat_meta::member_comms_id::roster_member_id_for_identity("repo:a")],
        ),
        (
            "other",
            ["repo:b", "repo:c"]
                .map(meerkat_meta::member_comms_id::roster_member_id_for_identity)
                .to_vec(),
        ),
    ])
    .unwrap();
    assert!(matches!(
        final_review_count(&config, &insufficient_quota),
        Err(ReviewProofError::InvalidIntent)
    ));
    // Admission can accept independent review, but final completion and merge
    // must call final_review_count and enforce profile coverage again.
    let independent = IndependentReviewConfig {
        enabled: true,
        ..Default::default()
    };
    assert_eq!(
        configured_review_count(&independent, &config, &missing_profile).unwrap(),
        3
    );
    config.enabled = false;
    assert!(matches!(
        configured_review_count(&IndependentReviewConfig::default(), &config, &sufficient),
        Err(ReviewProofError::Unavailable)
    ));
}
