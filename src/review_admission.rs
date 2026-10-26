//! In-process review admission with native caller identity.
//! Caller and retained-work data never pass through model arguments or HTTP.

use meerkat_core::retained_work::RetainedWorkIdentity;
use meerkat_core::{MobMemberBinding, ToolDispatchContext};
use meerkat_mob::run::FlowRunObserver;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use crate::config::ServiceConfig;
use crate::independent_review::IndependentReviewAdmissionRequest;
use crate::meta_runtime::MetaRuntimeRegistry;
use crate::native_review::{ReviewIntent, ReviewProofError, ReviewReference};
use crate::orchestrator::InternalMessage;
use crate::review_requests::{ReviewRequestDraft, ReviewRequestError};

#[derive(Clone)]
pub(crate) struct ReviewSession {
    binding: MobMemberBinding,
    sender: mpsc::Sender<InternalMessage>,
}

impl ReviewSession {
    pub(crate) fn new(binding: MobMemberBinding, sender: mpsc::Sender<InternalMessage>) -> Self {
        Self { binding, sender }
    }

    pub(crate) async fn admit(
        &self,
        request: IndependentReviewAdmissionRequest,
        context: &ToolDispatchContext,
    ) -> Result<AdmittedReview, AdmissionFailure> {
        let origin_session_id = context
            .origin_session_id()
            .cloned()
            .ok_or(AdmissionFailure::MissingCaller)?;
        let caller = ReviewCaller {
            binding: self.binding.clone(),
            retained_work: context.retained_work().cloned(),
            origin_session_id,
        };
        let (response, result) = oneshot::channel();
        let command = NativeReviewAdmission {
            caller,
            request,
            response,
        };
        let wait = async {
            self.sender
                .send(InternalMessage::NativeReviewAdmission(Box::new(command)))
                .await
                .map_err(|_| AdmissionFailure::Unavailable)?;
            result.await.map_err(|_| AdmissionFailure::Unavailable)?
        };
        let limit = std::time::Duration::from_secs(30);
        let limit = context
            .remaining_tool_time()
            .map_or(limit, |left| left.min(limit));
        tokio::time::timeout(limit, wait)
            .await
            .map_err(|_| AdmissionFailure::Unavailable)?
    }
}

/// An opaque internal command. Only the native session hook and dispatch
/// context can construct its caller; there is no deserialization boundary.
#[derive(Debug)]
pub struct NativeReviewAdmission {
    caller: ReviewCaller,
    request: IndependentReviewAdmissionRequest,
    response: oneshot::Sender<Result<AdmittedReview, AdmissionFailure>>,
}

impl NativeReviewAdmission {
    pub(crate) async fn execute(self, config: &ServiceConfig, registry: &MetaRuntimeRegistry) {
        let result = async {
            let draft = resolve(config, self.request).await?;
            let (intent, native_review) = registry
                .admit_member_native_review(draft, &self.caller)
                .await?;
            Ok(AdmittedReview {
                accepted: true,
                intent,
                native_review,
            })
        }
        .await;
        // A dropped reply does not undo admission. The same request/key
        // recovers the original native runs. Core retains current observer
        // work, or refreshes stale work after respawn under its actor check.
        if self.response.send(result).is_err() {
            tracing::debug!("native review admission caller stopped waiting for its reply");
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReviewCaller {
    binding: MobMemberBinding,
    retained_work: Option<RetainedWorkIdentity>,
    origin_session_id: meerkat_core::SessionId,
}

impl ReviewCaller {
    pub(crate) fn binding(&self) -> &MobMemberBinding {
        &self.binding
    }

    pub(crate) fn origin_session_id(&self) -> &meerkat_core::SessionId {
        &self.origin_session_id
    }

    pub(crate) fn observer(&self) -> FlowRunObserver {
        let observer = FlowRunObserver::new(self.binding.member.as_str().into());
        // Ordinary native sessions have no retained work. Core owns whether
        // this observer requires governed work and validates any supplied identity.
        match &self.retained_work {
            Some(work) => observer.with_retained_work(work.clone()),
            None => observer,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct AdmittedReview {
    accepted: bool,
    intent: ReviewIntent,
    native_review: ReviewReference,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmissionFailure {
    #[error("Independent review is disabled for this repository.")]
    Disabled,
    #[error("Review admission requires the native repository session binding.")]
    MissingCaller,
    #[error("The PR head changed. Inspect it before requesting review.")]
    CandidateChanged,
    #[error("Review candidate is unavailable: {0}")]
    Candidate(ReviewProofError),
    #[error("Review admission could not be confirmed. Retry the unchanged request and key.")]
    Unavailable,
    #[error(transparent)]
    Request(#[from] ReviewRequestError),
}

impl AdmissionFailure {
    pub(crate) fn tool_result(&self, id: &str) -> meerkat_core::ToolResult {
        let mut message = self.to_string();
        if matches!(
            self,
            Self::Request(ReviewRequestError::Admission(
                crate::native_review::ReviewAdmissionError::Runtime(_)
            ))
        ) {
            message.push_str(" Some reviewer runs may already be admitted. When retrying, use the unchanged request and idempotency key to recover them.");
        }
        meerkat_core::ToolResult::new(
            id.to_owned(),
            serde_json::json!({
                "error": self.code(), "message": message,
                "current_request": self.current_request(),
            })
            .to_string(),
            true,
        )
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Disabled => "independent_review_disabled",
            Self::MissingCaller => "native_review_caller_required",
            Self::CandidateChanged => "review_candidate_changed",
            Self::Candidate(_) => "review_candidate_unavailable",
            Self::Unavailable => "native_review_admission_unconfirmed",
            Self::Request(_) => "native_review_admission_rejected",
        }
    }

    pub(crate) fn current_request(&self) -> Option<&str> {
        match self {
            Self::Request(ReviewRequestError::CurrentChanged { current }) => current.as_deref(),
            _ => None,
        }
    }
}

pub(crate) async fn resolve(
    config: &ServiceConfig,
    request: IndependentReviewAdmissionRequest,
) -> Result<ReviewRequestDraft, AdmissionFailure> {
    if !config.independent_review.enabled && !config.final_review.enabled {
        return Err(AdmissionFailure::Disabled);
    }
    let (candidate, _) = crate::native_review::resolve_candidate(
        config,
        request.issue,
        &request.repo,
        crate::tools::GitHubPullRequestRef::Number {
            number: request.pull_request,
        },
    )
    .await
    .map_err(AdmissionFailure::Candidate)?;
    if candidate.head_sha != request.head_sha {
        return Err(AdmissionFailure::CandidateChanged);
    }
    Ok(ReviewRequestDraft {
        candidate,
        reviewers: request.reviewers,
        task: request.task,
        idempotency_key: request.idempotency_key,
        supersedes: request.supersedes,
    })
}
