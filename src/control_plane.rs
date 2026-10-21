use serde::{Deserialize, Serialize};

use crate::ims::{
    ImsCommentChange, ImsCommentObservation, ImsStageClassification, ImsStageObservation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePresence {
    Unknown,
    Absent,
    Present,
    Retired,
}

impl RuntimePresence {
    pub fn has_running_mob(self) -> bool {
        matches!(self, Self::Present)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostControlObservation<CommentId> {
    IssueFirstSeen {
        issue: String,
        lane: String,
        classification: ImsStageClassification,
        runtime_presence: RuntimePresence,
    },
    IssueStageChanged {
        issue: String,
        from: String,
        to: String,
        classification: ImsStageClassification,
        runtime_presence: RuntimePresence,
    },
    OpenIssueWithoutRuntime {
        issue: String,
        lane: String,
        classification: ImsStageClassification,
    },
    IssueCommentAdded {
        issue: String,
        comment_id: CommentId,
        body_markdown: String,
    },
    IssueCommentChanged {
        issue: String,
        comment_id: CommentId,
        body_markdown: String,
    },
    IssueCommentDeleted {
        issue: String,
        comment_id: CommentId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneEffect<CommentId> {
    NotifyKapellmeister(HostControlObservation<CommentId>),
}

pub fn reduce_stage_observation<CommentId>(
    observation: ImsStageObservation,
    classification: ImsStageClassification,
    runtime_presence: RuntimePresence,
) -> Vec<ControlPlaneEffect<CommentId>> {
    match observation {
        ImsStageObservation::Unchanged => Vec::new(),
        ImsStageObservation::FirstSeen {
            identifier,
            current,
        } => {
            if runtime_presence.has_running_mob()
                || !classification.should_report_first_seen_without_runtime()
            {
                Vec::new()
            } else {
                vec![ControlPlaneEffect::NotifyKapellmeister(
                    HostControlObservation::IssueFirstSeen {
                        issue: identifier,
                        lane: current,
                        classification,
                        runtime_presence,
                    },
                )]
            }
        }
        ImsStageObservation::Changed {
            identifier,
            previous,
            current,
        } => vec![ControlPlaneEffect::NotifyKapellmeister(
            HostControlObservation::IssueStageChanged {
                issue: identifier,
                from: previous,
                to: current,
                classification,
                runtime_presence,
            },
        )],
    }
}

pub fn reduce_open_issue_without_runtime<CommentId>(
    issue: String,
    lane: String,
    classification: ImsStageClassification,
    runtime_presence: RuntimePresence,
) -> Vec<ControlPlaneEffect<CommentId>> {
    if runtime_presence.has_running_mob()
        || !classification.should_report_first_seen_without_runtime()
    {
        return Vec::new();
    }
    vec![ControlPlaneEffect::NotifyKapellmeister(
        HostControlObservation::OpenIssueWithoutRuntime {
            issue,
            lane,
            classification,
        },
    )]
}

pub fn reduce_comment_observation<CommentId>(
    issue: String,
    observation: ImsCommentObservation<CommentId>,
) -> Vec<ControlPlaneEffect<CommentId>> {
    match observation {
        ImsCommentObservation::FirstSeen | ImsCommentObservation::Unchanged => Vec::new(),
        ImsCommentObservation::Changed { changes } => changes
            .into_iter()
            .map(|change| {
                ControlPlaneEffect::NotifyKapellmeister(match change {
                    ImsCommentChange::Added {
                        comment_id,
                        body_markdown,
                    } => HostControlObservation::IssueCommentAdded {
                        issue: issue.clone(),
                        comment_id,
                        body_markdown,
                    },
                    ImsCommentChange::Changed {
                        comment_id,
                        body_markdown,
                    } => HostControlObservation::IssueCommentChanged {
                        issue: issue.clone(),
                        comment_id,
                        body_markdown,
                    },
                    ImsCommentChange::Deleted { comment_id } => {
                        HostControlObservation::IssueCommentDeleted {
                            issue: issue.clone(),
                            comment_id,
                        }
                    }
                })
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CommentId(&'static str);

    #[test]
    fn reducer_reports_external_stage_changes_without_encoding_mob_steps() {
        let effects = reduce_stage_observation::<CommentId>(
            ImsStageObservation::Changed {
                identifier: "LUC-582".to_string(),
                previous: "Implementing".to_string(),
                current: "Rework".to_string(),
            },
            ImsStageClassification::Intake,
            RuntimePresence::Present,
        );

        assert_eq!(
            effects,
            vec![ControlPlaneEffect::NotifyKapellmeister(
                HostControlObservation::IssueStageChanged {
                    issue: "LUC-582".to_string(),
                    from: "Implementing".to_string(),
                    to: "Rework".to_string(),
                    classification: ImsStageClassification::Intake,
                    runtime_presence: RuntimePresence::Present,
                },
            )]
        );
    }

    #[test]
    fn reducer_does_not_treat_projected_active_lane_as_a_mob_transition() {
        let effects = reduce_stage_observation::<CommentId>(
            ImsStageObservation::FirstSeen {
                identifier: "LUC-582".to_string(),
                current: "Implementing".to_string(),
            },
            ImsStageClassification::ProjectedActive,
            RuntimePresence::Present,
        );

        assert!(effects.is_empty());
    }

    #[test]
    fn reducer_reports_idle_control_plane_work_to_kapellmeister_only() {
        let effects = reduce_stage_observation::<CommentId>(
            ImsStageObservation::FirstSeen {
                identifier: "LUC-526".to_string(),
                current: "Todo".to_string(),
            },
            ImsStageClassification::Intake,
            RuntimePresence::Absent,
        );

        assert!(matches!(
            effects.as_slice(),
            [ControlPlaneEffect::NotifyKapellmeister(
                HostControlObservation::IssueFirstSeen { .. }
            )]
        ));
    }

    #[test]
    fn reducer_reports_comment_changes_as_observations() {
        let effects = reduce_comment_observation(
            "LUC-582".to_string(),
            ImsCommentObservation::Changed {
                changes: vec![
                    ImsCommentChange::Added {
                        comment_id: CommentId("comment-1"),
                        body_markdown: "operator review".to_string(),
                    },
                    ImsCommentChange::Deleted {
                        comment_id: CommentId("comment-2"),
                    },
                ],
            },
        );

        assert_eq!(effects.len(), 2);
        assert!(matches!(
            effects.first(),
            Some(ControlPlaneEffect::NotifyKapellmeister(
                HostControlObservation::IssueCommentAdded { .. }
            ))
        ));
        assert!(matches!(
            effects.get(1),
            Some(ControlPlaneEffect::NotifyKapellmeister(
                HostControlObservation::IssueCommentDeleted { .. }
            ))
        ));
    }
}
