/// Stable native mob identity, shared by runtime and WorkGraph scoping.
pub const FUGUE_MOB_ID: &str = "fugue";

use crate::config::MaterializedMobTemplate;
use crate::tools::{FugueToolOperation, MobEventKind, MobEventSnapshot, MobRunStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMobE2eEvidence {
    pub opened_pr_count: usize,
    pub review_rejections: usize,
    pub rejections_routed_to_coder: usize,
    pub max_review_cycles: Option<u64>,
    pub completed_successfully: bool,
}

impl NativeMobE2eEvidence {
    pub fn inspect(
        events: &[MobEventSnapshot],
        template: &MaterializedMobTemplate,
    ) -> NativeMobE2eEvidence {
        inspect_native_mob_e2e_contract(events, template.max_review_cycles)
    }

    pub const fn opens_exactly_one_pr(&self) -> bool {
        self.opened_pr_count == 1
    }

    pub const fn completed_issue_with_single_pr(&self) -> bool {
        self.completed_successfully && self.opens_exactly_one_pr()
    }

    pub const fn stays_within_review_cycle_limit(&self) -> bool {
        match self.max_review_cycles {
            Some(limit) => self.review_rejections as u64 <= limit,
            None => true,
        }
    }

    pub const fn routes_every_rejection_to_coder(&self) -> bool {
        self.review_rejections == self.rejections_routed_to_coder
    }

    pub const fn review_rework_loop_is_bounded_and_routed(&self) -> bool {
        self.routes_every_rejection_to_coder() && self.stays_within_review_cycle_limit()
    }
}

pub fn inspect_native_mob_e2e_contract(
    events: &[MobEventSnapshot],
    max_review_cycles: Option<u64>,
) -> NativeMobE2eEvidence {
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|event| event.sequence);

    let opened_pr_count = ordered
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::GitHubCreatePr
                }
            )
        })
        .count();
    let completed_successfully = ordered.iter().any(|event| {
        matches!(
            event.kind,
            MobEventKind::RunCompleted {
                status: MobRunStatus::Succeeded,
                ..
            }
        )
    });

    let rejection_indices = ordered
        .iter()
        .enumerate()
        .filter_map(|(index, event)| review_rejection(event).then_some(index))
        .collect::<Vec<_>>();
    let rejections_routed_to_coder = rejection_indices
        .iter()
        .filter(|index| routed_to_coder_after_rejection(&ordered, **index))
        .count();

    NativeMobE2eEvidence {
        opened_pr_count,
        review_rejections: rejection_indices.len(),
        rejections_routed_to_coder,
        max_review_cycles,
        completed_successfully,
    }
}

fn review_rejection(event: &MobEventSnapshot) -> bool {
    matches!(
        &event.kind,
        MobEventKind::MemberCompleted { member }
            if member.eq_ignore_ascii_case("reviewer")
                && event
                    .message
                    .as_deref()
                    .is_some_and(message_requests_rework)
    )
}

fn message_requests_rework(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("reject")
        || normalized.contains("changes requested")
        || normalized.contains("needs rework")
        || normalized.contains("request changes")
}

fn routed_to_coder_after_rejection(events: &[&MobEventSnapshot], rejection_index: usize) -> bool {
    events
        .iter()
        .skip(rejection_index.saturating_add(1))
        .take_while(|event| {
            !matches!(event.kind, MobEventKind::RunCompleted { .. })
                && !matches!(
                        &event.kind,
                        MobEventKind::MemberCompleted { member }
                            if member.eq_ignore_ascii_case("reviewer")
                )
        })
        .any(|event| {
            matches!(
                &event.kind,
                MobEventKind::MemberStarted { member }
                    if member.eq_ignore_ascii_case("coder")
                        || member.eq_ignore_ascii_case("rework")
            )
        })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::tools::MobEventSnapshot;

    fn event(sequence: u64, kind: MobEventKind, message: Option<&str>) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence,
            occurred_at: Utc::now(),
            kind,
            message: message.map(str::to_string),
        }
    }

    #[test]
    fn native_mob_contract_accepts_successful_run_with_exactly_one_pr() {
        let evidence = inspect_native_mob_e2e_contract(
            &[
                event(1, MobEventKind::RunStarted, None),
                event(
                    2,
                    MobEventKind::ToolCallCompleted {
                        operation: FugueToolOperation::GitHubCreatePr,
                    },
                    Some("opened pull request #7"),
                ),
                event(
                    3,
                    MobEventKind::RunCompleted {
                        status: MobRunStatus::Succeeded,
                        structured_output: None,
                    },
                    None,
                ),
            ],
            Some(2),
        );

        assert!(evidence.completed_issue_with_single_pr());
        assert!(evidence.review_rework_loop_is_bounded_and_routed());
    }

    #[test]
    fn native_mob_contract_rejects_duplicate_pr_openings() {
        let evidence = inspect_native_mob_e2e_contract(
            &[
                event(
                    1,
                    MobEventKind::ToolCallCompleted {
                        operation: FugueToolOperation::GitHubCreatePr,
                    },
                    Some("opened pull request #7"),
                ),
                event(
                    2,
                    MobEventKind::ToolCallCompleted {
                        operation: FugueToolOperation::GitHubCreatePr,
                    },
                    Some("opened pull request #8"),
                ),
                event(
                    3,
                    MobEventKind::RunCompleted {
                        status: MobRunStatus::Succeeded,
                        structured_output: None,
                    },
                    None,
                ),
            ],
            Some(2),
        );

        assert_eq!(evidence.opened_pr_count, 2);
        assert!(!evidence.completed_issue_with_single_pr());
    }

    #[test]
    fn native_mob_contract_requires_reviewer_rejections_to_route_back_to_coder_within_limit() {
        let evidence = inspect_native_mob_e2e_contract(
            &[
                event(
                    1,
                    MobEventKind::MemberCompleted {
                        member: "reviewer".to_string(),
                    },
                    Some("changes requested: add validation"),
                ),
                event(
                    2,
                    MobEventKind::MemberStarted {
                        member: "coder".to_string(),
                    },
                    Some("addressing reviewer feedback"),
                ),
                event(
                    3,
                    MobEventKind::MemberCompleted {
                        member: "reviewer".to_string(),
                    },
                    Some("rejected: docs missing"),
                ),
                event(
                    4,
                    MobEventKind::MemberStarted {
                        member: "rework".to_string(),
                    },
                    Some("bounded final rework"),
                ),
            ],
            Some(2),
        );

        assert_eq!(evidence.review_rejections, 2);
        assert_eq!(evidence.rejections_routed_to_coder, 2);
        assert!(evidence.review_rework_loop_is_bounded_and_routed());

        let exceeded = inspect_native_mob_e2e_contract(
            &[
                event(
                    1,
                    MobEventKind::MemberCompleted {
                        member: "reviewer".to_string(),
                    },
                    Some("rejected"),
                ),
                event(
                    2,
                    MobEventKind::MemberStarted {
                        member: "coder".to_string(),
                    },
                    None,
                ),
                event(
                    3,
                    MobEventKind::MemberCompleted {
                        member: "reviewer".to_string(),
                    },
                    Some("rejected"),
                ),
            ],
            Some(1),
        );
        assert!(!exceeded.review_rework_loop_is_bounded_and_routed());
    }
}
