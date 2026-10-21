use crate::config::MaterializedLinearProjection;
use crate::domain::normalize_state;
use crate::tools::{MobEventKind, MobEventSnapshot, MobRunStatus};

pub fn evaluate_linear_projection(
    projection: &MaterializedLinearProjection,
    run_status: MobRunStatus,
    events: &[MobEventSnapshot],
) -> Option<String> {
    terminal_lane(projection, run_status)
        .or_else(|| latest_terminal_event_lane(projection, events))
        .or_else(|| active_lane(projection, events))
}

pub fn projection_lane_for_node<'a>(
    projection: &'a MaterializedLinearProjection,
    node_or_member: &str,
) -> Option<&'a str> {
    projection
        .node_lanes
        .get(node_or_member)
        .map(String::as_str)
}

pub fn projection_phase_label_for_node<'a>(
    projection: &'a MaterializedLinearProjection,
    node_or_member: &str,
) -> Option<&'a str> {
    projection
        .node_phase_labels
        .get(node_or_member)
        .map(String::as_str)
}

pub fn active_projection_phase_labels(
    projection: &MaterializedLinearProjection,
    events: &[MobEventSnapshot],
) -> Vec<String> {
    active_nodes(events)
        .into_iter()
        .filter_map(|node| projection.node_phase_labels.get(&node).cloned())
        .collect()
}

fn terminal_lane(
    projection: &MaterializedLinearProjection,
    status: MobRunStatus,
) -> Option<String> {
    match status {
        MobRunStatus::Succeeded => Some(projection.terminal_lanes.succeeded.clone()),
        MobRunStatus::Failed => Some(projection.terminal_lanes.failed.clone()),
        MobRunStatus::Cancelled => Some(projection.terminal_lanes.cancelled.clone()),
        MobRunStatus::Pending | MobRunStatus::Running | MobRunStatus::WaitingForInput => None,
    }
}

fn latest_terminal_event_lane(
    projection: &MaterializedLinearProjection,
    events: &[MobEventSnapshot],
) -> Option<String> {
    events.iter().rev().find_map(|event| {
        if let MobEventKind::RunCompleted { status, .. } = event.kind {
            terminal_lane(projection, status)
        } else {
            None
        }
    })
}

fn active_lane(
    projection: &MaterializedLinearProjection,
    events: &[MobEventSnapshot],
) -> Option<String> {
    let active = active_nodes(events);

    if active.is_empty() {
        return None;
    }

    let active_lanes = active
        .iter()
        .filter_map(|node| projection.node_lanes.get(node))
        .cloned()
        .collect::<Vec<_>>();
    if active_lanes.is_empty() {
        return projection.aggregate.fallback.clone();
    }
    projection
        .aggregate
        .precedence
        .iter()
        .find(|lane| {
            let normalized = normalize_state(lane);
            active_lanes
                .iter()
                .any(|candidate| normalize_state(candidate) == normalized)
        })
        .cloned()
        .or_else(|| active_lanes.into_iter().next())
}

fn active_nodes(events: &[MobEventSnapshot]) -> Vec<String> {
    let mut active = Vec::<String>::new();
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|event| event.sequence);
    for event in ordered {
        match &event.kind {
            MobEventKind::MemberStarted { member } => {
                active.retain(|candidate| candidate != member);
                active.push(member.clone());
            }
            MobEventKind::MemberCompleted { member } => {
                active.retain(|candidate| candidate != member);
            }
            MobEventKind::MemberKickoffUpdated { .. }
            | MobEventKind::RunStarted
            | MobEventKind::ToolCallStarted { .. }
            | MobEventKind::ToolCallCompleted { .. }
            | MobEventKind::RunCompleted { .. } => {}
        }
    }
    active
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MaterializedLinearProjection, NativeLinearProjectionAggregate,
        NativeLinearProjectionTerminals,
    };
    use crate::tools::{FugueToolOperation, MobEventKind, MobEventSnapshot, MobRunStatus};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn projection() -> MaterializedLinearProjection {
        MaterializedLinearProjection {
            id: "planner_coder_reviewer".to_string(),
            node_lanes: BTreeMap::from([
                ("planner".to_string(), "Planning".to_string()),
                ("coder".to_string(), "Implementing".to_string()),
                ("reviewer".to_string(), "Reviewing".to_string()),
            ]),
            node_phase_labels: BTreeMap::from([
                ("planner".to_string(), "fugue:phase:planning".to_string()),
                ("coder".to_string(), "fugue:phase:implementing".to_string()),
                ("reviewer".to_string(), "fugue:phase:reviewing".to_string()),
            ]),
            aggregate: NativeLinearProjectionAggregate {
                precedence: vec![
                    "Reviewing".to_string(),
                    "Implementing".to_string(),
                    "Planning".to_string(),
                ],
                fallback: Some("Reviewing".to_string()),
            },
            terminal_lanes: NativeLinearProjectionTerminals {
                succeeded: "Done".to_string(),
                failed: "Reviewing".to_string(),
                cancelled: "Canceled".to_string(),
            },
        }
    }

    fn event(sequence: u64, kind: MobEventKind) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence,
            occurred_at: Utc.timestamp_opt(sequence as i64, 0).single().unwrap(),
            kind,
            message: None,
        }
    }

    #[test]
    fn planner_maps_to_planning() {
        let events = vec![event(
            1,
            MobEventKind::MemberStarted {
                member: "planner".to_string(),
            },
        )];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            Some("Planning")
        );
    }

    #[test]
    fn parallel_coder_and_reviewer_uses_precedence() {
        let events = vec![
            event(
                1,
                MobEventKind::MemberStarted {
                    member: "coder".to_string(),
                },
            ),
            event(
                2,
                MobEventKind::MemberStarted {
                    member: "reviewer".to_string(),
                },
            ),
        ];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            Some("Reviewing")
        );
    }

    #[test]
    fn custom_lanes_are_not_hardcoded() {
        let mut projection = projection();
        projection
            .node_lanes
            .insert("planner".to_string(), "Design".to_string());
        projection
            .node_lanes
            .insert("coder".to_string(), "Build".to_string());
        projection
            .node_lanes
            .insert("reviewer".to_string(), "Gate".to_string());
        projection.aggregate.precedence = vec!["Gate".to_string(), "Build".to_string()];
        let events = vec![event(
            1,
            MobEventKind::MemberStarted {
                member: "coder".to_string(),
            },
        )];

        assert_eq!(
            evaluate_linear_projection(&projection, MobRunStatus::Running, &events).as_deref(),
            Some("Build")
        );
    }

    #[test]
    fn terminal_status_maps_only_through_projection_config() {
        let projection = projection();
        assert_eq!(
            evaluate_linear_projection(&projection, MobRunStatus::Succeeded, &[]).as_deref(),
            Some("Done")
        );
        assert_eq!(
            evaluate_linear_projection(&projection, MobRunStatus::Failed, &[]).as_deref(),
            Some("Reviewing")
        );
        assert_eq!(
            evaluate_linear_projection(&projection, MobRunStatus::Cancelled, &[]).as_deref(),
            Some("Canceled")
        );
    }

    #[test]
    fn non_lifecycle_tool_events_do_not_create_a_parallel_state_machine() {
        let events = vec![event(
            1,
            MobEventKind::ToolCallCompleted {
                operation: FugueToolOperation::GitHubCreatePr,
            },
        )];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            None
        );
    }

    #[test]
    fn running_without_active_nodes_does_not_fall_back_to_reviewing() {
        let events = vec![event(1, MobEventKind::RunStarted)];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            None
        );
    }

    #[test]
    fn member_kickoff_updates_do_not_drive_projection() {
        let events = vec![
            event(
                1,
                MobEventKind::MemberStarted {
                    member: "planner".to_string(),
                },
            ),
            event(
                2,
                MobEventKind::MemberKickoffUpdated {
                    member: "reviewer".to_string(),
                    phase: "started".to_string(),
                },
            ),
        ];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            Some("Planning")
        );
    }

    #[test]
    fn kickoff_without_flow_node_does_not_use_fallback() {
        let events = vec![event(
            1,
            MobEventKind::MemberKickoffUpdated {
                member: "reviewer".to_string(),
                phase: "started".to_string(),
            },
        )];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            None
        );
    }

    #[test]
    fn fallback_only_applies_when_unmapped_active_nodes_exist() {
        let events = vec![event(
            1,
            MobEventKind::MemberStarted {
                member: "unknown-specialist".to_string(),
            },
        )];

        assert_eq!(
            evaluate_linear_projection(&projection(), MobRunStatus::Running, &events).as_deref(),
            Some("Reviewing")
        );
    }

    #[test]
    fn projection_exposes_phase_labels_separately_from_lanes() {
        let projection = projection();

        assert_eq!(
            projection_lane_for_node(&projection, "coder"),
            Some("Implementing")
        );
        assert_eq!(
            projection_phase_label_for_node(&projection, "coder"),
            Some("fugue:phase:implementing")
        );
    }

    #[test]
    fn active_phase_labels_follow_current_active_nodes() {
        let events = vec![
            event(
                1,
                MobEventKind::MemberStarted {
                    member: "planner".to_string(),
                },
            ),
            event(
                2,
                MobEventKind::MemberCompleted {
                    member: "planner".to_string(),
                },
            ),
            event(
                3,
                MobEventKind::MemberStarted {
                    member: "coder".to_string(),
                },
            ),
        ];

        assert_eq!(
            active_projection_phase_labels(&projection(), &events),
            vec!["fugue:phase:implementing".to_string()]
        );
    }
}
