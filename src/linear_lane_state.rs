use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;
use crate::domain::{Issue, normalize_state};
use crate::error::{FugueError, Result};
use crate::ims::{
    ImsCommentChange, ImsCommentObservation, ImsStageClassification, ImsStageObservation,
};
use crate::tools::{LinearCommentId, LinearIssueIdentifier, LinearIssueRef, LinearStateRef};

pub const LINEAR_LANE_STATE_PATH_ENV: &str = "FUGUE_LINEAR_LANE_STATE_PATH";
pub const LINEAR_COMMENT_STATE_FILE: &str = "linear-comments.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinearLaneState {
    #[serde(default)]
    lanes: BTreeMap<String, String>,
}

impl LinearLaneState {
    pub fn read(path: &Path) -> Result<Self> {
        match fs::read_to_string(path) {
            Ok(contents) if contents.trim().is_empty() => Ok(Self {
                lanes: BTreeMap::new(),
            }),
            Ok(contents) => serde_json::from_str(&contents).map_err(|error| {
                FugueError::InvalidConfig(format!(
                    "parse Linear lane state {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Self {
                lanes: BTreeMap::new(),
            }),
            Err(error) => Err(FugueError::Workspace(format!(
                "read Linear lane state {}: {error}",
                path.display()
            ))),
        }
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                FugueError::Workspace(format!(
                    "create Linear lane state directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let rendered = serde_json::to_string_pretty(self)
            .map_err(|error| FugueError::InvalidConfig(error.to_string()))?;
        atomic_write(path, rendered.as_bytes()).map_err(|error| {
            FugueError::Workspace(format!(
                "write Linear lane state {}: {error}",
                path.display()
            ))
        })?;
        Ok(())
    }

    pub fn observe_issue(&mut self, issue: &Issue) -> LinearLaneObservation {
        self.observe_lane(&issue.identifier, &issue.state)
    }

    pub fn observe_lane(&mut self, identifier: &str, lane: &str) -> LinearLaneObservation {
        match self.lanes.insert(identifier.to_string(), lane.to_string()) {
            Some(previous) if normalize_state(&previous) != normalize_state(lane) => {
                LinearLaneObservation::Changed {
                    identifier: identifier.to_string(),
                    previous,
                    current: lane.to_string(),
                }
            }
            Some(_) => LinearLaneObservation::Unchanged,
            None => LinearLaneObservation::FirstSeen {
                identifier: identifier.to_string(),
                current: lane.to_string(),
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }
}

pub type LinearLaneObservation = ImsStageObservation;
pub type LinearLaneClassification = ImsStageClassification;

pub fn classify_linear_lane(config: &ServiceConfig, lane: &str) -> LinearLaneClassification {
    if same_lane(lane, &config.linear.lanes.parked) {
        return LinearLaneClassification::Parked;
    }
    if config
        .linear
        .lanes
        .review_ready
        .as_ref()
        .is_some_and(|review_ready| same_lane(lane, review_ready))
        || (config.final_review.enabled && same_lane(lane, &config.final_review.lane))
        || config
            .linear
            .lanes
            .blocked
            .as_ref()
            .is_some_and(|blocked| same_lane(lane, blocked))
    {
        return LinearLaneClassification::AttentionOrTerminal;
    }
    if config.tracker.is_terminal_state(lane) {
        return LinearLaneClassification::AttentionOrTerminal;
    }
    if config.tracker.is_intake_state(lane) {
        return LinearLaneClassification::Intake;
    }
    if config
        .linear
        .lanes
        .active
        .as_ref()
        .is_some_and(|active| same_lane(lane, active))
    {
        return LinearLaneClassification::ProjectedActive;
    }
    if is_projected_active_lane(config, lane) {
        return LinearLaneClassification::ProjectedActive;
    }
    if is_rework_entry_lane(config, lane) {
        return LinearLaneClassification::Intake;
    }
    if is_control_lane(config, lane) {
        return LinearLaneClassification::ControlActive;
    }
    if config.tracker.is_active_state(lane) {
        return LinearLaneClassification::ControlActive;
    }
    LinearLaneClassification::UnexpectedNonterminal
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyLinearLaneRepair {
    pub source_lane: String,
    pub target_lane: String,
    pub labels: Vec<String>,
    pub reason: String,
}

pub fn legacy_linear_lane_repair(
    config: &ServiceConfig,
    lane: &str,
) -> Option<LegacyLinearLaneRepair> {
    let (target_lane, labels, reason) = match normalize_state(lane).as_str() {
        "todo" => (
            config.linear.lanes.ready.clone(),
            Vec::new(),
            "legacy Todo intake lane maps to the configured intake lane",
        ),
        "planning" => (
            canonical_active_lane(config),
            vec!["fugue:phase:planning".to_string()],
            "legacy Planning execution phase maps to Active plus a phase label",
        ),
        "implementing" => (
            canonical_active_lane(config),
            vec!["fugue:phase:implementing".to_string()],
            "legacy Implementing execution phase maps to Active plus a phase label",
        ),
        "reviewing" => (
            canonical_active_lane(config),
            vec!["fugue:phase:reviewing".to_string()],
            "legacy Reviewing execution phase maps to Active plus a phase label",
        ),
        "ready for review" => (
            configured_review_action_lane(config).unwrap_or_else(|| canonical_active_lane(config)),
            Vec::new(),
            "legacy Ready for review maps to the configured review action lane when present",
        ),
        "do review" => (
            configured_review_action_lane(config).unwrap_or_else(|| canonical_active_lane(config)),
            Vec::new(),
            "legacy Do Review maps to the configured review action lane",
        ),
        "final review & merge" => (
            configured_deploy_action_lane(config)
                .or_else(|| configured_review_action_lane(config))
                .unwrap_or_else(|| canonical_active_lane(config)),
            Vec::new(),
            "legacy Final Review & Merge maps to Deploy when configured, otherwise Review/Active",
        ),
        "rework" => (
            config.linear.lanes.ready.clone(),
            Vec::new(),
            "legacy Rework maps to intake; unresolved WorkGraph blockers drive focused active work",
        ),
        "human review" => (
            config
                .linear
                .lanes
                .blocked
                .clone()
                .unwrap_or_else(|| "Blocked".to_string()),
            vec!["fugue:needs:human".to_string()],
            "legacy Human Review maps to the configured blocked lane plus human-needed label",
        ),
        "in progress" => (
            canonical_active_lane(config),
            Vec::new(),
            "legacy In Progress maps to the configured active lane",
        ),
        _ => return None,
    };
    if same_lane(lane, &target_lane) && labels.is_empty() {
        return None;
    }
    Some(LegacyLinearLaneRepair {
        source_lane: lane.to_string(),
        target_lane,
        labels,
        reason: reason.to_string(),
    })
}

fn canonical_active_lane(config: &ServiceConfig) -> String {
    config
        .linear
        .lanes
        .active
        .clone()
        .or_else(|| {
            config.mob.templates.values().find_map(|template| {
                let projection = template.linear_projection.as_ref()?;
                projection.node_lanes.values().next().cloned()
            })
        })
        .unwrap_or_else(|| "Active".to_string())
}

fn configured_review_action_lane(config: &ServiceConfig) -> Option<String> {
    config.linear.lanes.do_review.clone().or_else(|| {
        config
            .independent_review
            .enabled
            .then(|| config.independent_review.lane.clone())
    })
}

fn configured_deploy_action_lane(config: &ServiceConfig) -> Option<String> {
    config
        .linear
        .lanes
        .deploy
        .clone()
        .or_else(|| config.deploy.enabled.then(|| config.deploy.lane.clone()))
}

fn is_rework_entry_lane(config: &ServiceConfig, lane: &str) -> bool {
    config
        .linear
        .lanes
        .rework
        .as_ref()
        .is_some_and(|rework| same_lane(lane, rework))
        || (config.final_review.enabled && same_lane(lane, &config.final_review.rework_lane))
        || (config.deploy.enabled && same_lane(lane, &config.deploy.rework_lane))
        || (config.independent_review.enabled
            && same_lane(lane, &config.independent_review.rework_lane))
}

fn is_control_lane(config: &ServiceConfig, lane: &str) -> bool {
    config
        .linear
        .lanes
        .do_review
        .as_ref()
        .is_some_and(|do_review| same_lane(lane, do_review))
        || config
            .linear
            .lanes
            .deploy
            .as_ref()
            .is_some_and(|deploy| same_lane(lane, deploy))
        || (config.independent_review.enabled && same_lane(lane, &config.independent_review.lane))
        || (config.deploy.enabled && same_lane(lane, &config.deploy.lane))
}

fn is_projected_active_lane(config: &ServiceConfig, lane: &str) -> bool {
    config.mob.templates.values().any(|template| {
        template
            .linear_projection
            .as_ref()
            .is_some_and(|projection| {
                projection
                    .node_lanes
                    .values()
                    .any(|projected_lane| same_lane(projected_lane, lane))
            })
    })
}

fn same_lane(left: &str, right: &str) -> bool {
    normalize_state(left) == normalize_state(right)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearCommentSnapshot {
    pub id: LinearCommentId,
    pub body_mark: String,
    pub body_markdown: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinearCommentState {
    #[serde(default)]
    issues: BTreeMap<String, BTreeMap<LinearCommentId, String>>,
}

impl LinearCommentState {
    pub fn read(path: &Path) -> Result<Self> {
        match fs::read_to_string(path) {
            Ok(contents) if contents.trim().is_empty() => Ok(Self {
                issues: BTreeMap::new(),
            }),
            Ok(contents) => serde_json::from_str(&contents).map_err(|error| {
                FugueError::InvalidConfig(format!(
                    "parse Linear comment state {}: {error}",
                    path.display()
                ))
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Self {
                issues: BTreeMap::new(),
            }),
            Err(error) => Err(FugueError::Workspace(format!(
                "read Linear comment state {}: {error}",
                path.display()
            ))),
        }
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                FugueError::Workspace(format!(
                    "create Linear comment state directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let rendered = serde_json::to_string_pretty(self)
            .map_err(|error| FugueError::InvalidConfig(error.to_string()))?;
        atomic_write(path, rendered.as_bytes()).map_err(|error| {
            FugueError::Workspace(format!(
                "write Linear comment state {}: {error}",
                path.display()
            ))
        })?;
        Ok(())
    }

    pub fn observe_issue_comments(
        &mut self,
        identifier: &str,
        comments: Vec<LinearCommentSnapshot>,
    ) -> LinearCommentObservation {
        let next = comments
            .into_iter()
            .map(|comment| {
                (
                    comment.id,
                    LinearCommentObservedBody {
                        mark: comment.body_mark,
                        markdown: comment.body_markdown,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let next_marks = next
            .iter()
            .map(|(comment_id, body)| (comment_id.clone(), body.mark.clone()))
            .collect::<BTreeMap<_, _>>();
        let Some(previous) = self.issues.insert(identifier.to_string(), next_marks) else {
            return LinearCommentObservation::FirstSeen;
        };
        let mut changes = Vec::new();
        for (comment_id, next_body) in &next {
            match previous.get(comment_id) {
                Some(previous_mark) if previous_mark != &next_body.mark => {
                    changes.push(LinearCommentChange::Changed {
                        comment_id: comment_id.clone(),
                        body_markdown: next_body.markdown.clone(),
                    });
                }
                Some(_) => {}
                None => {
                    changes.push(LinearCommentChange::Added {
                        comment_id: comment_id.clone(),
                        body_markdown: next_body.markdown.clone(),
                    });
                }
            }
        }
        for comment_id in previous.keys() {
            if !next.contains_key(comment_id) {
                changes.push(LinearCommentChange::Deleted {
                    comment_id: comment_id.clone(),
                });
            }
        }
        if changes.is_empty() {
            LinearCommentObservation::Unchanged
        } else {
            LinearCommentObservation::Changed { changes }
        }
    }
}

fn atomic_write(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let tmp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    fs::write(&tmp_path, contents)?;
    fs::rename(tmp_path, path)
}

pub type LinearCommentObservation = ImsCommentObservation<LinearCommentId>;
pub type LinearCommentChange = ImsCommentChange<LinearCommentId>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LinearCommentObservedBody {
    mark: String,
    markdown: String,
}

pub fn lane_state_path_for_workflow(workflow_dir: &Path) -> PathBuf {
    workflow_dir.join(".fugue").join("linear-lanes.json")
}

pub fn comment_state_path_for_workflow(workflow_dir: &Path) -> PathBuf {
    workflow_dir.join(".fugue").join(LINEAR_COMMENT_STATE_FILE)
}

pub fn record_internal_lane_change_from_env(identifier: &str, lane: &str) -> Result<bool> {
    let Some(path) = std::env::var_os(LINEAR_LANE_STATE_PATH_ENV).map(PathBuf::from) else {
        return Ok(false);
    };
    let mut state = LinearLaneState::read(&path)?;
    state.observe_lane(identifier, lane);
    state.write(&path)?;
    Ok(true)
}

pub fn issue_ref_for_lane_observation(identifier: String) -> LinearIssueRef {
    LinearIssueRef::Identifier {
        identifier: LinearIssueIdentifier(identifier),
    }
}

pub fn state_ref_for_lane(lane: String) -> LinearStateRef {
    LinearStateRef::Name { name: lane }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use serde_json::json;

    use crate::config::{ServiceConfig, WorkflowDefinition};

    fn classifier_config() -> ServiceConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "linear",
                    "api_key": "token",
                    "project_slug": "project",
                    "intake_states": ["Todo"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "lanes": {
                        "ready": "Todo",
                        "rework": "Rework",
                        "do_review": "Do Review",
                        "deploy": "Deploy",
                        "review_ready": "Ready for review",
                        "blocked": "Blocked"
                    },
                    "required_states": []
                },
                "deploy": {
                    "enabled": true,
                    "allowed_repositories": [
                        {"owner": "lukacf", "name": "meerkat-fugue"}
                    ]
                },
                "independent_review": {
                    "enabled": true
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap()
    }

    #[test]
    fn classifier_uses_configured_deploy_and_review_control_lanes() {
        let config = classifier_config();

        assert_eq!(
            classify_linear_lane(&config, "Do Review"),
            LinearLaneClassification::ControlActive
        );
        assert_eq!(
            classify_linear_lane(&config, "Deploy"),
            LinearLaneClassification::ControlActive
        );
        assert_eq!(
            classify_linear_lane(&config, "Rework"),
            LinearLaneClassification::Intake
        );
        assert_eq!(
            classify_linear_lane(&config, "In Progress"),
            LinearLaneClassification::UnexpectedNonterminal
        );
    }

    #[test]
    fn classifier_and_repair_use_canonical_tracker_lanes() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {
                    "kind": "linear",
                    "api_key": "token",
                    "project_slug": "project",
                    "intake_states": ["Queue"],
                    "in_flight_states": ["Active"],
                    "active_states": ["Queue", "Active", "Review", "Deploy"],
                    "terminal_states": ["Done", "Canceled"]
                },
                "linear_runtime": {
                    "provisioning": {"mode": "validate"},
                    "lanes": {
                        "parked": "Backlog",
                        "ready": "Queue",
                        "active": "Active",
                        "do_review": "Review",
                        "deploy": "Deploy",
                        "blocked": "Blocked",
                        "success": "Done",
                        "canceled": "Canceled"
                    },
                    "required_states": []
                },
                "mob": {
                    "templates": {
                        "planner_coder_reviewer": {
                            "backend": "native-execution",
                            "members": [],
                            "linear_projection": {
                                "id": "planner_coder_reviewer",
                                "node_lanes": {"planner": "Active", "coder": "Active", "reviewer": "Active"},
                                "node_phase_labels": {"planner": "fugue:phase:planning", "coder": "fugue:phase:implementing", "reviewer": "fugue:phase:reviewing"},
                                "aggregate": {"precedence": ["Active"], "fallback": "Active"},
                                "terminal_lanes": {"succeeded": "Done", "failed": "Active", "cancelled": "Canceled"}
                            }
                        }
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(
            classify_linear_lane(&config, "Queue"),
            LinearLaneClassification::Intake
        );
        assert_eq!(
            classify_linear_lane(&config, "Active"),
            LinearLaneClassification::ProjectedActive
        );
        assert_eq!(
            classify_linear_lane(&config, "Review"),
            LinearLaneClassification::ControlActive
        );
        assert_eq!(
            classify_linear_lane(&config, "Deploy"),
            LinearLaneClassification::ControlActive
        );

        let planning = legacy_linear_lane_repair(&config, "Planning").unwrap();
        assert_eq!(planning.target_lane, "Active");
        assert_eq!(planning.labels, vec!["fugue:phase:planning"]);
        assert_eq!(
            legacy_linear_lane_repair(&config, "Todo")
                .unwrap()
                .target_lane,
            "Queue"
        );
        assert_eq!(
            legacy_linear_lane_repair(&config, "Do Review")
                .unwrap()
                .target_lane,
            "Review"
        );
        assert_eq!(
            legacy_linear_lane_repair(&config, "Human Review")
                .unwrap()
                .target_lane,
            "Blocked"
        );
    }

    #[test]
    fn lane_state_distinguishes_internal_echo_from_external_change() {
        let mut state = LinearLaneState {
            lanes: BTreeMap::new(),
        };

        assert_eq!(
            state.observe_lane("LUC-519", "Implementing"),
            LinearLaneObservation::FirstSeen {
                identifier: "LUC-519".to_string(),
                current: "Implementing".to_string(),
            }
        );
        assert_eq!(
            state.observe_lane("LUC-519", "implementing"),
            LinearLaneObservation::Unchanged
        );
        assert_eq!(
            state.lanes.get("LUC-519").map(String::as_str),
            Some("implementing")
        );
        assert_eq!(
            state.observe_lane("LUC-519", "Rework"),
            LinearLaneObservation::Changed {
                identifier: "LUC-519".to_string(),
                previous: "implementing".to_string(),
                current: "Rework".to_string(),
            }
        );
        assert_eq!(
            state.observe_lane("LUC-519", "Rework"),
            LinearLaneObservation::Unchanged
        );
    }

    #[test]
    fn empty_lane_state_file_recovers_as_empty_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("linear-lanes.json");
        fs::write(&path, "").expect("empty state file");

        let state = LinearLaneState::read(&path).expect("empty state is recoverable");

        assert!(state.is_empty());
    }

    #[test]
    fn empty_comment_state_file_recovers_as_empty_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("linear-comments.json");
        fs::write(&path, "").expect("empty comment state file");

        let mut state = LinearCommentState::read(&path).expect("empty state is recoverable");
        let observation = state.observe_issue_comments("LUC-519", Vec::new());

        assert_eq!(observation, LinearCommentObservation::FirstSeen);
    }

    #[test]
    fn comment_state_detects_added_changed_and_deleted_comments() {
        let mut state = LinearCommentState {
            issues: BTreeMap::new(),
        };

        assert_eq!(
            state.observe_issue_comments(
                "LUC-519",
                vec![LinearCommentSnapshot {
                    id: LinearCommentId("comment-1".to_string()),
                    body_mark: "hash-a".to_string(),
                    body_markdown: "a".to_string(),
                }],
            ),
            LinearCommentObservation::FirstSeen
        );

        assert_eq!(
            state.observe_issue_comments(
                "LUC-519",
                vec![
                    LinearCommentSnapshot {
                        id: LinearCommentId("comment-1".to_string()),
                        body_mark: "hash-b".to_string(),
                        body_markdown: "b".to_string(),
                    },
                    LinearCommentSnapshot {
                        id: LinearCommentId("comment-2".to_string()),
                        body_mark: "hash-c".to_string(),
                        body_markdown: "c".to_string(),
                    },
                ],
            ),
            LinearCommentObservation::Changed {
                changes: vec![
                    LinearCommentChange::Changed {
                        comment_id: LinearCommentId("comment-1".to_string()),
                        body_markdown: "b".to_string(),
                    },
                    LinearCommentChange::Added {
                        comment_id: LinearCommentId("comment-2".to_string()),
                        body_markdown: "c".to_string(),
                    },
                ],
            }
        );

        assert_eq!(
            state.observe_issue_comments("LUC-519", vec![]),
            LinearCommentObservation::Changed {
                changes: vec![
                    LinearCommentChange::Deleted {
                        comment_id: LinearCommentId("comment-1".to_string()),
                    },
                    LinearCommentChange::Deleted {
                        comment_id: LinearCommentId("comment-2".to_string()),
                    },
                ],
            }
        );
    }
}
