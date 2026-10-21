use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};

use crate::tools::{MobEventKind, MobEventSnapshot};

pub const REVIEWER_GATE_SCHEMA_VERSION: &str = "fugue.reviewer_gate.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewerGateObservation {
    Absent,
    Invalid(String),
    Present(ReviewerGateArtifact),
}

impl ReviewerGateObservation {
    pub fn requests_replan(&self) -> bool {
        matches!(
            self,
            Self::Present(gate) if gate.requests_replan()
        )
    }

    pub fn allows_pr_handoff(&self) -> bool {
        matches!(
            self,
            Self::Present(gate) if gate.allows_pr_handoff()
        )
    }

    pub const fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }

    pub fn invalid_reason(&self) -> Option<&str> {
        match self {
            Self::Invalid(reason) => Some(reason),
            Self::Absent | Self::Present(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewerGateArtifact {
    pub schema_version: String,
    pub status: ReviewerGateStatus,
    pub next_action: ReviewerGateNextAction,
    pub summary: String,
    #[serde(default, deserialize_with = "deserialize_reviewer_gate_findings")]
    pub findings: Vec<ReviewerGateFinding>,
}

impl ReviewerGateArtifact {
    pub fn requests_replan(&self) -> bool {
        matches!(
            (self.status, self.next_action),
            (ReviewerGateStatus::Red, _)
                | (ReviewerGateStatus::Blocked, _)
                | (_, ReviewerGateNextAction::Replan)
                | (_, ReviewerGateNextAction::Blocked)
        )
    }

    pub fn allows_pr_handoff(&self) -> bool {
        self.status == ReviewerGateStatus::Green
            && self.next_action == ReviewerGateNextAction::PrHandoff
            && self.schema_version == REVIEWER_GATE_SCHEMA_VERSION
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerGateStatus {
    Green,
    Red,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerGateNextAction {
    PrHandoff,
    Replan,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewerGateFinding {
    pub severity: ReviewerGateFindingSeverity,
    pub title: String,
    pub detail: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ReviewerGateFindingWire {
    Full(ReviewerGateFindingFullWire),
    Text(ReviewerGateFindingTextWire),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewerGateFindingFullWire {
    severity: ReviewerGateFindingSeverity,
    title: String,
    detail: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewerGateFindingTextWire {
    severity: ReviewerGateFindingSeverity,
    text: String,
}

impl From<ReviewerGateFindingWire> for ReviewerGateFinding {
    fn from(value: ReviewerGateFindingWire) -> Self {
        match value {
            ReviewerGateFindingWire::Full(ReviewerGateFindingFullWire {
                severity,
                title,
                detail,
            }) => Self {
                severity,
                title,
                detail,
            },
            ReviewerGateFindingWire::Text(ReviewerGateFindingTextWire { severity, text }) => Self {
                severity,
                title: "Reviewer finding".to_string(),
                detail: text,
            },
        }
    }
}

fn deserialize_reviewer_gate_findings<'de, D>(
    deserializer: D,
) -> Result<Vec<ReviewerGateFinding>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<ReviewerGateFindingWire>::deserialize(deserializer)
        .map(|findings| findings.into_iter().map(Into::into).collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerGateFindingSeverity {
    Info,
    Low,
    Medium,
    High,
    Blocker,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowStructuredOutput {
    steps: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ReviewerStepOutput {
    Direct(ReviewerGateArtifact),
    RoleWrapped(ReviewerStepRoleWrappedOutput),
}

impl ReviewerStepOutput {
    fn into_gate(self) -> ReviewerGateArtifact {
        match self {
            Self::Direct(gate) => gate,
            Self::RoleWrapped(output) => output.reviewer,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewerStepRoleWrappedOutput {
    reviewer: ReviewerGateArtifact,
}

pub fn reviewer_gate_output_schema() -> Map<String, Value> {
    object_from_value(json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["schema_version", "status", "next_action", "summary", "findings"],
        "properties": {
            "schema_version": {
                "type": "string",
                "enum": [REVIEWER_GATE_SCHEMA_VERSION]
            },
            "status": {
                "type": "string",
                "enum": ["green", "red", "blocked"]
            },
            "next_action": {
                "type": "string",
                "enum": ["pr_handoff", "replan", "blocked"]
            },
            "summary": {
                "type": "string"
            },
            "findings": {
                "type": "array",
                "items": {
                    "oneOf": [
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["severity", "title", "detail"],
                            "properties": {
                                "severity": {
                                    "type": "string",
                                    "enum": ["info", "low", "medium", "high", "blocker"]
                                },
                                "title": {
                                    "type": "string"
                                },
                                "detail": {
                                    "type": "string"
                                }
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["severity", "text"],
                            "properties": {
                                "severity": {
                                    "type": "string",
                                    "enum": ["info", "low", "medium", "high", "blocker"]
                                },
                                "text": {
                                    "type": "string"
                                }
                            }
                        }
                    ]
                }
            }
        }
    }))
}

pub fn latest_reviewer_gate_observation(events: &[MobEventSnapshot]) -> ReviewerGateObservation {
    events
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            MobEventKind::RunCompleted {
                structured_output: Some(output),
                ..
            } => Some(reviewer_gate_from_flow_structured_output(output)),
            MobEventKind::RunCompleted { .. } => None,
            MobEventKind::RunStarted
            | MobEventKind::MemberStarted { .. }
            | MobEventKind::MemberCompleted { .. }
            | MobEventKind::MemberKickoffUpdated { .. }
            | MobEventKind::ToolCallStarted { .. }
            | MobEventKind::ToolCallCompleted { .. } => None,
        })
        .unwrap_or(ReviewerGateObservation::Absent)
}

fn reviewer_gate_from_flow_structured_output(output: &Value) -> ReviewerGateObservation {
    let flow: FlowStructuredOutput = match serde_json::from_value(output.clone()) {
        Ok(flow) => flow,
        Err(err) => {
            return ReviewerGateObservation::Invalid(format!(
                "Meta flow structured_output did not match Fugue flow output shape: {err}"
            ));
        }
    };
    let Some(reviewer) = flow.steps.get("reviewer") else {
        return ReviewerGateObservation::Invalid(
            "Meta flow structured_output omitted steps.reviewer reviewer gate".to_string(),
        );
    };
    let gate = match serde_json::from_value::<ReviewerStepOutput>(reviewer.clone()) {
        Ok(output) => output.into_gate(),
        Err(err) => {
            return ReviewerGateObservation::Invalid(format!(
                "Meta flow structured_output steps.reviewer did not match Fugue reviewer gate schema: {err}"
            ));
        }
    };
    if gate.schema_version != REVIEWER_GATE_SCHEMA_VERSION {
        return ReviewerGateObservation::Invalid(format!(
            "Meta reviewer gate schema_version {:?} is not supported",
            gate.schema_version
        ));
    }
    ReviewerGateObservation::Present(gate)
}

fn object_from_value(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Map::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::tools::{MobEventSnapshot, MobRunStatus};

    fn terminal_event(output: Value) -> MobEventSnapshot {
        MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc::now(),
            kind: MobEventKind::RunCompleted {
                status: MobRunStatus::Succeeded,
                structured_output: Some(output),
            },
            message: None,
        }
    }

    #[test]
    fn parses_green_reviewer_gate_from_flow_output() {
        let observation = latest_reviewer_gate_observation(&[terminal_event(json!({
            "steps": {
                "reviewer": {
                    "schema_version": REVIEWER_GATE_SCHEMA_VERSION,
                    "status": "green",
                    "next_action": "pr_handoff",
                    "summary": "Local reviewer gate is green.",
                    "findings": []
                }
            }
        }))]);

        assert!(observation.allows_pr_handoff());
        assert!(!observation.requests_replan());
    }

    #[test]
    fn accepts_text_finding_shorthand_from_flow_output() {
        let observation = latest_reviewer_gate_observation(&[terminal_event(json!({
            "steps": {
                "reviewer": {
                    "schema_version": REVIEWER_GATE_SCHEMA_VERSION,
                    "status": "green",
                    "next_action": "pr_handoff",
                    "summary": "Reviewer gate is green.",
                    "findings": [{
                        "severity": "info",
                        "text": "Reviewer produced a concise structured finding."
                    }]
                }
            }
        }))]);

        let ReviewerGateObservation::Present(gate) = observation else {
            panic!("text finding shorthand should parse as a typed reviewer gate");
        };
        assert!(gate.allows_pr_handoff());
        assert_eq!(gate.findings.len(), 1);
        assert_eq!(gate.findings[0].title, "Reviewer finding");
        assert_eq!(
            gate.findings[0].detail,
            "Reviewer produced a concise structured finding."
        );
    }

    #[test]
    fn accepts_role_wrapped_reviewer_gate_from_meta_flow_output() {
        let observation = latest_reviewer_gate_observation(&[terminal_event(json!({
            "steps": {
                "reviewer": {
                    "reviewer": {
                        "schema_version": REVIEWER_GATE_SCHEMA_VERSION,
                        "status": "green",
                        "next_action": "pr_handoff",
                        "summary": "Meta wraps one-to-one step output under the member role.",
                        "findings": [{
                            "severity": "info",
                            "title": "Validation",
                            "detail": "cargo test passed"
                        }]
                    }
                }
            }
        }))]);

        let ReviewerGateObservation::Present(gate) = observation else {
            panic!("role-wrapped reviewer step output should parse as a typed reviewer gate");
        };
        assert!(gate.allows_pr_handoff());
        assert_eq!(gate.findings.len(), 1);
        assert_eq!(gate.findings[0].title, "Validation");
    }

    #[test]
    fn parses_red_reviewer_gate_as_replan_request() {
        let observation = latest_reviewer_gate_observation(&[terminal_event(json!({
            "steps": {
                "reviewer": {
                    "schema_version": REVIEWER_GATE_SCHEMA_VERSION,
                    "status": "red",
                    "next_action": "replan",
                    "summary": "Tests are missing.",
                    "findings": [{
                        "severity": "high",
                        "title": "Missing test",
                        "detail": "Add the focused regression test before PR handoff."
                    }]
                }
            }
        }))]);

        assert!(observation.requests_replan());
        assert!(!observation.allows_pr_handoff());
    }

    #[test]
    fn durable_reviewer_gate_survives_later_terminal_event_without_structured_output() {
        let observation = latest_reviewer_gate_observation(&[
            terminal_event(json!({
                "steps": {
                    "reviewer": {
                        "schema_version": REVIEWER_GATE_SCHEMA_VERSION,
                        "status": "green",
                        "next_action": "pr_handoff",
                        "summary": "Reviewer gate passed before finalization failed.",
                        "findings": []
                    }
                }
            })),
            MobEventSnapshot {
                sequence: 2,
                occurred_at: Utc::now(),
                kind: MobEventKind::RunCompleted {
                    status: MobRunStatus::Failed,
                    structured_output: None,
                },
                message: Some(
                    "turn abandoned: apply failed: Terminal failure: Failed (LlmFailure)"
                        .to_string(),
                ),
            },
        ]);

        assert!(observation.allows_pr_handoff());
        assert!(!observation.requests_replan());
    }

    #[test]
    fn invalid_reviewer_gate_does_not_look_green() {
        let observation = latest_reviewer_gate_observation(&[terminal_event(json!({
            "steps": {
                "reviewer": {
                    "status": "green",
                    "summary": "Missing schema version."
                }
            }
        }))]);

        assert!(observation.is_invalid());
        assert!(!observation.allows_pr_handoff());
    }
}
