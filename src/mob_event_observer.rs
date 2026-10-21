use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::ServiceConfig;
use crate::error::{FugueError, Result};
use crate::mob_tools::{
    MetaStructuralEventCursor, NativeMobExecutionHost, ObserveStructuralEventsInput,
};
use crate::tools::{
    Cursor, FugueToolOutput, FugueToolResult, IdempotencyKey, LinearIssueRef, MobEventKind,
    MobEventSnapshot, MobRunId, MobRunSnapshot, ObserveEventsOutput, UpsertWorkpadInput,
};

const CURSOR_FILE_NAME: &str = "meta-structural-cursors.json";
const WORKPAD_HEADING: &str = "## Fugue Workpad";

#[derive(Debug, Clone)]
pub struct MobEventCursorStore {
    path: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoredMobEventCursors {
    version: u16,
    cursors: BTreeMap<String, StoredMobEventCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMobEventCursor {
    cursor: Cursor,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMobObservation {
    pub events: Vec<MobEventSnapshot>,
    pub next_cursor: Option<MetaStructuralEventCursor>,
}

impl MobEventCursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn for_config(config: &ServiceConfig) -> Self {
        Self::new(config.workflow_dir.join(".fugue").join(CURSOR_FILE_NAME))
    }

    pub fn load(&self, run_id: &MobRunId) -> Result<Option<MetaStructuralEventCursor>> {
        let stored = self.read_all()?;
        Ok(stored
            .cursors
            .get(&run_id.0)
            .map(|cursor| MetaStructuralEventCursor(cursor.cursor.clone())))
    }

    pub fn advance(
        &self,
        run_id: &MobRunId,
        cursor: MetaStructuralEventCursor,
        observed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let mut stored = self.read_all()?;
        stored.version = 1;
        let should_advance = stored
            .cursors
            .get(&run_id.0)
            .map(|current| cursor_after(&cursor.0, &current.cursor))
            .unwrap_or(true);
        if !should_advance {
            return Ok(false);
        }
        stored.cursors.insert(
            run_id.0.clone(),
            StoredMobEventCursor {
                cursor: cursor.0,
                observed_at,
            },
        );
        self.write_all(&stored)?;
        Ok(true)
    }

    pub fn reset(&self, run_id: &MobRunId) -> Result<bool> {
        let mut stored = self.read_all()?;
        let removed = stored.cursors.remove(&run_id.0).is_some();
        if removed {
            self.write_all(&stored)?;
        }
        Ok(removed)
    }

    fn read_all(&self) -> Result<StoredMobEventCursors> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => serde_json::from_str(&contents).map_err(|err| {
                FugueError::Workspace(format!(
                    "failed to parse Meta structural cursor store {}: {err}",
                    self.path.display()
                ))
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(StoredMobEventCursors {
                version: 1,
                cursors: BTreeMap::new(),
            }),
            Err(err) => Err(FugueError::Workspace(format!(
                "failed to read Meta structural cursor store {}: {err}",
                self.path.display()
            ))),
        }
    }

    fn write_all(&self, stored: &StoredMobEventCursors) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                FugueError::Workspace(format!(
                    "failed to create Meta cursor directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let tmp_path = temporary_path(&self.path);
        let contents = serde_json::to_string_pretty(stored).map_err(|err| {
            FugueError::Workspace(format!("failed to encode Meta cursor store: {err}"))
        })?;
        fs::write(&tmp_path, contents).map_err(|err| {
            FugueError::Workspace(format!(
                "failed to write Meta cursor store {}: {err}",
                tmp_path.display()
            ))
        })?;
        fs::rename(&tmp_path, &self.path).map_err(|err| {
            FugueError::Workspace(format!(
                "failed to replace Meta cursor store {}: {err}",
                self.path.display()
            ))
        })
    }
}

#[derive(Debug, Clone)]
pub struct NativeMobEventObserver {
    cursor_store: MobEventCursorStore,
}

impl NativeMobEventObserver {
    pub fn new(cursor_store: MobEventCursorStore) -> Self {
        Self { cursor_store }
    }

    pub fn for_config(config: &ServiceConfig) -> Self {
        Self::new(MobEventCursorStore::for_config(config))
    }

    pub fn observe(
        &self,
        host: &(dyn NativeMobExecutionHost + Send + Sync),
        run_id: &MobRunId,
        limit: u16,
    ) -> Result<NativeMobObservation> {
        let cursor = self.cursor_store.load(run_id)?;
        let input = ObserveStructuralEventsInput {
            run_id: run_id.clone(),
            cursor: cursor.clone(),
            limit,
        };
        let result = host.observe_structural_events(input.clone());
        match decode_observation_result(result) {
            Ok(observation) => Ok(observation),
            Err(error) if cursor.is_some() && stale_mob_event_cursor_error(&error) => {
                self.cursor_store.reset(run_id)?;
                decode_observation_result(host.observe_structural_events(
                    ObserveStructuralEventsInput {
                        cursor: None,
                        ..input
                    },
                ))
            }
            Err(error) => Err(error),
        }
    }

    pub fn commit(&self, run_id: &MobRunId, observation: &NativeMobObservation) -> Result<bool> {
        let Some(cursor) = observation.next_cursor.clone() else {
            return Ok(false);
        };
        self.cursor_store.advance(run_id, cursor, Utc::now())
    }
}

fn decode_observation_result(result: FugueToolResult) -> Result<NativeMobObservation> {
    let (events, next_cursor) = match result {
        FugueToolResult::Ok {
            output:
                FugueToolOutput::MobObserveEvents(ObserveEventsOutput {
                    events,
                    next_cursor,
                }),
            ..
        } => (events, next_cursor.map(MetaStructuralEventCursor)),
        FugueToolResult::Ok { output, .. } => {
            return Err(FugueError::Workspace(format!(
                "Meta structural event observation returned unexpected output: {output:?}"
            )));
        }
        FugueToolResult::Err { error, .. } => {
            return Err(FugueError::Workspace(format!(
                "Meta structural event observation failed: {}",
                error.message
            )));
        }
    };
    Ok(NativeMobObservation {
        events,
        next_cursor,
    })
}

fn stale_mob_event_cursor_error(error: &FugueError) -> bool {
    match error {
        FugueError::Workspace(message) => {
            message.contains("stale mob event cursor")
                || (message.contains("after_cursor") && message.contains("latest_cursor"))
        }
        _ => false,
    }
}

pub fn native_mob_workpad_input(
    issue: LinearIssueRef,
    run: &MobRunSnapshot,
    events: &[MobEventSnapshot],
) -> UpsertWorkpadInput {
    native_mob_workpad_input_with_existing(issue, run, events, None)
}

pub fn native_mob_workpad_input_with_existing(
    issue: LinearIssueRef,
    run: &MobRunSnapshot,
    events: &[MobEventSnapshot],
    existing_body: Option<&str>,
) -> UpsertWorkpadInput {
    let cursor_suffix = events.iter().map(|event| event.sequence).max().unwrap_or(0);
    UpsertWorkpadInput {
        issue,
        heading: WORKPAD_HEADING.to_string(),
        body_markdown: render_native_mob_workpad_with_existing(run, events, existing_body),
        create_if_missing: true,
        idempotency_key: IdempotencyKey(format!(
            "native-mob-workpad:{}:{cursor_suffix}",
            run.run_id.0
        )),
    }
}

pub fn native_mob_workpad_should_upsert(
    events: &[MobEventSnapshot],
    existing_body: Option<&str>,
) -> bool {
    latest_role_workpad(events).is_some()
        || events.iter().any(event_contains_handoff_evidence)
        || existing_body
            .and_then(extract_non_generic_workpad_body)
            .is_some()
}

fn event_contains_handoff_evidence(event: &MobEventSnapshot) -> bool {
    matches!(
        event.kind,
        MobEventKind::ToolCallCompleted {
            operation: crate::tools::FugueToolOperation::GitHubCreatePr
                | crate::tools::FugueToolOperation::GitHubCheckCi
                | crate::tools::FugueToolOperation::GitHubMergePr,
        } | MobEventKind::RunCompleted { .. }
    )
}

pub fn render_native_mob_workpad_with_existing(
    run: &MobRunSnapshot,
    events: &[MobEventSnapshot],
    existing_body: Option<&str>,
) -> String {
    let merged = crate::review_findings::merge_preserved_review_findings(
        &render_native_mob_workpad_with_base(run, events, existing_body),
        existing_body,
    );
    let mut normalized = merged.trim_end().to_string();
    normalized.push('\n');
    normalized
}

pub fn render_native_mob_workpad(run: &MobRunSnapshot, events: &[MobEventSnapshot]) -> String {
    render_native_mob_workpad_with_base(run, events, None)
}

fn render_native_mob_workpad_with_base(
    _run: &MobRunSnapshot,
    events: &[MobEventSnapshot],
    existing_body: Option<&str>,
) -> String {
    let progress = progress_lines(events);
    let role_workpad = latest_role_workpad(events);
    let existing_workpad = existing_body
        .and_then(extract_non_generic_workpad_body)
        .map(str::to_string);
    let mut body = String::new();
    let authoritative_workpad = select_authoritative_workpad(role_workpad, existing_workpad);
    let has_authoritative_workpad = authoritative_workpad.is_some();
    match authoritative_workpad {
        Some(workpad) => {
            body.push_str(workpad.trim());
            body.push_str("\n\n");
        }
        None => {
            body.push_str("### Plan\n");
            body.push_str("- [ ] Pending planner-authored issue-specific checkbox plan.\n\n");
        }
    }
    if !has_authoritative_workpad || !progress.is_empty() {
        append_progress_section_if_missing(&mut body, &progress);
    }
    if !has_authoritative_workpad {
        body.push_str("\n### Pull request or Handoff\n");
        body.push_str("- [ ] Pending PR or handoff evidence from the repo agent.\n");
    }
    body
}

fn append_progress_section_if_missing(body: &mut String, progress: &[String]) {
    if contains_markdown_section(body, "Progress") {
        return;
    }
    body.push_str("### Progress\n");
    if progress.is_empty() {
        body.push_str("- [ ] Waiting for planner-authored Workpad content.\n");
    } else {
        for line in progress {
            body.push_str("- [x] ");
            body.push_str(line);
            body.push('\n');
        }
    }
    body.push('\n');
}

fn contains_markdown_section(body: &str, section: &str) -> bool {
    let expected = format!("### {section}");
    body.lines().any(|line| line.trim() == expected)
}

fn latest_role_workpad(events: &[MobEventSnapshot]) -> Option<String> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            MobEventKind::MemberCompleted { member }
                if member == "planner" || member == "rework" =>
            {
                event
                    .message
                    .as_deref()
                    .and_then(member_message_text)
                    .and_then(|message| extract_workpad_body(message.as_str()).map(str::to_string))
                    .filter(|workpad| workpad_contains_issue_specific_plan(workpad))
                    .map(|workpad| (event.sequence, workpad))
            }
            _ => None,
        })
        .max_by_key(|(sequence, _)| *sequence)
        .map(|(_, workpad)| workpad)
}

fn extract_non_generic_workpad_body(body: &str) -> Option<&str> {
    let workpad = extract_workpad_body(body).unwrap_or(body);
    workpad_contains_issue_specific_plan(workpad).then_some(workpad)
}

fn select_authoritative_workpad(
    role_workpad: Option<String>,
    existing_workpad: Option<String>,
) -> Option<String> {
    match (role_workpad, existing_workpad) {
        (Some(role), Some(existing))
            if existing_workpad_has_later_handoff_evidence(&existing)
                && !existing_workpad_has_later_handoff_evidence(&role) =>
        {
            Some(existing)
        }
        (Some(role), _) => Some(role),
        (None, existing) => existing,
    }
}

fn existing_workpad_has_later_handoff_evidence(workpad: &str) -> bool {
    workpad.contains("/pull/")
        || workpad.contains("PR #")
        || workpad.contains("Reviewer decision:")
        || workpad.contains("### Reviewer gate")
        || workpad.contains("### Pull request or Handoff\n- [x]")
}

fn extract_workpad_body(message: &str) -> Option<&str> {
    let mut offset = 0;
    for line in message.lines() {
        let trimmed = line.trim();
        if trimmed == WORKPAD_HEADING
            || (trimmed.starts_with("## ")
                && trimmed.ends_with(" Workpad")
                && trimmed.len() > "##  Workpad".len())
        {
            return Some(message[offset + line.len()..].trim());
        }
        offset += line.len() + 1;
    }
    None
}

fn workpad_contains_issue_specific_plan(workpad: &str) -> bool {
    let has_issue_specific_section = [
        "### Plan",
        "### Implementation plan",
        "### Protected behavior",
        "### Acceptance criteria",
        "### Acceptance checklist",
    ]
    .iter()
    .any(|section| workpad.contains(section));
    has_issue_specific_section
        && (workpad.contains("- [ ]") || workpad.contains("- [x]") || workpad.contains("- [X]"))
        && !workpad.contains("Launch native execution mob through Meta")
        && !workpad.contains("Pending planner-authored issue-specific checkbox plan")
}

#[derive(Debug, Deserialize)]
struct NativeMemberResponseText {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    content: Option<NativeMemberContent>,
    #[serde(default)]
    result: Option<Box<NativeMemberResponseText>>,
    #[serde(default)]
    response_summary: Option<Box<NativeMemberResponseText>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum NativeMemberContent {
    Text(String),
    Parts(Vec<NativeMemberContentPart>),
}

#[derive(Debug, Deserialize)]
struct NativeMemberContentPart {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

fn member_message_text(message: &str) -> Option<String> {
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str::<NativeMemberResponseText>(trimmed) {
        Ok(response) => {
            let mut out = String::new();
            response.collect_text(&mut out);
            (!out.trim().is_empty()).then(|| out.trim().to_string())
        }
        Err(_) => Some(trimmed.to_string()),
    }
}

impl NativeMemberResponseText {
    fn collect_text(&self, out: &mut String) {
        push_text(out, self.text.as_deref());
        push_text(out, self.message.as_deref());
        if let Some(content) = self.content.as_ref() {
            content.collect_text(out);
        }
        if let Some(result) = self.result.as_ref() {
            result.collect_text(out);
        }
        if let Some(summary) = self.response_summary.as_ref() {
            summary.collect_text(out);
        }
    }
}

impl NativeMemberContent {
    fn collect_text(&self, out: &mut String) {
        match self {
            Self::Text(text) => push_text(out, Some(text)),
            Self::Parts(parts) => {
                for part in parts {
                    push_text(out, part.text.as_deref());
                    push_text(out, part.content.as_deref());
                }
            }
        }
    }
}

fn push_text(out: &mut String, text: Option<&str>) {
    let Some(text) = text.map(str::trim).filter(|text| !text.is_empty()) else {
        return;
    };
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(text);
}

pub fn merge_mob_event_snapshots(
    existing: &mut Vec<MobEventSnapshot>,
    incoming: &[MobEventSnapshot],
) {
    let mut seen = existing
        .iter()
        .map(|event| event.sequence)
        .collect::<BTreeSet<_>>();
    existing.extend(
        incoming
            .iter()
            .filter(|event| seen.insert(event.sequence))
            .cloned(),
    );
    existing.sort_by_key(|event| event.sequence);
}

fn progress_lines(events: &[MobEventSnapshot]) -> Vec<String> {
    let mut lines = BTreeSet::new();
    for event in events {
        match &event.kind {
            MobEventKind::RunStarted => {
                lines.insert("Issue mob started.".to_string());
            }
            MobEventKind::MemberStarted { member } => {
                lines.insert(format!("{} started.", normalize_member(member)));
            }
            MobEventKind::MemberCompleted { member } => {
                let member = normalize_member(member);
                match event
                    .message
                    .as_deref()
                    .and_then(member_message_text)
                    .filter(|message| !message.trim().is_empty())
                    .and_then(|message| member_progress_summary(message.trim()))
                {
                    Some(summary) => {
                        lines.insert(format!("{member} completed: {summary}"));
                    }
                    None => {
                        lines.insert(format!("{member} completed."));
                    }
                }
            }
            MobEventKind::MemberKickoffUpdated { .. } => {}
            MobEventKind::ToolCallStarted { .. } => {}
            MobEventKind::ToolCallCompleted { operation } => {
                if let Some(summary) = tool_progress_summary(*operation, event.message.as_deref()) {
                    lines.insert(summary);
                }
            }
            MobEventKind::RunCompleted { status, .. } => {
                match event
                    .message
                    .as_deref()
                    .filter(|message| !message.trim().is_empty())
                {
                    Some(message) => {
                        let summary = progress_message_summary(message.trim());
                        lines.insert(format!(
                            "Issue mob completed with status `{status:?}`: {summary}"
                        ));
                    }
                    None => {
                        lines.insert(format!("Issue mob completed with status `{status:?}`."));
                    }
                }
            }
        }
    }
    lines.into_iter().collect()
}

fn member_progress_summary(message: &str) -> Option<String> {
    if extract_workpad_body(message).is_some() {
        return Some("published Fugue Workpad plan/checkoffs.".to_string());
    }
    if is_opaque_member_message(message) || is_mechanism_diagnostic_message(message) {
        return None;
    }
    Some(progress_message_summary(message))
}

fn progress_message_summary(message: &str) -> String {
    const MAX_PROGRESS_MESSAGE_CHARS: usize = 500;
    if message.chars().count() <= MAX_PROGRESS_MESSAGE_CHARS {
        return message.to_string();
    }
    let mut truncated = message
        .chars()
        .take(MAX_PROGRESS_MESSAGE_CHARS)
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn tool_progress_summary(
    operation: crate::tools::FugueToolOperation,
    message: Option<&str>,
) -> Option<String> {
    let message = message.map(str::trim).filter(|message| !message.is_empty());
    match operation {
        crate::tools::FugueToolOperation::GitHubCreatePr => Some(format!(
            "Pull request evidence: {}",
            message
                .map(progress_message_summary)
                .unwrap_or_else(|| "PR creation completed.".to_string())
        )),
        crate::tools::FugueToolOperation::GitHubCheckCi => Some(format!(
            "CI evidence: {}",
            message
                .map(progress_message_summary)
                .unwrap_or_else(|| "CI check completed.".to_string())
        )),
        crate::tools::FugueToolOperation::GitHubMergePr => Some(format!(
            "Merge evidence: {}",
            message
                .map(progress_message_summary)
                .unwrap_or_else(|| "PR merge completed.".to_string())
        )),
        _ => None,
    }
}

fn is_opaque_member_message(message: &str) -> bool {
    matches!(message.trim().to_ascii_lowercase().as_str(), "ok" | "done")
}

fn is_mechanism_diagnostic_message(message: &str) -> bool {
    message.contains("native member tool/MCP availability")
        || message.contains("native member tool/MCP launch preflight")
        || message.contains("readiness_gate=")
        || message.contains("state_bound_rpc=[")
        || message.contains("agent_side_mob_tools=[")
        || message.contains("in_mob_operator_tools=[")
        || message.contains("configured_mcp_servers=[")
        || message.starts_with("mob/flow_run run_id=")
}

fn normalize_member(member: &str) -> String {
    match member {
        "planner" => "Planner".to_string(),
        "coder" => "Coder".to_string(),
        "reviewer" => "Reviewer".to_string(),
        "rework" => "Rework".to_string(),
        other => other.to_string(),
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!("{value}.tmp"))
        .unwrap_or_else(|| "tmp".to_string());
    tmp.set_extension(extension);
    tmp
}

fn cursor_after(candidate: &Cursor, current: &Cursor) -> bool {
    match (cursor_sequence(candidate), cursor_sequence(current)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => candidate.0 > current.0,
    }
}

pub fn cursor_sequence(cursor: &Cursor) -> Option<u64> {
    cursor
        .0
        .strip_prefix("sequence:")
        .unwrap_or(cursor.0.as_str())
        .parse::<u64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mob_tools::ObserveStructuralEventsInput;
    use crate::tools::{
        FugueToolError, FugueToolErrorCode, FugueToolOperation, LinearIssueIdentifier,
        MobRunStatus, RetryAdvice,
    };
    use chrono::TimeZone;
    use std::sync::{Arc, Mutex};

    struct RecordingObserveHost {
        seen_cursors: Arc<Mutex<Vec<Option<MetaStructuralEventCursor>>>>,
    }

    impl NativeMobExecutionHost for RecordingObserveHost {
        fn list_runs(&self, _input: crate::tools::ListRunsInput) -> FugueToolResult {
            unimplemented!()
        }

        fn status(&self, _input: crate::tools::InspectRunInput) -> FugueToolResult {
            unimplemented!()
        }

        fn spawn_or_resume_execution(
            &self,
            _input: crate::tools::SpawnOrResumeExecutionInput,
        ) -> FugueToolResult {
            unimplemented!()
        }

        fn cancel_execution(&self, _input: crate::tools::CancelExecutionInput) -> FugueToolResult {
            unimplemented!()
        }

        fn observe_structural_events(
            &self,
            input: ObserveStructuralEventsInput,
        ) -> FugueToolResult {
            self.seen_cursors.lock().unwrap().push(input.cursor);
            FugueToolResult::Ok {
                output: FugueToolOutput::MobObserveEvents(ObserveEventsOutput {
                    events: Vec::new(),
                    next_cursor: Some(Cursor("sequence:7".to_string())),
                }),
                metadata: crate::tools::ToolExecutionMetadata::default(),
            }
        }
    }

    #[test]
    fn meta_event_cursor_persistence_is_replay_safe_across_restart() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cursors.json");
        let store = MobEventCursorStore::new(&path);
        let run_id = MobRunId("run-1".to_string());
        let at = Utc.timestamp_opt(10, 0).unwrap();

        assert!(
            store
                .advance(
                    &run_id,
                    MetaStructuralEventCursor(Cursor("sequence:4".to_string())),
                    at,
                )
                .unwrap()
        );
        assert!(
            !store
                .advance(
                    &run_id,
                    MetaStructuralEventCursor(Cursor("sequence:3".to_string())),
                    at,
                )
                .unwrap()
        );

        let restarted = MobEventCursorStore::new(path);
        assert_eq!(
            restarted.load(&run_id).unwrap(),
            Some(MetaStructuralEventCursor(Cursor("sequence:4".to_string())))
        );
    }

    #[test]
    fn native_event_observer_uses_persisted_cursor_and_commits_returned_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let store = MobEventCursorStore::new(temp.path().join("cursors.json"));
        let run_id = MobRunId("run-1".to_string());
        store
            .advance(
                &run_id,
                MetaStructuralEventCursor(Cursor("sequence:2".to_string())),
                Utc.timestamp_opt(1, 0).unwrap(),
            )
            .unwrap();
        let seen_cursors = Arc::new(Mutex::new(Vec::new()));
        let host = RecordingObserveHost {
            seen_cursors: seen_cursors.clone(),
        };
        let observer = NativeMobEventObserver::new(store.clone());

        let observation = observer.observe(&host, &run_id, 25).unwrap();
        assert!(observer.commit(&run_id, &observation).unwrap());

        assert_eq!(
            seen_cursors.lock().unwrap().as_slice(),
            &[Some(MetaStructuralEventCursor(Cursor(
                "sequence:2".to_string()
            )))]
        );
        assert_eq!(
            store.load(&run_id).unwrap(),
            Some(MetaStructuralEventCursor(Cursor("sequence:7".to_string())))
        );
    }

    #[test]
    fn native_event_observer_does_not_advance_cursor_until_observation_is_committed() {
        let temp = tempfile::tempdir().unwrap();
        let store = MobEventCursorStore::new(temp.path().join("cursors.json"));
        let run_id = MobRunId("run-1".to_string());
        let seen_cursors = Arc::new(Mutex::new(Vec::new()));
        let host = RecordingObserveHost {
            seen_cursors: seen_cursors.clone(),
        };
        let observer = NativeMobEventObserver::new(store.clone());

        let observation = observer.observe(&host, &run_id, 25).unwrap();

        assert_eq!(store.load(&run_id).unwrap(), None);
        assert!(observer.commit(&run_id, &observation).unwrap());
        assert_eq!(
            store.load(&run_id).unwrap(),
            Some(MetaStructuralEventCursor(Cursor("sequence:7".to_string())))
        );
    }

    struct StaleCursorThenResetHost {
        seen_cursors: Arc<Mutex<Vec<Option<MetaStructuralEventCursor>>>>,
    }

    impl NativeMobExecutionHost for StaleCursorThenResetHost {
        fn list_runs(&self, _input: crate::tools::ListRunsInput) -> FugueToolResult {
            unimplemented!()
        }

        fn status(&self, _input: crate::tools::InspectRunInput) -> FugueToolResult {
            unimplemented!()
        }

        fn spawn_or_resume_execution(
            &self,
            _input: crate::tools::SpawnOrResumeExecutionInput,
        ) -> FugueToolResult {
            unimplemented!()
        }

        fn cancel_execution(&self, _input: crate::tools::CancelExecutionInput) -> FugueToolResult {
            unimplemented!()
        }

        fn observe_structural_events(
            &self,
            input: ObserveStructuralEventsInput,
        ) -> FugueToolResult {
            self.seen_cursors.lock().unwrap().push(input.cursor.clone());
            if input.cursor.is_some() {
                return FugueToolResult::Err {
                    error: FugueToolError {
                        operation: FugueToolOperation::MobObserveEvents,
                        code: FugueToolErrorCode::ExecutionFailed,
                        message:
                            "Meta structural event query failed: stale mob event cursor: requested 24, latest 15"
                                .to_string(),
                        retry: RetryAdvice::SafeToRetry,
                        idempotency_key: None,
                    },
                    metadata: crate::tools::ToolExecutionMetadata::default(),
                };
            }
            FugueToolResult::Ok {
                output: FugueToolOutput::MobObserveEvents(ObserveEventsOutput {
                    events: Vec::new(),
                    next_cursor: Some(Cursor("sequence:15".to_string())),
                }),
                metadata: crate::tools::ToolExecutionMetadata::default(),
            }
        }
    }

    #[test]
    fn native_event_observer_resets_stale_cursor_after_runtime_store_quarantine() {
        let temp = tempfile::tempdir().unwrap();
        let store = MobEventCursorStore::new(temp.path().join("cursors.json"));
        let run_id = MobRunId("run-1".to_string());
        store
            .advance(
                &run_id,
                MetaStructuralEventCursor(Cursor("sequence:24".to_string())),
                Utc.timestamp_opt(1, 0).unwrap(),
            )
            .unwrap();
        let seen_cursors = Arc::new(Mutex::new(Vec::new()));
        let host = StaleCursorThenResetHost {
            seen_cursors: seen_cursors.clone(),
        };
        let observer = NativeMobEventObserver::new(store.clone());

        let observation = observer.observe(&host, &run_id, 25).unwrap();
        assert!(observer.commit(&run_id, &observation).unwrap());

        assert_eq!(
            seen_cursors.lock().unwrap().as_slice(),
            &[
                Some(MetaStructuralEventCursor(Cursor("sequence:24".to_string()))),
                None,
            ]
        );
        assert_eq!(
            store.load(&run_id).unwrap(),
            Some(MetaStructuralEventCursor(Cursor("sequence:15".to_string())))
        );
    }

    #[test]
    fn workpad_progress_updates_are_idempotent_and_do_not_duplicate_sections() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-172".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let events = vec![
            MobEventSnapshot {
                sequence: 1,
                occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
                kind: MobEventKind::MemberCompleted {
                    member: "planner".to_string(),
                },
                message: Some("Plan drafted".to_string()),
            },
            MobEventSnapshot {
                sequence: 1,
                occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
                kind: MobEventKind::MemberCompleted {
                    member: "planner".to_string(),
                },
                message: Some("Plan drafted".to_string()),
            },
            MobEventSnapshot {
                sequence: 2,
                occurred_at: Utc.timestamp_opt(2, 0).unwrap(),
                kind: MobEventKind::MemberStarted {
                    member: "coder".to_string(),
                },
                message: None,
            },
        ];

        let first = render_native_mob_workpad(&run, &events);
        let second = render_native_mob_workpad(&run, &events);

        assert_eq!(first, second);
        assert_eq!(first.matches("### Progress").count(), 1);
        assert_eq!(first.matches("Planner completed: Plan drafted").count(), 1);
        assert_eq!(first.matches("Coder started.").count(), 1);
        assert_eq!(first.matches("### Pull request or Handoff").count(), 1);
        assert!(!first.contains("Launch native execution mob through Meta"));
        assert!(!first.contains("Observe Meta structural events"));
        assert!(!first.contains("`cargo fmt --check`"));
    }

    #[test]
    fn sequential_meta_batches_preserve_cumulative_workpad_progress() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-172".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let first_batch = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "planner".to_string(),
            },
            message: Some("Plan drafted".to_string()),
        }];
        let second_batch = vec![MobEventSnapshot {
            sequence: 2,
            occurred_at: Utc.timestamp_opt(2, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "coder".to_string(),
            },
            message: Some("Implementation pushed".to_string()),
        }];
        let mut cumulative = Vec::new();

        merge_mob_event_snapshots(&mut cumulative, &first_batch);
        merge_mob_event_snapshots(&mut cumulative, &second_batch);
        merge_mob_event_snapshots(&mut cumulative, &first_batch);
        let rendered = render_native_mob_workpad(&run, &cumulative);

        assert_eq!(
            rendered.matches("Planner completed: Plan drafted").count(),
            1
        );
        assert_eq!(
            rendered
                .matches("Coder completed: Implementation pushed")
                .count(),
            1
        );
        assert!(rendered.contains("Planner completed: Plan drafted"));
        assert!(rendered.contains("Coder completed: Implementation pushed"));
    }

    #[test]
    fn planner_workpad_from_serialized_meta_response_becomes_authoritative_plan() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-224".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let planner_output = serde_json::json!({
            "result": {
                "text": "## Fugue Workpad\n\n### Plan\n- [ ] Add typed Meta trace records.\n- [ ] Persist trace evidence on each native run.\n\n### Acceptance criteria\n- [ ] Operators can inspect trace output without SSH spelunking.\n"
            }
        })
        .to_string();
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "planner".to_string(),
            },
            message: Some(planner_output),
        }];

        let rendered = render_native_mob_workpad(&run, &events);

        assert!(rendered.contains("Add typed Meta trace records"));
        assert!(rendered.contains("Persist trace evidence on each native run"));
        assert!(!rendered.contains("Launch native execution mob through Meta"));
        assert_eq!(rendered.matches("### Plan").count(), 1);
        assert!(rendered.contains("Planner completed: published Fugue Workpad plan/checkoffs."));
    }

    #[test]
    fn planner_issue_specific_workpad_heading_becomes_authoritative_plan() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-392".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let planner_output = serde_json::json!({
            "result": {
                "text": "## LUC-392 Workpad\n\n### Plan\n- [ ] Preserve the issue-specific plan.\n\n### Acceptance criteria\n- [ ] Do not create a placeholder Workpad.\n"
            }
        })
        .to_string();
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "planner".to_string(),
            },
            message: Some(planner_output),
        }];

        let rendered = render_native_mob_workpad(&run, &events);

        assert!(rendered.contains("Preserve the issue-specific plan"));
        assert!(!rendered.contains("Pending planner-authored issue-specific checkbox plan"));
        assert_eq!(rendered.matches("### Plan").count(), 1);
    }

    #[test]
    fn opaque_member_ok_does_not_replace_issue_specific_workpad() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-224".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "planner".to_string(),
            },
            message: Some(r#"{"text":"ok"}"#.to_string()),
        }];
        let existing = "## Fugue Workpad\n\n### Plan\n- [ ] Add native Meta flow-run evidence.\n- [ ] Preserve scoped MCP server env.\n\n### Acceptance criteria\n- [ ] Operators can see the Meta flow ids.\n";

        let rendered = render_native_mob_workpad_with_existing(&run, &events, Some(existing));

        assert!(rendered.contains("Add native Meta flow-run evidence"));
        assert!(rendered.contains("Preserve scoped MCP server env"));
        assert!(rendered.contains("Planner completed."));
        assert!(!rendered.contains("Planner completed: ok"));
        assert!(!rendered.contains("### Plan\nok"));
        assert!(!rendered.contains("Waiting for the long-lived planner member"));
    }

    #[test]
    fn terminal_failure_preserves_issue_specific_workpad_without_literal_plan_heading() {
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-450".to_string()),
            meta_flow_run_id: Some("flow-450".to_string()),
            flow_id: Some("planner-coder-reviewer".to_string()),
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-450".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Failed,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let existing = "## Fugue Workpad\n\n### Protected behavior\n- [x] Document the OSS-safe Meta console deployment boundary.\n\n### Implementation plan\n- [x] Add or update a focused docs/static check before or alongside the docs change.\n\n### Focused checks / static validation run\n- [x] `cargo test --test docs_references meta_console_docs_raw_ip_rpc_boundary` -- passed.\n";
        let events = vec![
            MobEventSnapshot {
                sequence: 10,
                occurred_at: Utc.timestamp_opt(10, 0).unwrap(),
                kind: MobEventKind::MemberCompleted {
                    member: "reviewer".to_string(),
                },
                message: Some(
                    "Meta flow step `reviewer` failed: internal error: turn abandoned: apply failed: Terminal failure: Failed (LlmFailure): LLM failure terminal turn"
                        .to_string(),
                ),
            },
            MobEventSnapshot {
                sequence: 11,
                occurred_at: Utc.timestamp_opt(11, 0).unwrap(),
                kind: MobEventKind::RunCompleted {
                    status: MobRunStatus::Failed,
                    structured_output: None,
                },
                message: Some(
                    "Meta flow `planner-coder-reviewer` failed with run id `flow-450`: root frame sealed failed"
                        .to_string(),
                ),
            },
        ];

        let rendered = render_native_mob_workpad_with_existing(&run, &events, Some(existing));

        assert!(rendered.contains("Document the OSS-safe Meta console deployment boundary"));
        assert!(rendered.contains("meta_console_docs_raw_ip_rpc_boundary"));
        assert!(rendered.contains("Reviewer completed: Meta flow step `reviewer` failed"));
        assert!(rendered.contains("Issue mob completed with status `Failed`"));
        assert!(!rendered.contains("Pending planner-authored issue-specific checkbox plan"));
        assert!(!rendered.contains("### Plan\n- [ ] Pending"));
    }

    #[test]
    fn native_mob_workpad_keeps_meta_flow_details_out_of_issue_plan() {
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-272".to_string()),
            meta_flow_run_id: Some("be1be0f6-5d4a-482f-93c6-3db0eaac32c4".to_string()),
            flow_id: Some("planner-coder-reviewer".to_string()),
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-272".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::ToolCallCompleted {
                operation: FugueToolOperation::MobSpawnOrResumeExecution,
            },
            message: Some(
                "mob/flow_run run_id=be1be0f6-5d4a-482f-93c6-3db0eaac32c4 flow_id=planner-coder-reviewer status=pending"
                    .to_string(),
            ),
        }];

        let rendered = render_native_mob_workpad(&run, &events);

        assert!(rendered.contains("Pending planner-authored issue-specific checkbox plan."));
        assert!(!rendered.contains("mob/flow_run"));
        assert!(!rendered.contains("be1be0f6-5d4a-482f-93c6-3db0eaac32c4"));
        assert!(!rendered.contains("planner-coder-reviewer"));
        assert!(!rendered.contains("native-execution:native-execution:LUC-272"));
    }

    #[test]
    fn native_mob_workpad_filters_verbose_tool_catalog_diagnostics() {
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-325".to_string()),
            meta_flow_run_id: Some("aab957a7-a56c-440f-999d-7289c4a6402c".to_string()),
            flow_id: Some("planner-coder-reviewer".to_string()),
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-325".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let events = vec![
            MobEventSnapshot {
                sequence: 1,
                occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
                kind: MobEventKind::RunStarted,
                message: None,
            },
            MobEventSnapshot {
                sequence: 2,
                occurred_at: Utc.timestamp_opt(2, 0).unwrap(),
                kind: MobEventKind::ToolCallCompleted {
                    operation: FugueToolOperation::MobSpawnOrResumeExecution,
                },
                message: Some(
                    "native member tool/MCP launch preflight before mob/flow_run: flow=planner-coder-reviewer; state_bound_rpc=[mob/create,mob/spawn_many,mob/flow_run]; agent_side_mob_tools=[delegate,mob_create]; in_mob_operator_tools=[spawn_member,mob_run_flow]; configured_mcp_servers=[github,linear]\nreadiness_gate=mob/wait_ready suppressed by Fugue launch policy"
                        .to_string(),
                ),
            },
        ];

        let rendered = render_native_mob_workpad(&run, &events);

        assert!(rendered.contains("Issue mob started."));
        assert!(!rendered.contains("native member tool/MCP launch preflight"));
        assert!(!rendered.contains("state_bound_rpc"));
        assert!(!rendered.contains("readiness_gate"));
        assert!(!rendered.contains("configured_mcp_servers"));
    }

    #[test]
    fn native_mob_workpad_preserves_launch_failure_before_flow_run() {
        let run = MobRunSnapshot {
            run_id: MobRunId("native-execution:native-execution:LUC-283".to_string()),
            meta_flow_run_id: None,
            flow_id: Some("planner-coder-reviewer".to_string()),
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-283".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Failed,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(20, 0).unwrap()),
        };
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::RunCompleted {
                status: MobRunStatus::Failed,
                structured_output: None,
            },
            message: Some(
                "Meta RPC mob/spawn_many did not start all required members before mob/flow_run: missing successful flow members [planner,coder]"
                    .to_string(),
            ),
        }];

        let rendered = render_native_mob_workpad(&run, &events);

        assert!(rendered.contains("Issue mob completed with status `Failed`"));
        assert!(!rendered.contains("Native launch phase before `mob/flow_run`"));
        assert!(!rendered.contains("Native `mob/flow_run` run id"));
        assert!(rendered.contains("missing successful flow members [planner,coder]"));
    }

    #[test]
    fn native_workpad_preserves_unresolved_review_findings_once() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-172".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let events = vec![MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::ToolCallCompleted {
                operation: FugueToolOperation::GitHubCheckCi,
            },
            message: Some("reran Claude Code Review".to_string()),
        }];
        let existing = "### Review findings\n- [ ] Source: blocking operator Rework comment must be addressed before rerunning Claude checks.\n- [ ] Tests: add regression coverage for the Rework handoff.\n- [ ] Docs: preserve the operator handoff note.\n\n### Pull request or Handoff\n- [ ] Pending\n";

        let rendered = render_native_mob_workpad_with_existing(&run, &events, Some(existing));
        let rendered_again =
            render_native_mob_workpad_with_existing(&run, &events, Some(rendered.as_str()));

        assert!(rendered.contains("Source: blocking operator Rework comment"));
        assert!(rendered.contains("Tests: add regression coverage"));
        assert!(rendered.contains("Docs: preserve the operator handoff note"));
        assert_eq!(rendered.matches("### Review findings").count(), 1);
        assert_eq!(rendered, rendered_again);
        assert!(
            rendered.find("### Review findings").unwrap()
                < rendered.find("### Pull request or Handoff").unwrap()
        );
    }

    #[test]
    fn host_does_not_create_placeholder_workpad_before_planner_content() {
        let run_started = MobEventSnapshot {
            sequence: 1,
            occurred_at: Utc.timestamp_opt(1, 0).unwrap(),
            kind: MobEventKind::RunStarted,
            message: Some("Meta RPC native execution launch started".to_string()),
        };

        assert!(!native_mob_workpad_should_upsert(&[run_started], None));
        assert!(!native_mob_workpad_should_upsert(
            &[],
            Some(
                "## Fugue Workpad\n\n### Plan\n- [ ] Pending planner-authored issue-specific checkbox plan.\n"
            )
        ));
    }

    #[test]
    fn host_may_preserve_authoritative_workpad_content() {
        let existing = "## Fugue Workpad\n\n### Plan\n- [ ] Add focused projection smoke docs.\n";

        assert!(native_mob_workpad_should_upsert(&[], Some(existing)));
    }

    #[test]
    fn planner_update_does_not_replace_existing_pr_handoff_evidence() {
        let run = MobRunSnapshot {
            run_id: MobRunId("run-1".to_string()),
            meta_flow_run_id: None,
            flow_id: None,
            issue: Some(LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-277".to_string()),
            }),
            workspace_key: None,
            template_id: None,
            status: MobRunStatus::Running,
            started_at: None,
            updated_at: Utc.timestamp_opt(20, 0).unwrap(),
            completed_at: None,
        };
        let planner_output = serde_json::json!({
            "result": {
                "text": "## Fugue Workpad\n\n### Plan\n- [ ] Restart from planning.\n\n### Acceptance criteria\n- [ ] Keep concrete evidence.\n"
            }
        })
        .to_string();
        let events = vec![MobEventSnapshot {
            sequence: 3,
            occurred_at: Utc.timestamp_opt(3, 0).unwrap(),
            kind: MobEventKind::MemberCompleted {
                member: "planner".to_string(),
            },
            message: Some(planner_output),
        }];
        let existing = "## Fugue Workpad\n\n### Plan\n- [x] Original detailed plan.\n\n### Reviewer gate\n- [x] Reviewer decision: **green with PR handoff pending CI/merge**.\n\n### Pull request or Handoff\n- [x] Repo boundary PR handoff: https://github.com/lukacf/meerkat-fugue/pull/67 (PR #67).\n";

        let rendered = render_native_mob_workpad_with_existing(&run, &events, Some(existing));

        assert!(rendered.contains("Original detailed plan"));
        assert!(!rendered.contains("Restart from planning"));
        assert!(rendered.contains("Reviewer decision: **green"));
        assert!(rendered.contains("https://github.com/lukacf/meerkat-fugue/pull/67"));
    }
}
