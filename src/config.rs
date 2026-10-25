use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use minijinja::{Environment, UndefinedBehavior, context};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::domain::{Issue, normalize_state, sanitize_workspace_key};
use crate::error::{FugueError, Result};
use crate::kapellmeister::{
    KapellmeisterAuthorityConfig, KapellmeisterAuthorityMode, KapellmeisterSafeSelectors,
    KapellmeisterToolAuthority,
};
use crate::tools::{
    GitHubOwner, GitHubRepoName, GitHubRepoRef, LinearStateType, MergeMethod, MobTemplateId,
};

/// Default model used by the meerkat agent when a workflow does not specify one.
pub use meerkat_models::global_default_model as default_agent_model;
pub const DEFAULT_KAPELLMEISTER_SHADOW_PROMPT: &str = "You are Kapellmeister running in Meerkat Fugue shadow mode. Observe repository-scoped orchestration signals, preserve durable cursors, and produce recommendations only. The Rust orchestrator remains authoritative; do not mutate Linear, GitHub, the workspace, or production state.";
const DEFAULT_PLAYWRIGHT_MCP_SERVER_ID: &str = "playwright";
const DEFAULT_PLAYWRIGHT_MCP_COMMAND: [&str; 4] =
    ["npx", "-y", "@playwright/mcp@latest", "--headless"];
const DEFAULT_NATIVE_LAUNCH_CONTEXT_TEMPLATE: &str = "Native execution mob launch context:\n- Issue: {{ issue.identifier }} ({{ issue.title }})\n- Worktree: {{ worktree }}\n- Workspace key: {{ workspace_key }}\n- Template: {{ template_id }}\n- Backend: {{ backend }}\n- Flow: {{ flow }}\n- Members: {{ members }}\n\nAll mob members must work only inside the explicit worktree above and preserve the Fugue IMS workpad/PR handoff contract.";

#[derive(Debug, Clone)]
pub struct WorkflowDefinition {
    pub config: Map<String, Value>,
    pub prompt_template: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceConfig {
    pub workflow_path: PathBuf,
    pub workflow_dir: PathBuf,
    pub prompt_template: String,
    pub tracker: TrackerConfig,
    pub linear: LinearRuntimeConfig,
    pub polling: PollingConfig,
    pub workspace: WorkspaceConfig,
    pub hooks: HooksConfig,
    pub agent: AgentConfig,
    pub meerkat: MeerkatConfig,
    pub mob: MobExecutionConfig,
    pub worker_policy: WorkerPolicyConfig,
    pub kapellmeister: KapellmeisterConfig,
    pub final_review: FinalReviewConfig,
    pub deploy: DeployLaneConfig,
    pub independent_review: IndependentReviewConfig,
    pub server: Option<ServerConfig>,
    pub ota_update: Option<OtaUpdateConfig>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackerConfig {
    /// Host-owned storage scope; never shared through process CWD or environment.
    #[serde(skip)]
    pub attachment_root: Option<PathBuf>,
    pub kind: String,
    pub endpoint: String,
    #[serde(skip_serializing)]
    pub api_key: Option<String>,
    pub jira: Option<crate::jira::JiraConfig>,
    pub project_slug: Option<String>,
    pub intake_states: Vec<String>,
    pub in_flight_states: Vec<String>,
    pub active_states: Vec<String>,
    pub max_attachment_files: usize,
    pub max_attachment_bytes: u64,
    pub max_attachment_total_bytes: u64,
    pub terminal_states: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct LinearRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<String>,
    pub provisioning: NativeLinearProvisioningConfig,
    pub lanes: NativeLinearLanes,
    pub required_states: Vec<LinearRequiredState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinearRequiredState {
    pub name: String,
    pub state_type: LinearStateType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeLinearLanes {
    #[serde(default = "default_linear_parked_lane")]
    pub parked: String,
    #[serde(default = "default_linear_ready_lane")]
    pub ready: String,
    #[serde(
        default = "default_linear_active_lane",
        skip_serializing_if = "Option::is_none"
    )]
    pub active: Option<String>,
    #[serde(default)]
    pub review_ready: Option<String>,
    #[serde(default)]
    pub rework: Option<String>,
    #[serde(default)]
    pub do_review: Option<String>,
    #[serde(default)]
    pub deploy: Option<String>,
    #[serde(default)]
    pub blocked: Option<String>,
    #[serde(default = "default_linear_success_lane")]
    pub success: String,
    #[serde(default = "default_linear_canceled_lane")]
    pub canceled: String,
    #[serde(default = "default_linear_duplicate_lane")]
    pub duplicate: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TrackerLaneRoles {
    pub parked: String,
    pub intake: String,
    pub active: String,
    pub review_action: String,
    pub deploy_action: String,
    pub blocked: String,
    pub success: String,
    pub canceled: String,
}

impl Default for TrackerLaneRoles {
    fn default() -> Self {
        Self {
            parked: "Backlog".to_string(),
            intake: "Queue".to_string(),
            active: "Active".to_string(),
            review_action: "Review".to_string(),
            deploy_action: "Deploy".to_string(),
            blocked: "Blocked".to_string(),
            success: "Done".to_string(),
            canceled: "Canceled".to_string(),
        }
    }
}

impl Default for NativeLinearLanes {
    fn default() -> Self {
        Self {
            parked: default_linear_parked_lane(),
            ready: default_linear_ready_lane(),
            active: default_linear_active_lane(),
            review_ready: None,
            rework: None,
            do_review: None,
            deploy: None,
            blocked: None,
            success: default_linear_success_lane(),
            canceled: default_linear_canceled_lane(),
            duplicate: default_linear_duplicate_lane(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NativeLinearProvisioningMode {
    #[default]
    Validate,
    CreateMissing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeLinearProvisioningConfig {
    #[serde(default)]
    pub mode: NativeLinearProvisioningMode,
}

impl Default for NativeLinearProvisioningConfig {
    fn default() -> Self {
        Self {
            mode: NativeLinearProvisioningMode::Validate,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeLinearProjectionConfig {
    pub node_lanes: BTreeMap<String, String>,
    #[serde(default)]
    pub node_phase_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub aggregate: NativeLinearProjectionAggregate,
    pub terminal_lanes: NativeLinearProjectionTerminals,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct NativeLinearProjectionAggregate {
    #[serde(default)]
    pub precedence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeLinearProjectionTerminals {
    pub succeeded: String,
    pub failed: String,
    pub cancelled: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterializedLinearProjection {
    pub id: String,
    pub node_lanes: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_phase_labels: BTreeMap<String, String>,
    pub aggregate: NativeLinearProjectionAggregate,
    pub terminal_lanes: NativeLinearProjectionTerminals,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FinalReviewConfig {
    pub enabled: bool,
    /// Minimum distinct specialists, in addition to required profile coverage.
    pub minimum_reviewers: u8,
    pub mode: FinalReviewMode,
    pub lane: String,
    pub rework_lane: String,
    pub blocked_lane: String,
    pub success_lane: String,
    pub merge: FinalReviewMergeConfig,
    pub reviewers: Vec<FinalReviewReviewerConfig>,
}

impl Default for FinalReviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            minimum_reviewers: 3,
            mode: FinalReviewMode::Manual,
            lane: default_final_review_lane(),
            rework_lane: default_final_review_rework_lane(),
            blocked_lane: default_final_review_blocked_lane(),
            success_lane: default_linear_success_lane(),
            merge: FinalReviewMergeConfig::default(),
            reviewers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinalReviewMode {
    Manual,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FinalReviewMergeConfig {
    pub strategy: FinalReviewMergeStrategy,
    pub delete_branch: bool,
    pub require_ci_success: bool,
    pub require_fresh_head: bool,
}

impl Default for FinalReviewMergeConfig {
    fn default() -> Self {
        Self {
            strategy: FinalReviewMergeStrategy::Squash,
            delete_branch: true,
            require_ci_success: true,
            require_fresh_head: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinalReviewMergeStrategy {
    Merge,
    Squash,
    Rebase,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FinalReviewReviewerConfig {
    pub id: String,
    pub profile: String,
    pub required: bool,
}

impl Default for FinalReviewReviewerConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            profile: String::new(),
            required: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct DeployLaneConfig {
    pub enabled: bool,
    pub lane: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_lane: Option<String>,
    pub procedure: String,
    pub success_lane: String,
    pub ready_lane: String,
    pub rework_lane: String,
    pub blocked_lane: String,
    pub allowed_repositories: Vec<GitHubRepoRef>,
    pub base_branch: String,
    pub merge_method: MergeMethod,
    pub require_ci_success: bool,
    /// Permit an explicitly empty host check set, while retaining all other CI gates.
    pub allow_no_ci_checks: bool,
    pub require_fresh_head: bool,
    pub require_no_blocking_workgraph: bool,
    pub require_ota_update: bool,
}

impl Default for DeployLaneConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lane: default_deploy_lane(),
            action_lane: None,
            procedure: default_deploy_procedure(),
            success_lane: default_linear_success_lane(),
            ready_lane: default_linear_review_ready_lane(),
            rework_lane: default_linear_rework_lane(),
            blocked_lane: default_final_review_blocked_lane(),
            allowed_repositories: Vec::new(),
            base_branch: "main".to_string(),
            merge_method: MergeMethod::Squash,
            require_ci_success: true,
            allow_no_ci_checks: false,
            require_fresh_head: true,
            require_no_blocking_workgraph: true,
            require_ota_update: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct IndependentReviewConfig {
    pub enabled: bool,
    pub lane: String,
    pub success_lane: String,
    pub rework_lane: String,
    pub blocked_lane: String,
    pub orchestrator_profile: String,
    pub reviewer_count: u8,
    pub timeout_ms: u64,
    pub findings_comment_marker: String,
    pub require_all_green: bool,
}

impl Default for IndependentReviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lane: default_do_review_lane(),
            success_lane: default_linear_review_ready_lane(),
            rework_lane: default_linear_rework_lane(),
            blocked_lane: default_final_review_blocked_lane(),
            orchestrator_profile: default_independent_review_orchestrator_profile(),
            reviewer_count: 3,
            timeout_ms: 900_000,
            findings_comment_marker: default_independent_review_findings_comment_marker(),
            require_all_green: true,
        }
    }
}

fn default_final_review_lane() -> String {
    "Final Review & Merge".to_string()
}
fn default_final_review_rework_lane() -> String {
    "Active".to_string()
}
fn default_final_review_blocked_lane() -> String {
    "Blocked".to_string()
}
fn default_deploy_lane() -> String {
    "Deploy".to_string()
}
fn default_deploy_procedure() -> String {
    ".fugue/deploy.md".to_string()
}
fn default_do_review_lane() -> String {
    "Review".to_string()
}
fn default_independent_review_orchestrator_profile() -> String {
    "review_orchestrator".to_string()
}
fn default_linear_rework_lane() -> String {
    "Rework".to_string()
}
fn default_linear_review_ready_lane() -> String {
    "Ready for review".to_string()
}
fn default_independent_review_findings_comment_marker() -> String {
    "Fugue independent review findings".to_string()
}

impl TrackerConfig {
    /// Stable tracker identity, excluding credentials and display settings.
    pub(crate) fn context_id(&self) -> String {
        // Jira also accepts environment references for its site and project.
        // Scope the effective values so changing a deployment's environment
        // cannot reuse another tracker's browser receipts or local files.
        let jira_endpoint = (self.kind == "jira")
            .then(|| crate::jira::env_value(&self.endpoint))
            .flatten();
        let jira_project = (self.kind == "jira")
            .then(|| {
                self.project_slug
                    .as_deref()
                    .and_then(crate::jira::env_value)
            })
            .flatten();
        let mut identity = Sha256::new();
        for component in [
            self.kind.as_str(),
            jira_endpoint.as_deref().unwrap_or(&self.endpoint),
            jira_project
                .as_deref()
                .or(self.project_slug.as_deref())
                .unwrap_or_default()
                .trim(),
        ] {
            identity.update((component.len() as u64).to_le_bytes());
            identity.update(component.as_bytes());
        }
        format!("{:x}", identity.finalize())
    }

    pub fn from_mcp_env(default_kind: &str) -> Result<Self> {
        let kind = env::var("FUGUE_TRACKER_KIND").unwrap_or_else(|_| default_kind.to_string());
        let mut raw = Map::new();
        raw.insert("kind".to_string(), json!(kind));
        let (endpoint_key, project_key) = match kind.as_str() {
            "jira" => ("JIRA_BASE_URL", "JIRA_PROJECT_KEY"),
            "fugue-ims" => ("FUGUE_IMS_ENDPOINT", "FUGUE_IMS_PROJECT"),
            _ => ("LINEAR_ENDPOINT", "LINEAR_PROJECT_SLUG"),
        };
        for (field, variable) in [("endpoint", endpoint_key), ("project_slug", project_key)] {
            if let Ok(value) = env::var(variable) {
                raw.insert(field.to_string(), json!(value));
            }
        }
        for (field, variable) in [
            ("intake_states", "FUGUE_LINEAR_INTAKE_STATES"),
            ("in_flight_states", "FUGUE_LINEAR_IN_FLIGHT_STATES"),
            ("active_states", "FUGUE_LINEAR_ACTIVE_STATES"),
            ("terminal_states", "FUGUE_LINEAR_TERMINAL_STATES"),
        ] {
            let states: Vec<_> = env::var(variable)
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            raw.insert(field.to_string(), json!(states));
        }
        if kind == "jira" {
            let mut jira = Map::new();
            for (field, variable) in [
                ("auth", "JIRA_AUTH_MODE"),
                ("api_version", "JIRA_API_VERSION"),
                ("jql", "JIRA_JQL"),
                ("issue_type", "JIRA_ISSUE_TYPE"),
                ("create_fields_file", "JIRA_CREATE_FIELDS_FILE"),
                ("transition_fields_file", "JIRA_TRANSITION_FIELDS_FILE"),
            ] {
                if let Ok(value) = env::var(variable) {
                    jira.insert(field.to_string(), json!(value));
                }
            }
            raw.insert("jira".to_string(), Value::Object(jira));
        }
        Self::from_raw(Some(&raw))
    }

    pub fn is_intake_state(&self, state: &str) -> bool {
        let normalized = normalize_state(state);
        self.intake_states
            .iter()
            .any(|candidate| normalize_state(candidate) == normalized)
    }

    pub fn is_active_state(&self, state: &str) -> bool {
        let normalized = normalize_state(state);
        self.active_states
            .iter()
            .any(|candidate| normalize_state(candidate) == normalized)
    }

    pub fn is_terminal_state(&self, state: &str) -> bool {
        let normalized = normalize_state(state);
        self.terminal_states
            .iter()
            .any(|candidate| normalize_state(candidate) == normalized)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PollingConfig {
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceConfig {
    pub root: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct HooksConfig {
    pub after_create: Option<String>,
    pub before_run: Option<String>,
    pub after_run: Option<String>,
    pub before_remove: Option<String>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentConfig {
    pub max_concurrent_agents: usize,
    pub max_turns: usize,
    pub max_retry_backoff_ms: u64,
    pub max_concurrent_agents_by_state: BTreeMap<String, usize>,
}

pub use meerkat_core::AuthBindingRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillKey {
    pub source_uuid: String,
    pub skill_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum SkillRef {
    #[serde(rename = "structured")]
    Structured {
        source_uuid: String,
        skill_name: String,
    },
}

/// The repository mob owns both durable domain specialists and ticket members.
/// Legacy queue-only workflows have no repository binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RepositoryRuntimeBinding {
    pub repository: crate::ensemble::config::RepoId,
    pub state_path: PathBuf,
}

impl RepositoryRuntimeBinding {
    pub fn mob_id(&self) -> String {
        format!("ensemble-{}", self.repository)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MeerkatConfig {
    #[serde(skip)]
    pub runtime_connections: Option<std::sync::Arc<crate::setup::connections::Connections>>,
    pub repository_runtime: Option<RepositoryRuntimeBinding>,
    pub command: String,
    pub realm: Option<String>,
    pub instance: Option<String>,
    pub realm_backend: Option<String>,
    pub state_root: Option<PathBuf>,
    pub context_root: Option<PathBuf>,
    pub user_config_root: Option<PathBuf>,
    pub auth_binding: Option<AuthBindingRef>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub max_tokens: Option<u64>,
    pub system_prompt: Option<String>,
    pub output_schema: Option<Map<String, Value>>,
    pub structured_output_retries: Option<u64>,
    pub hooks_override: Option<Map<String, Value>>,
    pub provider_params: Option<Map<String, Value>>,
    pub enable_shell: Option<bool>,
    pub enable_builtins: Option<bool>,
    pub enable_mob: Option<bool>,
    pub enable_memory: Option<bool>,
    pub keep_alive: bool,
    pub max_sessions: usize,
    pub comms_name: Option<String>,
    pub peer_meta: Option<Map<String, Value>>,
    pub budget_limits: Option<Map<String, Value>>,
    pub preload_skills: Option<Vec<SkillKey>>,
    pub skill_refs: Option<Vec<SkillRef>>,
    pub labels: Option<BTreeMap<String, String>>,
    pub additional_instructions: Option<Vec<String>>,
    pub app_context: Option<Map<String, Value>>,
    pub shell_env: Option<BTreeMap<String, String>>,
    pub flow_tool_overlay: Option<Map<String, Value>>,
    pub first_turn_activation_prompt: String,
    pub turn_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub stall_timeout_ms: i64,
}

impl MeerkatConfig {
    pub fn mob_id(&self) -> std::borrow::Cow<'_, str> {
        match &self.repository_runtime {
            Some(binding) => std::borrow::Cow::Owned(binding.mob_id()),
            None => std::borrow::Cow::Borrowed(crate::native_mob_contract::FUGUE_MOB_ID),
        }
    }

    pub fn command_line(&self) -> String {
        let mut command = self.command.clone();
        append_flag(&mut command, "--realm", self.realm.as_deref());
        append_flag(&mut command, "--instance", self.instance.as_deref());
        append_flag(
            &mut command,
            "--realm-backend",
            self.realm_backend.as_deref(),
        );
        append_flag_path(&mut command, "--state-root", self.state_root.as_deref());
        append_flag_path(&mut command, "--context-root", self.context_root.as_deref());
        append_flag_path(
            &mut command,
            "--user-config-root",
            self.user_config_root.as_deref(),
        );
        command
    }

    pub fn create_session_params(
        &self,
        prompt: String,
        external_tools: Option<Vec<Value>>,
    ) -> Value {
        let mut params = Map::new();
        params.insert("prompt".to_string(), Value::String(prompt));
        params.insert(
            "initial_turn".to_string(),
            Value::String("deferred".to_string()),
        );
        insert_opt_string(&mut params, "model", self.model.clone());
        insert_opt_string(&mut params, "provider", self.provider.clone());
        insert_opt_u64(&mut params, "max_tokens", self.max_tokens);
        insert_opt_string(&mut params, "system_prompt", self.system_prompt.clone());
        insert_opt_object(&mut params, "output_schema", self.output_schema.clone());
        insert_opt_u64(
            &mut params,
            "structured_output_retries",
            self.structured_output_retries,
        );
        insert_opt_object(&mut params, "hooks_override", self.hooks_override.clone());
        insert_opt_object(&mut params, "provider_params", self.provider_params.clone());
        insert_opt_bool(&mut params, "enable_shell", self.enable_shell);
        insert_opt_bool(&mut params, "enable_builtins", self.enable_builtins);
        insert_opt_bool(&mut params, "enable_mob", self.enable_mob);
        insert_opt_bool(&mut params, "enable_memory", self.enable_memory);
        params.insert("keep_alive".to_string(), Value::Bool(self.keep_alive));
        insert_opt_string(&mut params, "comms_name", self.comms_name.clone());
        insert_opt_object(&mut params, "peer_meta", self.peer_meta.clone());
        insert_opt_object(&mut params, "budget_limits", self.budget_limits.clone());
        if let Some(auth_binding) = &self.auth_binding {
            params.insert("auth_binding".to_string(), json!(auth_binding));
        }
        if let Some(preload_skills) = &self.preload_skills {
            params.insert("preload_skills".to_string(), json!(preload_skills));
        }
        if let Some(skill_refs) = &self.skill_refs {
            params.insert("skill_refs".to_string(), json!(skill_refs));
        }
        if let Some(labels) = &self.labels {
            params.insert("labels".to_string(), json!(labels));
        }
        if let Some(additional_instructions) = &self.additional_instructions {
            params.insert(
                "additional_instructions".to_string(),
                json!(additional_instructions),
            );
        }
        insert_opt_object(&mut params, "app_context", self.app_context.clone());
        if let Some(shell_env) = &self.shell_env {
            params.insert("shell_env".to_string(), json!(shell_env));
        }
        if let Some(external_tools) = external_tools {
            params.insert("external_tools".to_string(), Value::Array(external_tools));
        }
        Value::Object(params)
    }

    pub fn turn_start_params(
        &self,
        session_id: String,
        prompt: String,
        pending: bool,
        materialized: bool,
    ) -> Value {
        let mut params = Map::new();
        params.insert("session_id".to_string(), Value::String(session_id));
        params.insert("prompt".to_string(), Value::String(prompt));
        if let Some(skill_refs) = &self.skill_refs {
            params.insert("skill_refs".to_string(), json!(skill_refs));
        }
        insert_opt_object(
            &mut params,
            "flow_tool_overlay",
            self.flow_tool_overlay.clone(),
        );
        if let Some(additional_instructions) = &self.additional_instructions {
            params.insert(
                "additional_instructions".to_string(),
                json!(additional_instructions),
            );
        }
        params.insert("keep_alive".to_string(), Value::Bool(self.keep_alive));
        insert_opt_string(&mut params, "model", self.model.clone());
        if self.model.is_some() {
            insert_opt_string(&mut params, "provider", self.provider.clone());
        }
        if let Some(auth_binding) = &self.auth_binding {
            params.insert("auth_binding".to_string(), json!(auth_binding));
        }
        if pending {
            insert_opt_u64(&mut params, "max_tokens", self.max_tokens);
            insert_opt_string(&mut params, "system_prompt", self.system_prompt.clone());
            insert_opt_object(&mut params, "output_schema", self.output_schema.clone());
            insert_opt_u64(
                &mut params,
                "structured_output_retries",
                self.structured_output_retries,
            );
            insert_opt_object(&mut params, "provider_params", self.provider_params.clone());
        } else if !materialized {
            insert_opt_object(&mut params, "provider_params", self.provider_params.clone());
        }
        Value::Object(params)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct KapellmeisterConfig {
    pub shadow_mode: KapellmeisterShadowModeConfig,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MobExecutionConfig {
    pub backend: MobExecutionBackend,
    pub native: NativeMobRuntimeConfig,
    pub templates: BTreeMap<String, MaterializedMobTemplate>,
    pub mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
    pub selection: NativeSelectionConfig,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerPolicyConfig {
    pub native_launch_context_template: Option<String>,
}

impl Default for MobExecutionConfig {
    fn default() -> Self {
        Self {
            backend: MobExecutionBackend::NativeExecution,
            native: NativeMobRuntimeConfig::native_default(),
            templates: BTreeMap::new(),
            mcp_servers: BTreeMap::new(),
            selection: NativeSelectionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum MobExecutionBackend {
    #[default]
    NativeExecution,
}

impl MobExecutionBackend {
    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::NativeExecution => "native-execution",
        }
    }

    pub const fn is_native(self) -> bool {
        matches!(self, Self::NativeExecution)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeMobRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub shadow: bool,
}

impl NativeMobRuntimeConfig {
    pub const fn native_default() -> Self {
        Self {
            enabled: true,
            shadow: false,
        }
    }
}

impl Default for NativeMobRuntimeConfig {
    fn default() -> Self {
        Self::native_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterializedMobTemplate {
    pub backend: MobExecutionBackend,
    pub members: Vec<MaterializedMobMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<NativeMobFlow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linear_projection: Option<MaterializedLinearProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_review_cycles: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterializedMobMember {
    pub name: String,
    pub profile: String,
    pub agent: NativeAgentProfile,
}

fn default_true() -> bool {
    true
}

fn default_linear_parked_lane() -> String {
    "Backlog".to_string()
}

fn default_linear_ready_lane() -> String {
    "Todo".to_string()
}

fn default_linear_active_lane() -> Option<String> {
    Some("Active".to_string())
}

fn default_linear_success_lane() -> String {
    "Done".to_string()
}

fn default_linear_canceled_lane() -> String {
    "Canceled".to_string()
}

fn default_linear_duplicate_lane() -> String {
    "Duplicate".to_string()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeMobFlow {
    Solo,
    PlannerCoder,
    PlannerCoderReviewer,
    PlannerCoderReviewerRework,
}

impl NativeMobFlow {
    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::Solo => "solo",
            Self::PlannerCoder => "planner-coder",
            Self::PlannerCoderReviewer => "planner-coder-reviewer",
            Self::PlannerCoderReviewerRework => "planner-coder-reviewer-rework",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KapellmeisterShadowModeConfig {
    pub enabled: bool,
    pub state_path: PathBuf,
    pub prompt: String,
    pub authority: KapellmeisterToolAuthority,
}

impl Default for KapellmeisterShadowModeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            state_path: PathBuf::from(".fugue/kapellmeister-shadow.json"),
            prompt: DEFAULT_KAPELLMEISTER_SHADOW_PROMPT.to_string(),
            authority: KapellmeisterToolAuthority::default(),
        }
    }
}

impl KapellmeisterShadowModeConfig {
    pub fn is_inert(&self) -> bool {
        !self.enabled
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub meta_console: MetaConsoleServerConfig,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MetaConsoleServerConfig {
    pub enabled: bool,
    pub path: String,
    pub asset_dir: Option<PathBuf>,
    pub identity_provider: Option<String>,
    pub rpc_timeout_ms: u64,
    pub basic_auth: Option<MetaConsoleBasicAuthConfig>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MetaConsoleBasicAuthConfig {
    pub username_env: String,
    pub password_env: String,
    pub realm: String,
}

impl Default for MetaConsoleServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: "/console".to_string(),
            asset_dir: None,
            identity_provider: None,
            rpc_timeout_ms: 30_000,
            basic_auth: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OtaUpdateConfig {
    pub enabled: bool,
    pub repo_path: PathBuf,
    pub branch: String,
    pub remote: String,
    pub build_command: String,
    pub health_check_command: Option<String>,
    pub deploy_command: String,
    pub timeout_ms: u64,
    pub require_clean: bool,
    pub lock_path: Option<PathBuf>,
    #[serde(skip_serializing)]
    pub trigger_token_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFugueConfig {
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub mob: Option<NativeMobExecutionSection>,
    #[serde(default)]
    pub tracker_lanes: Option<TrackerLaneRoles>,
    #[serde(alias = "tracker")]
    pub linear: NativeLinearConfig,
    pub selection: NativeSelectionConfig,
    #[serde(default)]
    pub agents: BTreeMap<String, NativeAgentProfile>,
    #[serde(default)]
    pub mcp_servers: BTreeMap<String, NativeMcpServerConfig>,
    #[serde(default)]
    pub linear_projections: BTreeMap<String, NativeLinearProjectionConfig>,
    #[serde(default)]
    pub mob_templates: BTreeMap<String, NativeMobTemplate>,
    pub worker: NativeWorkerConfig,
    #[serde(default)]
    pub polling: Option<NativePollingConfig>,
    #[serde(default)]
    pub workspace: Option<NativeWorkspaceConfig>,
    #[serde(default)]
    pub hooks: Option<NativeHooksConfig>,
    #[serde(default, rename = "agent")]
    pub runtime_agent: Option<NativeRuntimeAgentConfig>,
    #[serde(default)]
    pub meerkat: Option<NativeMeerkatConfig>,
    #[serde(default)]
    pub kapellmeister: Option<NativeKapellmeisterConfig>,
    #[serde(default)]
    pub final_review: Option<FinalReviewConfig>,
    #[serde(default)]
    pub deploy: Option<DeployLaneConfig>,
    #[serde(default)]
    pub independent_review: Option<IndependentReviewConfig>,
    #[serde(default)]
    pub server: Option<NativeServerConfig>,
    #[serde(default)]
    pub ota_update: Option<NativeOtaUpdateConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLinearConfig {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub jira: Option<crate::jira::JiraConfig>,
    #[serde(default)]
    pub project_slug: Option<String>,
    #[serde(default)]
    pub team: Option<String>,
    #[serde(default)]
    pub provisioning: NativeLinearProvisioningConfig,
    #[serde(default)]
    pub lanes: NativeLinearLanes,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct NativeSelectionConfig {
    pub default_template: String,
    #[serde(default)]
    pub by_label: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeAgentProfile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_binding: Option<AuthBindingRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output_retries: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_params: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_instructions: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeMcpServerConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMobTemplate {
    #[serde(default)]
    pub backend: Option<String>,
    pub members: Vec<NativeMobMember>,
    #[serde(default)]
    pub flow: Option<String>,
    #[serde(default)]
    pub linear_projection: Option<String>,
    #[serde(default)]
    pub max_review_cycles: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMobExecutionSection {
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub native: Option<NativeMobRuntimeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NativeMobMember {
    Profile(String),
    Detailed { name: String, profile: String },
}

impl NativeMobMember {
    fn profile_name(&self) -> &str {
        match self {
            Self::Profile(profile) => profile,
            Self::Detailed { profile, .. } => profile,
        }
    }

    fn member_name(&self) -> &str {
        match self {
            Self::Profile(profile) => profile,
            Self::Detailed { name, .. } => name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkerConfig {
    #[serde(default)]
    pub prompt_template: Option<String>,
    #[serde(default)]
    pub prompt_file: Option<PathBuf>,
    #[serde(default)]
    pub native_launch_context_template: Option<String>,
    #[serde(default)]
    pub native_launch_context_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePollingConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkspaceConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHooksConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_create: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_remove: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRuntimeAgentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_agents: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retry_backoff_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_agents_by_state: Option<BTreeMap<String, usize>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMeerkatConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_runtime: Option<RepositoryRuntimeBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_config_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_binding: Option<AuthBindingRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output_retries: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks_override: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_params: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_shell: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_builtins: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_mob: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_memory: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_alive: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sessions: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comms_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_meta: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_limits: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preload_skills: Option<Vec<SkillKey>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_refs: Option<Vec<SkillRef>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_instructions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_context: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_env: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_tool_overlay: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_turn_activation_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall_timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeKapellmeisterConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_mode: Option<NativeKapellmeisterShadowModeConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_mode: Option<KapellmeisterAuthorityMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_selectors: Option<KapellmeisterSafeSelectors>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeKapellmeisterShadowModeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeServerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_console: Option<NativeMetaConsoleServerConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMetaConsoleServerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_dir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic_auth: Option<NativeMetaConsoleBasicAuthConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMetaConsoleBasicAuthConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOtaUpdateConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deploy_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_clean: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_token_env: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowConfigManager {
    workflow_path: PathBuf,
    last_modified: Option<SystemTime>,
    definition: WorkflowDefinition,
    effective: ServiceConfig,
    last_reload_error: Option<String>,
    tracker_context: String,
    connections: Option<std::sync::Arc<crate::setup::connections::Connections>>,
}

impl WorkflowConfigManager {
    #[cfg(test)]
    pub(crate) fn from_definition_for_test(
        path: PathBuf,
        definition: WorkflowDefinition,
    ) -> Result<Self> {
        let effective = ServiceConfig::from_definition(path.clone(), &definition)?;
        Ok(Self {
            workflow_path: path,
            last_modified: None,
            definition,
            tracker_context: effective.tracker_context_id(),
            effective,
            last_reload_error: None,
            connections: None,
        })
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let path = absolute_clean_path(
            &env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            &path,
        );
        let (definition, modified) = load_workflow_file(&path)?;
        let effective = ServiceConfig::from_definition(path.clone(), &definition)?;
        Ok(Self {
            workflow_path: path,
            last_modified: modified,
            definition,
            tracker_context: effective.tracker_context_id(),
            effective,
            last_reload_error: None,
            connections: None,
        })
    }

    pub fn load_scoped(
        path: PathBuf,
        connections: std::sync::Arc<crate::setup::connections::Connections>,
    ) -> Result<Self> {
        let (mut definition, modified) = load_workflow_file(&path)?;
        resolve_connection_variables(&mut definition.config, &connections);
        let mut effective = ServiceConfig::from_definition(path.clone(), &definition)?;
        effective.meerkat.runtime_connections = Some(connections.clone());
        if let Some(binding) = connections
            .model_binding(&crate::meta_runtime::meta_runtime_state_path(
                &effective.meerkat,
            ))
            .map_err(|error| FugueError::InvalidConfig(error.to_string()))?
        {
            effective.meerkat.auth_binding = Some(binding);
        }
        Ok(Self {
            workflow_path: path,
            last_modified: modified,
            definition,
            tracker_context: effective.tracker_context_id(),
            effective,
            last_reload_error: None,
            connections: Some(connections),
        })
    }

    pub fn effective(&self) -> &ServiceConfig {
        &self.effective
    }

    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }

    pub fn last_reload_error(&self) -> Option<&str> {
        self.last_reload_error.as_deref()
    }

    pub fn validate_dispatch(&self) -> Result<()> {
        self.effective.validate_dispatch()
    }

    pub fn reload_if_changed(&mut self) -> bool {
        let modified = fs::metadata(&self.workflow_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        if modified.is_some() && modified == self.last_modified {
            return false;
        }

        match load_workflow_file(&self.workflow_path).and_then(|(mut definition, modified)| {
            if let Some(connections) = &self.connections {
                resolve_connection_variables(&mut definition.config, connections);
            }
            let mut effective =
                ServiceConfig::from_definition(self.workflow_path.clone(), &definition)?;
            if effective.tracker_context_id() != self.tracker_context {
                return Err(FugueError::InvalidConfig(
                    "The running repository cannot change tracker identity. Restore its original tracker settings, or use a new workspace for the new tracker.".into(),
                ));
            }
            effective.meerkat.runtime_connections = self.connections.clone();
            if let Some(connections) = &self.connections
                && let Some(binding) = connections
                    .model_binding(&crate::meta_runtime::meta_runtime_state_path(
                        &effective.meerkat,
                    ))
                    .map_err(|error| FugueError::InvalidConfig(error.to_string()))?
            {
                effective.meerkat.auth_binding = Some(binding);
            }
            Ok((definition, modified, effective))
        }) {
            Ok((definition, modified, effective)) => {
                self.definition = definition;
                self.effective = effective;
                self.last_modified = modified;
                self.last_reload_error = None;
                true
            }
            Err(err) => {
                self.last_reload_error = Some(err.to_string());
                false
            }
        }
    }

    pub fn render_prompt(&self, issue: &Issue, attempt: Option<u32>) -> Result<String> {
        render_prompt(&self.definition.prompt_template, issue, attempt)
    }
}

fn resolve_connection_variables(
    config: &mut Map<String, Value>,
    connections: &crate::setup::connections::Connections,
) {
    fn visit(value: &mut Value, env: &BTreeMap<&str, String>) {
        match value {
            Value::String(text) => {
                if let Some(variable) = text.strip_prefix('$') {
                    if let Some(value) = env.get(variable) {
                        *text = value.clone();
                    } else if crate::setup::connections::SERVICE_CREDENTIAL_ENV.contains(&variable)
                    {
                        text.clear();
                    }
                }
            }
            Value::Array(values) => values.iter_mut().for_each(|value| visit(value, env)),
            Value::Object(values) => values.values_mut().for_each(|value| visit(value, env)),
            _ => {}
        }
    }
    let env = connections.environment();
    for value in config.values_mut() {
        visit(value, &env);
    }
}

impl ServiceConfig {
    /// A tracker with one shared unstarted status has no separate parked lane.
    /// Explicit intake scope takes precedence so that status remains runnable.
    pub fn is_parked_state(&self, state: &str) -> bool {
        normalize_state(state) == normalize_state(&self.linear.lanes.parked)
            && !self.tracker.is_intake_state(state)
    }

    /// Workflows with few statuses can use one holding status for review.
    /// The status requests attention; only native review evidence grants approval.
    pub(crate) fn independent_review_handoff_lane(&self) -> Option<&str> {
        let lane = self.linear.lanes.review_ready.as_deref()?;
        (self.independent_review.enabled
            && self.linear.lanes.do_review.is_none()
            && normalize_state(lane) == normalize_state(&self.independent_review.lane))
        .then_some(lane)
    }

    pub fn from_definition(path: PathBuf, definition: &WorkflowDefinition) -> Result<Self> {
        let workflow_dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let tracker = TrackerConfig::from_raw(section_object(&definition.config, "tracker")?)?;
        let linear =
            LinearRuntimeConfig::from_raw(section_object(&definition.config, "linear_runtime")?)?;
        let polling = PollingConfig::from_raw(section_object(&definition.config, "polling")?)?;
        let workspace = WorkspaceConfig::from_raw(
            section_object(&definition.config, "workspace")?,
            &workflow_dir,
        )?;
        let hooks = HooksConfig::from_raw(section_object(&definition.config, "hooks")?)?;
        let agent = AgentConfig::from_raw(section_object(&definition.config, "agent")?)?;
        let meerkat = MeerkatConfig::from_raw(
            section_object(&definition.config, "meerkat")?,
            &workflow_dir,
        )?;
        let mut mob = MobExecutionConfig::from_raw(section_object(&definition.config, "mob")?)?;
        for server in mob.mcp_servers.values_mut() {
            server.env.insert(
                crate::workgraph_gate::WORKGRAPH_MOB_ID_ENV.into(),
                meerkat.mob_id().into_owned(),
            );
        }
        let mut worker_policy =
            WorkerPolicyConfig::from_raw(section_object(&definition.config, "worker_policy")?)?;
        if worker_policy.native_launch_context_template.is_none() {
            worker_policy.native_launch_context_template =
                Some(DEFAULT_NATIVE_LAUNCH_CONTEXT_TEMPLATE.to_string());
        }
        let kapellmeister = KapellmeisterConfig::from_raw(
            section_object(&definition.config, "kapellmeister")?,
            &workflow_dir,
        )?;
        let final_review =
            FinalReviewConfig::from_raw(section_object(&definition.config, "final_review")?)?;
        let deploy = DeployLaneConfig::from_raw(section_object(&definition.config, "deploy")?)?;
        let independent_review = IndependentReviewConfig::from_raw(section_object(
            &definition.config,
            "independent_review",
        )?)?;
        let server = ServerConfig::from_raw(section_object(&definition.config, "server")?)?;
        let ota_update = OtaUpdateConfig::from_raw(
            section_object(&definition.config, "ota_update")?,
            &workflow_dir,
        )?;
        let mut config = Self {
            workflow_path: path,
            workflow_dir,
            prompt_template: definition.prompt_template.clone(),
            tracker,
            linear,
            polling,
            workspace,
            hooks,
            agent,
            meerkat,
            mob,
            worker_policy,
            kapellmeister,
            final_review,
            deploy,
            independent_review,
            server,
            ota_update,
        };
        config.tracker.attachment_root = Some(config.attachment_storage_root());
        config.validate_static()?;
        Ok(config)
    }

    pub(crate) fn attachment_storage_root(&self) -> PathBuf {
        self.meerkat
            .repository_runtime
            .as_ref()
            .map(|binding| {
                binding
                    .state_path
                    .join("files")
                    .join("tracker-files")
                    .join(self.tracker_context_id())
            })
            .unwrap_or_else(|| {
                // Explicit workflows may share a directory or database. Keep
                // their files scoped even when tracker keys overlap.
                self.workflow_dir
                    .join(".fugue")
                    .join("tracker-files")
                    .join(self.tracker_context_id())
            })
    }

    /// Stable tracker identity, excluding credentials and display settings.
    pub(crate) fn tracker_context_id(&self) -> String {
        self.tracker.context_id()
    }

    fn validate_static(&self) -> Result<()> {
        if self.polling.interval_ms == 0 {
            return Err(FugueError::InvalidConfig(
                "polling.interval_ms must be positive".to_string(),
            ));
        }
        if self.hooks.timeout_ms == 0 {
            return Err(FugueError::InvalidConfig(
                "hooks.timeout_ms must be positive".to_string(),
            ));
        }
        if self.agent.max_turns == 0 {
            return Err(FugueError::InvalidConfig(
                "agent.max_turns must be positive".to_string(),
            ));
        }
        if self.agent.max_concurrent_agents == 0 {
            return Err(FugueError::InvalidConfig(
                "agent.max_concurrent_agents must be positive".to_string(),
            ));
        }
        self.final_review.validate_static()?;
        self.deploy.validate_static()?;
        self.independent_review.validate_static()?;
        if self.meerkat.keep_alive
            && self
                .meerkat
                .comms_name
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(FugueError::InvalidConfig(
                "meerkat.keep_alive requires meerkat.comms_name".to_string(),
            ));
        }
        if self.meerkat.first_turn_activation_prompt.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "meerkat.first_turn_activation_prompt must be non-empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_dispatch(&self) -> Result<()> {
        if !matches!(self.tracker.kind.as_str(), "linear" | "fugue-ims" | "jira") {
            return Err(FugueError::UnsupportedTrackerKind(
                self.tracker.kind.clone(),
            ));
        }
        if (self.independent_review.enabled || self.final_review.enabled)
            && self.deploy.allowed_repositories.is_empty()
        {
            return Err(FugueError::InvalidConfig(
                "Review requires deploy.allowed_repositories to scope the candidate; deploy.enabled may remain false. Set deploy.base_branch to the branch being reviewed."
                    .into(),
            ));
        }
        if self.tracker.kind == "fugue-ims" {
            if self
                .tracker
                .project_slug
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(FugueError::MissingTrackerProjectSlug);
            }
            if self.tracker.endpoint.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "tracker.endpoint must point at a fugue-ims sqlite database".to_string(),
                ));
            }
            self.mob.validate_before_dispatch()?;
            return Ok(());
        }
        if self.tracker.kind == "jira" {
            crate::jira::JiraClient::from_config(&self.tracker)?;
        }
        if self.tracker.api_key.as_deref().is_none_or(str::is_empty) {
            return Err(FugueError::MissingTrackerApiKey);
        }
        if self
            .tracker
            .project_slug
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(FugueError::MissingTrackerProjectSlug);
        }
        if self.meerkat.command.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "meerkat.command must be non-empty".to_string(),
            ));
        }
        self.mob.validate_before_dispatch()?;
        self.validate_static()
    }

    pub fn select_template_for_issue(&self, issue: &Issue) -> Result<SelectedMobTemplate<'_>> {
        let template_name = select_template_name_for_labels(&self.mob.selection, &issue.labels);
        let Some(template) = self.mob.templates.get(template_name) else {
            return Err(FugueError::InvalidConfig(format!(
                "selection references missing materialized mob template {template_name:?}"
            )));
        };
        Ok(SelectedMobTemplate {
            template_id: MobTemplateId(template_name.to_string()),
            template,
        })
    }

    pub fn render_native_execution_instructions(
        &self,
        issue: &Issue,
        attempt: Option<u32>,
        worktree_path: &Path,
        template_id: &MobTemplateId,
        template: &MaterializedMobTemplate,
    ) -> Result<String> {
        let prompt = render_prompt(&self.prompt_template, issue, attempt)?;
        let native_context_template = self
            .worker_policy
            .native_launch_context_template
            .as_deref()
            .unwrap_or(DEFAULT_NATIVE_LAUNCH_CONTEXT_TEMPLATE);
        let native_context = render_native_launch_context(
            native_context_template,
            issue,
            worktree_path,
            template_id,
            template,
        )?;
        let attachment_context = render_issue_attachment_context(issue);
        Ok(match attachment_context {
            Some(attachment_context) => {
                format!("{prompt}\n\n{native_context}\n\n{attachment_context}")
            }
            None => format!("{prompt}\n\n{native_context}"),
        })
    }
}

fn render_issue_attachment_context(issue: &Issue) -> Option<String> {
    if issue.attachments.is_empty() {
        return None;
    }
    let mut context = String::from("Issue attachments downloaded for agents:\n");
    for attachment in &issue.attachments {
        let title = attachment.title.as_deref().unwrap_or("untitled attachment");
        let path = attachment
            .local_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<not downloaded>".to_string());
        if let Some(id) = attachment.id.as_deref() {
            context.push_str(&format!("- {title} (`{id}`): {path}\n"));
        } else {
            context.push_str(&format!("- {title}: {path}\n"));
        }
    }
    Some(context.trim_end().to_string())
}

pub struct SelectedMobTemplate<'a> {
    pub template_id: MobTemplateId,
    pub template: &'a MaterializedMobTemplate,
}

fn select_template_name_for_labels<'a>(
    selection: &'a NativeSelectionConfig,
    labels: &[String],
) -> &'a str {
    selection
        .by_label
        .iter()
        .find_map(|(label, template)| {
            labels
                .iter()
                .any(|issue_label| issue_label.eq_ignore_ascii_case(label))
                .then_some(template.as_str())
        })
        .unwrap_or(selection.default_template.as_str())
}

fn format_template_members(template: &MaterializedMobTemplate) -> String {
    struct Members<'a>(&'a MaterializedMobTemplate);

    impl std::fmt::Display for Members<'_> {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for (index, member) in self.0.members.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{} ({})", member.name, member.profile)?;
            }
            Ok(())
        }
    }

    Members(template).to_string()
}

fn render_native_launch_context(
    template: &str,
    issue: &Issue,
    worktree_path: &Path,
    template_id: &MobTemplateId,
    mob_template: &MaterializedMobTemplate,
) -> Result<String> {
    let body = template.trim();
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env.add_template("native_launch_context", body)
        .map_err(|err| FugueError::TemplateParseError(err.to_string()))?;
    env.get_template("native_launch_context")
        .map_err(|err| FugueError::TemplateParseError(err.to_string()))?
        .render(context! {
            issue => issue,
            worktree => worktree_path.display().to_string(),
            workspace_key => sanitize_workspace_key(&issue.identifier),
            template_id => template_id.0.as_str(),
            backend => mob_template.backend.canonical_name(),
            flow => mob_template.flow.map(|flow| flow.canonical_name()).unwrap_or("unspecified"),
            members => format_template_members(mob_template),
        })
        .map(|rendered| rendered.trim().to_string())
        .map_err(|err| FugueError::TemplateRenderError(err.to_string()))
}

impl TrackerConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        let kind = opt_string(raw, "kind")?.unwrap_or_default();
        let endpoint = opt_string(raw, "endpoint")?.unwrap_or_else(|| {
            if kind == "fugue-ims" {
                "fugue-ims.sqlite".to_string()
            } else if kind == "jira" {
                env::var("JIRA_BASE_URL").unwrap_or_default()
            } else {
                "https://api.linear.app/graphql".to_string()
            }
        });
        let configured_api_key = opt_string(raw, "api_key")?;
        let api_key = match configured_api_key {
            Some(value) => resolve_env_string(&value),
            None => env::var(if kind == "jira" {
                "JIRA_API_TOKEN"
            } else {
                "LINEAR_API_KEY"
            })
            .ok()
            .filter(|value| !value.is_empty()),
        };
        let active_states = opt_string_list(raw, "active_states")?
            .unwrap_or_else(|| vec!["Todo".to_string(), "Active".to_string()]);
        let intake_states = opt_string_list(raw, "intake_states")?.unwrap_or_default();
        let in_flight_states = opt_string_list(raw, "in_flight_states")?.unwrap_or_default();
        let terminal_states = opt_string_list(raw, "terminal_states")?.unwrap_or_else(|| {
            vec![
                "Closed".to_string(),
                "Cancelled".to_string(),
                "Canceled".to_string(),
                "Duplicate".to_string(),
                "Done".to_string(),
            ]
        });
        Ok(Self {
            kind,
            endpoint,
            api_key,
            jira: opt_value(raw, "jira")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()
                .map_err(|err| FugueError::InvalidConfig(format!("tracker.jira: {err}")))?,
            project_slug: opt_string(raw, "project_slug")?.map(|scope| scope.trim().to_owned()),
            intake_states,
            in_flight_states,
            active_states,
            attachment_root: None,
            max_attachment_files: opt_usize(raw, "max_attachment_files")?.unwrap_or(20),
            max_attachment_bytes: opt_u64(raw, "max_attachment_bytes")?.unwrap_or(25 * 1024 * 1024),
            max_attachment_total_bytes: opt_u64(raw, "max_attachment_total_bytes")?
                .unwrap_or(100 * 1024 * 1024),
            terminal_states,
        })
    }
}

impl FinalReviewConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        raw.map(|value| {
            serde_json::from_value(Value::Object(value.clone()))
                .map_err(|err| FugueError::InvalidConfig(format!("final_review is invalid: {err}")))
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
    }

    fn validate_static(&self) -> Result<()> {
        if self.minimum_reviewers == 0 {
            return Err(FugueError::InvalidConfig(
                "final_review.minimum_reviewers must be positive".into(),
            ));
        }
        let lanes = [
            ("lane", self.lane.as_str()),
            ("rework_lane", self.rework_lane.as_str()),
            ("blocked_lane", self.blocked_lane.as_str()),
            ("success_lane", self.success_lane.as_str()),
        ];
        for (name, lane) in lanes {
            if lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "final_review.{name} must be non-empty"
                )));
            }
        }
        let mut reviewer_ids = BTreeSet::new();
        for reviewer in &self.reviewers {
            if reviewer.id.trim().is_empty() || reviewer.profile.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "final_review.reviewers entries require non-empty id and profile".to_string(),
                ));
            }
            if !reviewer_ids.insert(reviewer.id.clone()) {
                return Err(FugueError::InvalidConfig(format!(
                    "final_review.reviewers contains duplicate id {:?}",
                    reviewer.id
                )));
            }
        }
        if self.enabled && !self.reviewers.iter().any(|reviewer| reviewer.required) {
            return Err(FugueError::InvalidConfig(
                "final_review.enabled requires at least one required reviewer".to_string(),
            ));
        }
        Ok(())
    }
}

impl DeployLaneConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        let mut config = raw
            .map(|value| {
                serde_json::from_value::<Self>(Value::Object(value.clone()))
                    .map_err(|err| FugueError::InvalidConfig(format!("deploy is invalid: {err}")))
            })
            .transpose()?
            .unwrap_or_default();
        if let Some(action_lane) = config.action_lane.take() {
            if raw.and_then(|value| value.get("lane")).is_some()
                && normalize_state(&config.lane) != normalize_state(&action_lane)
            {
                return Err(FugueError::InvalidConfig(
                    "deploy.lane and deploy.action_lane must match when both are set".to_string(),
                ));
            }
            config.lane = action_lane;
        }
        Ok(config)
    }

    fn validate_static(&self) -> Result<()> {
        let lanes = [
            ("lane", self.lane.as_str()),
            ("procedure", self.procedure.as_str()),
            ("success_lane", self.success_lane.as_str()),
            ("ready_lane", self.ready_lane.as_str()),
            ("rework_lane", self.rework_lane.as_str()),
            ("blocked_lane", self.blocked_lane.as_str()),
            ("base_branch", self.base_branch.as_str()),
        ];
        for (name, lane) in lanes {
            if lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "deploy.{name} must be non-empty"
                )));
            }
        }
        if self.enabled && self.allowed_repositories.is_empty() {
            return Err(FugueError::InvalidConfig(
                "deploy.enabled requires at least one allowed repository".to_string(),
            ));
        }
        if self
            .success_lane
            .trim()
            .eq_ignore_ascii_case(self.blocked_lane.trim())
        {
            return Err(FugueError::InvalidConfig(
                "deploy.success_lane and deploy.blocked_lane must differ".into(),
            ));
        }
        for repo in &self.allowed_repositories {
            if repo.owner.0.trim().is_empty() || repo.name.0.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "deploy.allowed_repositories entries require non-empty owner and name"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn allows_repository(&self, repo: &GitHubRepoRef) -> bool {
        self.allowed_repositories.iter().any(|allowed| {
            allowed.hostname() == repo.hostname()
                && allowed.owner.0.eq_ignore_ascii_case(&repo.owner.0)
                && allowed.name.0.eq_ignore_ascii_case(&repo.name.0)
        })
    }
}

impl IndependentReviewConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        raw.map(|value| {
            serde_json::from_value(Value::Object(value.clone())).map_err(|err| {
                FugueError::InvalidConfig(format!("independent_review is invalid: {err}"))
            })
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
    }

    fn validate_static(&self) -> Result<()> {
        let lanes = [
            ("lane", self.lane.as_str()),
            ("success_lane", self.success_lane.as_str()),
            ("rework_lane", self.rework_lane.as_str()),
            ("blocked_lane", self.blocked_lane.as_str()),
            ("orchestrator_profile", self.orchestrator_profile.as_str()),
        ];
        for (name, lane) in lanes {
            if lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "independent_review.{name} must be non-empty"
                )));
            }
        }
        if self.reviewer_count == 0 {
            return Err(FugueError::InvalidConfig(
                "independent_review.reviewer_count must be positive".to_string(),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(FugueError::InvalidConfig(
                "independent_review.timeout_ms must be positive".to_string(),
            ));
        }
        if self.findings_comment_marker.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "independent_review.findings_comment_marker must be non-empty".to_string(),
            ));
        }
        Ok(())
    }
}

impl LinearRuntimeConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        serde_json::from_value(Value::Object(raw.clone()))
            .map_err(|err| FugueError::InvalidConfig(format!("linear_runtime is invalid: {err}")))
    }
}

impl PollingConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        Ok(Self {
            interval_ms: opt_u64(raw, "interval_ms")?.unwrap_or(30_000),
        })
    }
}

impl WorkspaceConfig {
    fn from_raw(raw: Option<&Map<String, Value>>, workflow_dir: &Path) -> Result<Self> {
        let default_root = env::temp_dir().join("fugue_workspaces");
        let root = opt_string(raw, "root")?
            .map(|value| resolve_path_value(&value, workflow_dir))
            .transpose()?
            .unwrap_or(default_root);
        Ok(Self {
            root: absolute_clean_path(workflow_dir, &root),
        })
    }
}

impl HooksConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        Ok(Self {
            after_create: opt_string(raw, "after_create")?,
            before_run: opt_string(raw, "before_run")?,
            after_run: opt_string(raw, "after_run")?,
            before_remove: opt_string(raw, "before_remove")?,
            timeout_ms: opt_u64(raw, "timeout_ms")?.unwrap_or(60_000),
        })
    }
}

impl AgentConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        Ok(Self {
            max_concurrent_agents: opt_usize(raw, "max_concurrent_agents")?.unwrap_or(10),
            max_turns: opt_usize(raw, "max_turns")?.unwrap_or(20),
            max_retry_backoff_ms: opt_u64(raw, "max_retry_backoff_ms")?.unwrap_or(300_000),
            max_concurrent_agents_by_state: parse_concurrency_by_state(raw)?,
        })
    }
}

impl MobExecutionConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let backend = opt_backend(Some(raw), "mob.backend")?.unwrap_or_default();
        let native = raw
            .get("native")
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|err| {
                    FugueError::InvalidConfig(format!("mob.native is invalid: {err}"))
                })
            })
            .transpose()?
            .unwrap_or_else(NativeMobRuntimeConfig::native_default);
        let templates = raw
            .get("templates")
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|err| {
                    FugueError::InvalidConfig(format!("mob.templates is invalid: {err}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let selection = raw
            .get("selection")
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|err| {
                    FugueError::InvalidConfig(format!("mob.selection is invalid: {err}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let mcp_servers = raw
            .get("mcp_servers")
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|err| {
                    FugueError::InvalidConfig(format!("mob.mcp_servers is invalid: {err}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            backend,
            native,
            templates,
            mcp_servers,
            selection,
        })
    }

    pub fn validate_before_dispatch(&self) -> Result<()> {
        if !self.native.enabled {
            return Err(FugueError::InvalidConfig(
                "mob.native.enabled must be true; Fugue only supports native Meta execution"
                    .to_string(),
            ));
        }
        if self.native.enabled && self.templates.is_empty() {
            return Err(FugueError::InvalidConfig(
                "mob.native.enabled requires at least one materialized mob template".to_string(),
            ));
        }
        Ok(())
    }
}

fn parse_concurrency_by_state(raw: Option<&Map<String, Value>>) -> Result<BTreeMap<String, usize>> {
    let mut by_state = BTreeMap::new();
    let Some(value) = opt_value(raw, "max_concurrent_agents_by_state") else {
        return Ok(by_state);
    };
    let Some(map) = value.as_object() else {
        return Err(invalid_type(
            "max_concurrent_agents_by_state",
            "a map of positive integers",
        ));
    };
    for (state, value) in map {
        let limit = value.as_u64().ok_or_else(|| {
            FugueError::InvalidConfig(format!(
                "max_concurrent_agents_by_state.{state} must be a positive integer"
            ))
        })?;
        if limit == 0 {
            return Err(FugueError::InvalidConfig(format!(
                "max_concurrent_agents_by_state.{state} must be positive"
            )));
        }
        let limit = usize::try_from(limit).map_err(|_| {
            FugueError::InvalidConfig(format!(
                "max_concurrent_agents_by_state.{state} is too large"
            ))
        })?;
        by_state.insert(normalize_state(state), limit);
    }
    Ok(by_state)
}

impl MeerkatConfig {
    fn from_raw(raw: Option<&Map<String, Value>>, workflow_dir: &Path) -> Result<Self> {
        if raw.is_some_and(|raw| raw.contains_key("connection_ref")) {
            return Err(FugueError::InvalidConfig(
                "meerkat.connection_ref was replaced by meerkat.auth_binding in 0.9.3".to_string(),
            ));
        }
        let command = opt_string(raw, "command")?.unwrap_or_else(|| "rkat-rpc".to_string());
        let realm_backend = opt_string(raw, "realm_backend")?;
        if let Some(value) = &realm_backend
            && value != "sqlite"
            && value != "jsonl"
        {
            return Err(FugueError::InvalidConfig(
                "meerkat.realm_backend must be sqlite or jsonl".to_string(),
            ));
        }
        let auth_binding = raw
            .and_then(|obj| obj.get("auth_binding"))
            .map(parse_auth_binding)
            .transpose()?;
        let preload_skills = raw
            .and_then(|obj| obj.get("preload_skills"))
            .map(parse_preload_skills)
            .transpose()?;
        let skill_refs = raw
            .and_then(|obj| obj.get("skill_refs"))
            .map(parse_skill_refs)
            .transpose()?;
        let max_sessions = opt_u64(raw, "max_sessions")?.unwrap_or(4096);
        if max_sessions == 0 {
            return Err(FugueError::InvalidConfig(
                "meerkat.max_sessions must be positive".to_string(),
            ));
        }
        let max_sessions = usize::try_from(max_sessions).map_err(|_| {
            FugueError::InvalidConfig("meerkat.max_sessions exceeds platform capacity".to_string())
        })?;
        Ok(Self {
            runtime_connections: None,
            command,
            repository_runtime: raw
                .and_then(|raw| raw.get("repository_runtime"))
                .filter(|value| !value.is_null())
                .map(|value| {
                    let mut binding: RepositoryRuntimeBinding =
                        serde_json::from_value(value.clone()).map_err(|error| {
                            FugueError::InvalidConfig(format!(
                                "meerkat.repository_runtime: {error}"
                            ))
                        })?;
                    binding.state_path = absolute_clean_path(workflow_dir, &binding.state_path);
                    Ok::<_, FugueError>(binding)
                })
                .transpose()?,
            realm: opt_string(raw, "realm")?.and_then(|v| resolve_env_string(&v)),
            instance: opt_string(raw, "instance")?.and_then(|v| resolve_env_string(&v)),
            realm_backend,
            state_root: path_field(raw, "state_root", workflow_dir)?,
            context_root: path_field(raw, "context_root", workflow_dir)?,
            user_config_root: path_field(raw, "user_config_root", workflow_dir)?,
            auth_binding,
            model: opt_string(raw, "model")?.or_else(|| Some(default_agent_model().to_string())),
            provider: opt_string(raw, "provider")?,
            max_tokens: opt_u64(raw, "max_tokens")?,
            system_prompt: opt_string(raw, "system_prompt")?,
            output_schema: opt_object_field(raw, "output_schema")?,
            structured_output_retries: opt_u64(raw, "structured_output_retries")?,
            hooks_override: opt_object_field(raw, "hooks_override")?,
            provider_params: opt_object_field(raw, "provider_params")?,
            enable_shell: opt_bool(raw, "enable_shell")?,
            enable_builtins: opt_bool(raw, "enable_builtins")?,
            enable_mob: opt_bool(raw, "enable_mob")?,
            enable_memory: opt_bool(raw, "enable_memory")?,
            keep_alive: opt_bool(raw, "keep_alive")?.unwrap_or(false),
            max_sessions,
            comms_name: opt_string(raw, "comms_name")?,
            peer_meta: opt_object_field(raw, "peer_meta")?,
            budget_limits: opt_object_field(raw, "budget_limits")?,
            preload_skills,
            skill_refs,
            labels: opt_string_map(raw, "labels")?,
            additional_instructions: opt_string_list(raw, "additional_instructions")?,
            app_context: opt_object_field(raw, "app_context")?,
            shell_env: opt_string_map(raw, "shell_env")?,
            flow_tool_overlay: opt_object_field(raw, "flow_tool_overlay")?,
            first_turn_activation_prompt: opt_string(raw, "first_turn_activation_prompt")?
                .unwrap_or_else(|| "Begin.".to_string()),
            turn_timeout_ms: opt_u64(raw, "turn_timeout_ms")?.unwrap_or(3_600_000),
            read_timeout_ms: opt_u64(raw, "read_timeout_ms")?.unwrap_or(5_000),
            stall_timeout_ms: opt_i64(raw, "stall_timeout_ms")?.unwrap_or(300_000),
        })
    }
}

impl KapellmeisterConfig {
    fn from_raw(raw: Option<&Map<String, Value>>, workflow_dir: &Path) -> Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let authority = KapellmeisterToolAuthority::from_config(authority_config_from_raw(raw)?)?;
        let shadow_mode = KapellmeisterShadowModeConfig::from_raw(
            section_object(raw, "shadow_mode")?,
            workflow_dir,
            authority,
        )?;
        Ok(Self { shadow_mode })
    }
}

impl KapellmeisterShadowModeConfig {
    fn from_raw(
        raw: Option<&Map<String, Value>>,
        workflow_dir: &Path,
        authority: KapellmeisterToolAuthority,
    ) -> Result<Self> {
        let Some(raw) = raw else {
            return Ok(Self {
                authority,
                ..Self::default()
            });
        };
        Ok(Self {
            enabled: opt_bool(Some(raw), "enabled")?.unwrap_or(false),
            state_path: path_field(Some(raw), "state_path", workflow_dir)?.unwrap_or_else(|| {
                absolute_clean_path(
                    workflow_dir,
                    &PathBuf::from(".fugue/kapellmeister-shadow.json"),
                )
            }),
            prompt: opt_string(Some(raw), "prompt")?
                .unwrap_or_else(|| DEFAULT_KAPELLMEISTER_SHADOW_PROMPT.to_string()),
            authority,
        })
    }
}

fn authority_config_from_raw(raw: &Map<String, Value>) -> Result<KapellmeisterAuthorityConfig> {
    let mode = opt_authority_mode(Some(raw), "authority_mode")?
        .unwrap_or(KapellmeisterAuthorityMode::ShadowOnly);
    let safe_selectors = safe_selectors_from_raw(section_object(raw, "safe_selectors")?)?;
    Ok(KapellmeisterAuthorityConfig {
        mode,
        safe_selectors,
    })
}

fn opt_authority_mode(
    raw: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<KapellmeisterAuthorityMode>> {
    opt_string(raw, key)?
        .map(|value| authority_mode_from_str(&value, key))
        .transpose()
}

fn authority_mode_from_str(value: &str, key: &str) -> Result<KapellmeisterAuthorityMode> {
    match value {
        "shadow_only" => Ok(KapellmeisterAuthorityMode::ShadowOnly),
        "guarded_labeled" => Ok(KapellmeisterAuthorityMode::GuardedLabeled),
        other => Err(FugueError::InvalidConfig(format!(
            "{key} must be shadow_only or guarded_labeled, got {other}"
        ))),
    }
}

fn safe_selectors_from_raw(raw: Option<&Map<String, Value>>) -> Result<KapellmeisterSafeSelectors> {
    let Some(raw) = raw else {
        return Ok(KapellmeisterSafeSelectors::default());
    };
    Ok(KapellmeisterSafeSelectors {
        repositories: opt_repo_list(Some(raw), "repositories")?.unwrap_or_default(),
        linear_labels: opt_string_list(Some(raw), "linear_labels")?.unwrap_or_default(),
    })
}

fn opt_repo_list(
    raw: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<Vec<GitHubRepoRef>>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| repo_ref_from_value(value, key))
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(_) => Err(invalid_type(key, "a list of repository selectors")),
    }
}

fn repo_ref_from_value(value: &Value, key: &str) -> Result<GitHubRepoRef> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_type(key, "a list of repository selector objects"))?;
    let owner = opt_string(Some(object), "owner")?
        .ok_or_else(|| FugueError::InvalidConfig(format!("{key}.owner is required")))?;
    let name = opt_string(Some(object), "name")?
        .ok_or_else(|| FugueError::InvalidConfig(format!("{key}.name is required")))?;
    Ok(GitHubRepoRef {
        host: opt_string(Some(object), "host")?
            .map(crate::tools::GitHubHost::try_from)
            .transpose()
            .map_err(FugueError::InvalidConfig)?,
        owner: GitHubOwner(owner),
        name: GitHubRepoName(name),
    })
}

impl ServerConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Option<Self>> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        if raw.contains_key("mobkit_console") {
            return Err(FugueError::InvalidConfig(
                "server.mobkit_console was replaced by server.meta_console in 0.9.3".to_string(),
            ));
        }
        let port = opt_u64(Some(raw), "port")?
            .map(|port| {
                u16::try_from(port).map_err(|_| {
                    FugueError::InvalidConfig("server.port must fit in u16".to_string())
                })
            })
            .transpose()?;
        let meta_console = MetaConsoleServerConfig::from_raw(section_object(raw, "meta_console")?)?;
        Ok(Some(Self { port, meta_console }))
    }
}

impl MetaConsoleServerConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        let enabled = opt_bool(raw, "enabled")?.unwrap_or(true);
        let path = opt_string(raw, "path")?.unwrap_or_else(|| "/console".to_string());
        validate_console_path(&path)?;
        let asset_dir = opt_string(raw, "asset_dir")?
            .map(|value| {
                if value.trim().is_empty() {
                    return Err(FugueError::InvalidConfig(
                        "server.meta_console.asset_dir must not be empty".to_string(),
                    ));
                }
                Ok(PathBuf::from(value))
            })
            .transpose()?;
        let identity_provider = opt_string(raw, "identity_provider")?;
        let rpc_timeout_ms = opt_u64(raw, "rpc_timeout_ms")?.unwrap_or(30_000);
        let basic_auth_raw = opt_object_field(raw, "basic_auth")?;
        let basic_auth = MetaConsoleBasicAuthConfig::from_raw(basic_auth_raw.as_ref())?;
        if rpc_timeout_ms == 0 {
            return Err(FugueError::InvalidConfig(
                "server.meta_console.rpc_timeout_ms must be positive".to_string(),
            ));
        }
        Ok(Self {
            enabled,
            path,
            asset_dir,
            identity_provider,
            rpc_timeout_ms,
            basic_auth,
        })
    }
}

fn validate_console_path(path: &str) -> Result<()> {
    if path != "/console" {
        return Err(FugueError::InvalidConfig(
            "server.meta_console.path must be /console; Fugue only serves the Meta console path"
                .to_string(),
        ));
    }
    if path == "/" || !path.starts_with('/') {
        return Err(FugueError::InvalidConfig(
            "server.meta_console.path must be an absolute non-root HTTP path".to_string(),
        ));
    }
    if path.ends_with('/') || path.contains("//") {
        return Err(FugueError::InvalidConfig(
            "server.meta_console.path must not end with '/' or contain '//'".to_string(),
        ));
    }
    if path.chars().any(|ch| {
        ch.is_control() || ch.is_whitespace() || matches!(ch, '?' | '#' | '{' | '}' | '*')
    }) {
        return Err(FugueError::InvalidConfig(
            "server.meta_console.path contains characters that cannot be used as a route"
                .to_string(),
        ));
    }
    Ok(())
}

impl MetaConsoleBasicAuthConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Option<Self>> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        let username_env = opt_string(Some(raw), "username_env")?
            .unwrap_or_else(|| "FUGUE_CONSOLE_USERNAME".to_string());
        let password_env = opt_string(Some(raw), "password_env")?
            .unwrap_or_else(|| "FUGUE_CONSOLE_PASSWORD".to_string());
        let realm = opt_string(Some(raw), "realm")?.unwrap_or_else(|| "Meerkat Fugue".to_string());
        validate_non_empty_console_auth_field(
            "server.meta_console.basic_auth.username_env",
            &username_env,
        )?;
        validate_non_empty_console_auth_field(
            "server.meta_console.basic_auth.password_env",
            &password_env,
        )?;
        validate_console_auth_realm(&realm)?;
        Ok(Some(Self {
            username_env,
            password_env,
            realm,
        }))
    }
}

fn validate_non_empty_console_auth_field(key: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(FugueError::InvalidConfig(format!(
            "{key} must be non-empty"
        )));
    }
    Ok(())
}

fn validate_console_auth_realm(value: &str) -> Result<()> {
    validate_non_empty_console_auth_field("server.meta_console.basic_auth.realm", value)?;
    if value
        .chars()
        .any(|character| character == '"' || character == '\\' || character.is_control())
    {
        return Err(FugueError::InvalidConfig(
            "server.meta_console.basic_auth.realm must not contain quotes, backslashes, or control characters"
                .to_string(),
        ));
    }
    Ok(())
}

impl OtaUpdateConfig {
    fn from_raw(raw: Option<&Map<String, Value>>, workflow_dir: &Path) -> Result<Option<Self>> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        let enabled = opt_bool(Some(raw), "enabled")?.unwrap_or(false);
        let repo_path = opt_string(Some(raw), "repo_path")?
            .map(|value| resolve_path_value(&value, workflow_dir))
            .transpose()?
            .unwrap_or_else(|| workflow_dir.to_path_buf());
        let branch = opt_string(Some(raw), "branch")?.unwrap_or_else(|| "main".to_string());
        let remote = opt_string(Some(raw), "remote")?.unwrap_or_else(|| "origin".to_string());
        let build_command = opt_string(Some(raw), "build_command")?.unwrap_or_default();
        let health_check_command = opt_string(Some(raw), "health_check_command")?;
        let deploy_command = opt_string(Some(raw), "deploy_command")?.unwrap_or_default();
        let timeout_ms = opt_u64(Some(raw), "timeout_ms")?.unwrap_or(600_000);
        let require_clean = opt_bool(Some(raw), "require_clean")?.unwrap_or(true);
        let lock_path = opt_string(Some(raw), "lock_path")?
            .map(|value| resolve_path_value(&value, workflow_dir))
            .transpose()?
            .map(|path| absolute_clean_path(workflow_dir, &path));
        let trigger_token_env = opt_string(Some(raw), "trigger_token_env")?;
        let config = Self {
            enabled,
            repo_path: absolute_clean_path(workflow_dir, &repo_path),
            branch,
            remote,
            build_command,
            health_check_command,
            deploy_command,
            timeout_ms,
            require_clean,
            lock_path,
            trigger_token_env,
        };
        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.branch.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "ota_update.branch must be non-empty when OTA updates are enabled".to_string(),
                ));
            }
            if self.remote.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "ota_update.remote must be non-empty when OTA updates are enabled".to_string(),
                ));
            }
            if self.build_command.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "ota_update.build_command must be non-empty when OTA updates are enabled"
                        .to_string(),
                ));
            }
            if self.deploy_command.trim().is_empty() {
                return Err(FugueError::InvalidConfig(
                    "ota_update.deploy_command must be non-empty when OTA updates are enabled"
                        .to_string(),
                ));
            }
            if self
                .health_check_command
                .as_deref()
                .is_some_and(|command| command.trim().is_empty())
            {
                return Err(FugueError::InvalidConfig(
                    "ota_update.health_check_command must be non-empty when set".to_string(),
                ));
            }
            if self
                .trigger_token_env
                .as_deref()
                .is_some_and(|name| name.trim().is_empty())
            {
                return Err(FugueError::InvalidConfig(
                    "ota_update.trigger_token_env must be non-empty when set".to_string(),
                ));
            }
        }
        if self.timeout_ms == 0 {
            return Err(FugueError::InvalidConfig(
                "ota_update.timeout_ms must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

fn load_workflow_file(path: &Path) -> Result<(WorkflowDefinition, Option<SystemTime>)> {
    let contents = fs::read_to_string(path)
        .map_err(|_| FugueError::MissingWorkflowFile(path.to_path_buf()))?;
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok();
    let definition = match path.extension().and_then(|extension| extension.to_str()) {
        Some("yaml" | "yml") => parse_native_workflow(path, &contents)?,
        _ => {
            return Err(FugueError::WorkflowParseError(
                "Fugue only supports fugue.yaml".to_string(),
            ));
        }
    };
    Ok((definition, modified))
}

pub fn parse_native_workflow(path: &Path, contents: &str) -> Result<WorkflowDefinition> {
    let native: NativeFugueConfig = serde_yaml::from_str(contents)
        .map_err(|err| FugueError::WorkflowParseError(err.to_string()))?;
    native.into_workflow_definition(path)
}

impl NativeFugueConfig {
    pub(crate) fn into_workflow_definition(self, path: &Path) -> Result<WorkflowDefinition> {
        self.validate()?;
        let workflow_dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let prompt_template = self.worker.load_prompt_template(&workflow_dir)?;
        let native_launch_context_template = self
            .worker
            .load_native_launch_context_template(&workflow_dir)?;
        let (backend, native_runtime) = self.native_backend_and_runtime()?;
        let effective_linear = self.effective_linear_config();
        let effective_mcp_servers = native_mcp_servers_with_default_servers(&self.mcp_servers);
        let effective_agents =
            native_agents_with_default_mcp_tools(&self.agents, &effective_mcp_servers);
        let materialized_templates = materialize_native_mob_templates(
            &effective_agents,
            &self.mob_templates,
            &self.linear_projections,
            &effective_linear,
            backend,
        )?;
        let mut root = Map::new();
        root.insert(
            "tracker".to_string(),
            native_tracker_value(
                &effective_linear,
                self.deploy.as_ref(),
                self.independent_review.as_ref(),
                materialized_templates.values(),
            ),
        );
        root.insert(
            "linear_runtime".to_string(),
            json!(native_linear_runtime_value(
                &effective_linear,
                self.final_review.as_ref(),
                self.deploy.as_ref(),
                self.independent_review.as_ref(),
                materialized_templates.values()
            )),
        );
        if let Some(polling) = &self.polling {
            root.insert("polling".to_string(), json!(polling));
        }
        if let Some(workspace) = &self.workspace {
            root.insert("workspace".to_string(), json!(workspace));
        }
        if let Some(hooks) = &self.hooks {
            root.insert("hooks".to_string(), json!(hooks));
        }
        if let Some(agent) = &self.runtime_agent {
            root.insert("agent".to_string(), json!(agent));
        }
        root.insert(
            "mob".to_string(),
            Value::Object(self.native_mob_value(
                backend,
                native_runtime,
                materialized_templates,
                &effective_mcp_servers,
            )?),
        );
        let meerkat = self.native_meerkat_value()?;
        if !meerkat.is_empty() {
            root.insert("meerkat".to_string(), Value::Object(meerkat));
        }
        if let Some(kapellmeister) = &self.kapellmeister {
            root.insert("kapellmeister".to_string(), json!(kapellmeister));
        }
        if let Some(final_review) = &self.final_review {
            root.insert("final_review".to_string(), json!(final_review));
        }
        if let Some(deploy) = &self.deploy {
            root.insert("deploy".to_string(), json!(deploy));
        }
        if let Some(independent_review) = &self.independent_review {
            root.insert("independent_review".to_string(), json!(independent_review));
        }
        if let Some(server) = &self.server {
            root.insert("server".to_string(), json!(server));
        }
        if let Some(ota_update) = &self.ota_update {
            root.insert("ota_update".to_string(), json!(ota_update));
        }
        if let Some(template) = native_launch_context_template {
            root.insert(
                "worker_policy".to_string(),
                json!({ "native_launch_context_template": template }),
            );
        }
        Ok(WorkflowDefinition {
            config: root,
            prompt_template,
        })
    }

    fn validate(&self) -> Result<()> {
        if let Some(version) = self.version
            && version != 1
        {
            return Err(FugueError::InvalidConfig(
                "fugue.yaml version must be 1 when set".to_string(),
            ));
        }
        let backend = native_config_backend(self.backend.as_deref(), "backend")?;
        if let Some(mob) = &self.mob {
            if let Some(mob_backend_value) = mob.backend.as_deref() {
                let mob_backend = parse_backend(Some(mob_backend_value), "mob.backend")?;
                if backend != mob_backend {
                    return Err(FugueError::InvalidConfig(
                        "backend and mob.backend must agree; Fugue only supports native-execution"
                            .to_string(),
                    ));
                }
            }
            if let Some(native) = &mob.native
                && !native.enabled
            {
                return Err(FugueError::InvalidConfig(
                        "mob.native.enabled=false is unsupported; Fugue only supports native Meta execution"
                            .to_string(),
                    ));
            }
        }
        let effective_linear = self.effective_linear_config();
        let effective_mcp_servers = native_mcp_servers_with_default_servers(&self.mcp_servers);
        validate_linear_config(&effective_linear)?;
        validate_linear_projections(&self.linear_projections)?;
        validate_labels(&self.linear.labels, &self.selection.by_label)?;
        validate_agent_profiles(
            &self.agents,
            &effective_mcp_servers,
            self.linear.kind.as_deref(),
        )?;
        validate_mob_templates(
            &self.agents,
            &self.mob_templates,
            &self.linear_projections,
            &effective_linear,
            backend,
        )?;
        validate_selection(&self.selection, &self.mob_templates)?;
        self.worker.validate()?;
        Ok(())
    }

    fn native_backend_and_runtime(&self) -> Result<(MobExecutionBackend, NativeMobRuntimeConfig)> {
        let default_backend = native_config_backend(self.backend.as_deref(), "backend")?;
        let backend = self
            .mob
            .as_ref()
            .and_then(|mob| mob.backend.as_deref())
            .map(|value| parse_backend(Some(value), "mob.backend"))
            .transpose()?
            .unwrap_or(default_backend);
        let native = self
            .mob
            .as_ref()
            .and_then(|mob| mob.native.clone())
            .unwrap_or_else(NativeMobRuntimeConfig::native_default);
        Ok((backend, native))
    }

    fn effective_linear_config(&self) -> NativeLinearConfig {
        let mut linear = self.linear.clone();
        if let Some(roles) = &self.tracker_lanes {
            linear.lanes.parked = roles.parked.clone();
            linear.lanes.ready = roles.intake.clone();
            linear.lanes.active = Some(roles.active.clone());
            linear.lanes.do_review = Some(roles.review_action.clone());
            linear.lanes.deploy = Some(roles.deploy_action.clone());
            linear.lanes.blocked = Some(roles.blocked.clone());
            linear.lanes.success = roles.success.clone();
            linear.lanes.canceled = roles.canceled.clone();
        }
        linear
    }

    fn native_mob_value(
        &self,
        backend: MobExecutionBackend,
        native: NativeMobRuntimeConfig,
        templates: BTreeMap<String, MaterializedMobTemplate>,
        mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
    ) -> Result<Map<String, Value>> {
        let mut value = Map::new();
        value.insert(
            "backend".to_string(),
            Value::String(backend.canonical_name().to_string()),
        );
        value.insert("native".to_string(), json!(native));
        value.insert("templates".to_string(), json!(templates));
        if !mcp_servers.is_empty() {
            value.insert(
                "mcp_servers".to_string(),
                json!(native_mcp_servers_with_linear_scope(
                    &self.linear,
                    mcp_servers
                )),
            );
        }
        value.insert("selection".to_string(), json!(self.selection));
        Ok(value)
    }

    fn native_meerkat_value(&self) -> Result<Map<String, Value>> {
        let mut value = self
            .meerkat
            .as_ref()
            .map(to_json_object)
            .transpose()?
            .unwrap_or_default();
        value
            .entry("command".to_string())
            .or_insert_with(|| Value::String("rkat-rpc".to_string()));

        if let Some(profile) = self.default_worker_profile()? {
            insert_profile_default(&mut value, "model", profile.model.clone());
            insert_profile_default(&mut value, "provider", profile.provider.clone());
            insert_profile_default_u64(&mut value, "max_tokens", profile.max_tokens);
            insert_profile_default(&mut value, "system_prompt", profile.system_prompt.clone());
            insert_profile_default_object(
                &mut value,
                "output_schema",
                profile.output_schema.clone(),
            );
            insert_profile_default_u64(
                &mut value,
                "structured_output_retries",
                profile.structured_output_retries,
            );
            insert_profile_default_object(
                &mut value,
                "provider_params",
                profile.provider_params.clone(),
            );
            if !profile.tools.is_empty() && !value.contains_key("labels") {
                let labels = profile
                    .tools
                    .iter()
                    .map(|tool| (tool.clone(), Value::String("enabled".to_string())))
                    .collect::<Map<_, _>>();
                value.insert("labels".to_string(), Value::Object(labels));
            }
            if !profile.skills.is_empty() && !value.contains_key("additional_instructions") {
                value.insert(
                    "additional_instructions".to_string(),
                    json!(
                        profile
                            .skills
                            .iter()
                            .map(|skill| format!("Skill requested by fugue.yaml profile: {skill}"))
                            .collect::<Vec<_>>()
                    ),
                );
            }
            if !value.contains_key("additional_instructions")
                && let Some(instructions) = &profile.additional_instructions
            {
                value.insert("additional_instructions".to_string(), json!(instructions));
            }
        }
        Ok(value)
    }

    fn default_worker_profile(&self) -> Result<Option<&NativeAgentProfile>> {
        let Some(template) = self.mob_templates.get(&self.selection.default_template) else {
            return Ok(None);
        };
        let Some(member) = template.members.first() else {
            return Ok(None);
        };
        Ok(self.agents.get(member.profile_name()))
    }
}

impl NativeWorkerConfig {
    fn validate(&self) -> Result<()> {
        match (&self.prompt_template, &self.prompt_file) {
            (Some(_), Some(_)) => Err(FugueError::InvalidConfig(
                "worker.prompt_template and worker.prompt_file are mutually exclusive".to_string(),
            )),
            (None, None) => Err(FugueError::InvalidConfig(
                "worker.prompt_template or worker.prompt_file is required".to_string(),
            )),
            _ => match (
                &self.native_launch_context_template,
                &self.native_launch_context_file,
            ) {
                (Some(_), Some(_)) => Err(FugueError::InvalidConfig(
                    "worker.native_launch_context_template and worker.native_launch_context_file are mutually exclusive"
                        .to_string(),
                )),
                _ => Ok(()),
            },
        }
    }

    fn load_prompt_template(&self, workflow_dir: &Path) -> Result<String> {
        if let Some(template) = &self.prompt_template {
            return Ok(template.trim().to_string());
        }
        let prompt_file = self.prompt_file.as_ref().ok_or_else(|| {
            FugueError::InvalidConfig("worker.prompt_file is required".to_string())
        })?;
        let prompt_path = absolute_clean_path(workflow_dir, prompt_file);
        fs::read_to_string(&prompt_path)
            .map(|contents| contents.trim().to_string())
            .map_err(|err| {
                FugueError::InvalidConfig(format!(
                    "failed to read worker.prompt_file {}: {err}",
                    prompt_path.display()
                ))
            })
    }

    fn load_native_launch_context_template(&self, workflow_dir: &Path) -> Result<Option<String>> {
        if let Some(template) = &self.native_launch_context_template {
            return Ok(Some(template.trim().to_string()));
        }
        let Some(context_file) = &self.native_launch_context_file else {
            return Ok(None);
        };
        let context_path = absolute_clean_path(workflow_dir, context_file);
        fs::read_to_string(&context_path)
            .map(|contents| Some(contents.trim().to_string()))
            .map_err(|err| {
                FugueError::InvalidConfig(format!(
                    "failed to read worker.native_launch_context_file {}: {err}",
                    context_path.display()
                ))
            })
    }
}

impl WorkerPolicyConfig {
    fn from_raw(raw: Option<&Map<String, Value>>) -> Result<Self> {
        Ok(Self {
            native_launch_context_template: opt_string(raw, "native_launch_context_template")?,
        })
    }
}

fn native_tracker_value<'a>(
    linear: &NativeLinearConfig,
    deploy: Option<&DeployLaneConfig>,
    independent_review: Option<&IndependentReviewConfig>,
    templates: impl Iterator<Item = &'a MaterializedMobTemplate>,
) -> Value {
    let mut intake_states = Vec::new();
    add_unique_state(&mut intake_states, linear.lanes.ready.clone());
    if let Some(rework) = &linear.lanes.rework {
        add_unique_state(&mut intake_states, rework.clone());
    }

    let mut in_flight_states = Vec::new();
    for template in templates {
        if let Some(projection) = &template.linear_projection {
            add_projection_active_lanes(&mut in_flight_states, projection);
        }
    }

    let mut active_states = intake_states.clone();
    for state in &in_flight_states {
        add_unique_state(&mut active_states, state.clone());
    }
    if let Some(do_review) = &linear.lanes.do_review {
        add_unique_state(&mut active_states, do_review.clone());
    }
    if let Some(deploy_lane) = &linear.lanes.deploy {
        add_unique_state(&mut active_states, deploy_lane.clone());
    }
    if let Some(review) = independent_review.filter(|config| config.enabled) {
        add_unique_state(&mut active_states, review.lane.clone());
    }
    if let Some(deploy) = deploy.filter(|config| config.enabled) {
        add_unique_state(&mut active_states, deploy.lane.clone());
    }

    let mut terminal_states = Vec::new();
    add_unique_state(&mut terminal_states, linear.lanes.success.clone());
    add_unique_state(&mut terminal_states, linear.lanes.canceled.clone());
    add_unique_state(&mut terminal_states, linear.lanes.duplicate.clone());
    if let Some(deploy) = deploy.filter(|config| config.enabled) {
        add_unique_state(&mut terminal_states, deploy.success_lane.clone());
    }
    let mut tracker = Map::new();
    tracker.insert(
        "kind".to_string(),
        Value::String(linear.kind.clone().unwrap_or_else(|| "linear".to_string())),
    );
    if let Some(endpoint) = &linear.endpoint {
        tracker.insert("endpoint".to_string(), Value::String(endpoint.clone()));
    }
    if let Some(api_key) = &linear.api_key {
        tracker.insert("api_key".to_string(), Value::String(api_key.clone()));
    }
    if let Some(jira) = &linear.jira {
        tracker.insert("jira".to_string(), json!(jira));
    }
    if let Some(project_slug) = &linear.project_slug {
        tracker.insert(
            "project_slug".to_string(),
            Value::String(project_slug.clone()),
        );
    }
    tracker.insert("intake_states".to_string(), json!(intake_states));
    tracker.insert("in_flight_states".to_string(), json!(in_flight_states));
    tracker.insert("active_states".to_string(), json!(active_states));
    tracker.insert("terminal_states".to_string(), json!(terminal_states));
    Value::Object(tracker)
}

fn native_linear_runtime_value<'a>(
    linear: &NativeLinearConfig,
    final_review: Option<&FinalReviewConfig>,
    deploy: Option<&DeployLaneConfig>,
    independent_review: Option<&IndependentReviewConfig>,
    templates: impl Iterator<Item = &'a MaterializedMobTemplate>,
) -> LinearRuntimeConfig {
    LinearRuntimeConfig {
        team: linear.team.clone(),
        provisioning: linear.provisioning.clone(),
        lanes: linear.lanes.clone(),
        required_states: required_linear_states(
            linear,
            final_review,
            deploy,
            independent_review,
            templates,
        ),
    }
}

fn required_linear_states<'a>(
    linear: &NativeLinearConfig,
    final_review: Option<&FinalReviewConfig>,
    deploy: Option<&DeployLaneConfig>,
    independent_review: Option<&IndependentReviewConfig>,
    templates: impl Iterator<Item = &'a MaterializedMobTemplate>,
) -> Vec<LinearRequiredState> {
    let mut required = Vec::new();
    push_required_state(
        &mut required,
        linear.lanes.parked.clone(),
        LinearStateType::Backlog,
    );
    push_required_state(
        &mut required,
        linear.lanes.ready.clone(),
        LinearStateType::Unstarted,
    );
    if let Some(rework) = &linear.lanes.rework {
        push_required_state(&mut required, rework.clone(), LinearStateType::Unstarted);
    }
    for template in templates {
        if let Some(projection) = &template.linear_projection {
            for state in projection_active_lane_order(projection) {
                push_required_state(&mut required, state, LinearStateType::Started);
            }
        }
    }
    if let Some(review_ready) = &linear.lanes.review_ready {
        push_required_state(
            &mut required,
            review_ready.clone(),
            LinearStateType::Started,
        );
    }
    if let Some(final_review) = final_review.filter(|config| config.enabled) {
        push_required_state(
            &mut required,
            final_review.lane.clone(),
            LinearStateType::Started,
        );
    }
    if let Some(do_review) = &linear.lanes.do_review {
        push_required_state(&mut required, do_review.clone(), LinearStateType::Started);
    }
    if let Some(deploy_lane) = &linear.lanes.deploy {
        push_required_state(&mut required, deploy_lane.clone(), LinearStateType::Started);
    }
    if let Some(review) = independent_review.filter(|config| config.enabled) {
        push_required_state(&mut required, review.lane.clone(), LinearStateType::Started);
    }
    if let Some(deploy) = deploy.filter(|config| config.enabled) {
        push_required_state(&mut required, deploy.lane.clone(), LinearStateType::Started);
        push_required_state(
            &mut required,
            deploy.success_lane.clone(),
            LinearStateType::Completed,
        );
    }
    if let Some(blocked) = &linear.lanes.blocked {
        push_required_state(&mut required, blocked.clone(), LinearStateType::Started);
    }
    push_required_state(
        &mut required,
        linear.lanes.success.clone(),
        LinearStateType::Completed,
    );
    push_required_state(
        &mut required,
        linear.lanes.canceled.clone(),
        LinearStateType::Canceled,
    );
    push_required_state(
        &mut required,
        linear.lanes.duplicate.clone(),
        LinearStateType::Duplicate,
    );
    required
}

fn add_unique_state(states: &mut Vec<String>, state: String) {
    if state.trim().is_empty() {
        return;
    }
    let normalized = normalize_state(&state);
    if !states
        .iter()
        .any(|candidate| normalize_state(candidate) == normalized)
    {
        states.push(state);
    }
}

fn add_projection_active_lanes(
    states: &mut Vec<String>,
    projection: &MaterializedLinearProjection,
) {
    for state in projection_active_lane_order(projection) {
        add_unique_state(states, state);
    }
}

fn projection_active_lane_order(projection: &MaterializedLinearProjection) -> Vec<String> {
    let mut states = Vec::new();

    for state in projection.aggregate.precedence.iter().rev() {
        add_unique_state(&mut states, state.clone());
    }
    for state in projection.node_lanes.values() {
        add_unique_state(&mut states, state.clone());
    }
    if let Some(fallback) = &projection.aggregate.fallback {
        add_unique_state(&mut states, fallback.clone());
    }
    add_unique_state(&mut states, projection.terminal_lanes.failed.clone());

    states
}

fn push_required_state(
    states: &mut Vec<LinearRequiredState>,
    name: String,
    state_type: LinearStateType,
) {
    if name.trim().is_empty() {
        return;
    }
    let normalized = normalize_state(&name);
    if states
        .iter()
        .any(|candidate| normalize_state(&candidate.name) == normalized)
    {
        return;
    }
    states.push(LinearRequiredState { name, state_type });
}

fn native_mcp_servers_with_default_servers(
    configured: &BTreeMap<String, NativeMcpServerConfig>,
) -> BTreeMap<String, NativeMcpServerConfig> {
    let mut servers = configured.clone();
    servers
        .entry(DEFAULT_PLAYWRIGHT_MCP_SERVER_ID.to_string())
        .or_insert_with(default_playwright_mcp_server);
    servers
}

fn default_playwright_mcp_server() -> NativeMcpServerConfig {
    NativeMcpServerConfig {
        command: DEFAULT_PLAYWRIGHT_MCP_COMMAND
            .iter()
            .map(|part| (*part).to_string())
            .collect(),
        url: None,
        env: BTreeMap::new(),
    }
}

fn native_agents_with_default_mcp_tools(
    agents: &BTreeMap<String, NativeAgentProfile>,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> BTreeMap<String, NativeAgentProfile> {
    agents
        .iter()
        .map(|(name, agent)| {
            let mut agent = agent.clone();
            add_default_agent_mcp_tools(&mut agent, mcp_servers);
            (name.clone(), agent)
        })
        .collect()
}

fn add_default_agent_mcp_tools(
    agent: &mut NativeAgentProfile,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) {
    if mcp_servers.contains_key(DEFAULT_PLAYWRIGHT_MCP_SERVER_ID) {
        let tool = format!("mcp:{DEFAULT_PLAYWRIGHT_MCP_SERVER_ID}");
        if !agent.tools.iter().any(|existing| existing == &tool) {
            agent.tools.push(tool);
        }
    }
}

fn native_mcp_servers_with_linear_scope(
    linear: &NativeLinearConfig,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
) -> BTreeMap<String, NativeMcpServerConfig> {
    let mut scoped = mcp_servers.clone();
    if let Some(project_slug) = linear
        .project_slug
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        for (name, server) in &mut scoped {
            if is_native_linear_mcp_server(name, server) {
                server
                    .env
                    .insert("LINEAR_PROJECT_SLUG".to_string(), project_slug.to_string());
                if linear.kind.as_deref() == Some("jira") {
                    if let Some(api_key) = &linear.api_key {
                        server
                            .env
                            .insert("JIRA_API_TOKEN".to_string(), api_key.clone());
                    }
                    server
                        .env
                        .insert("FUGUE_TRACKER_KIND".to_string(), "jira".to_string());
                    server
                        .env
                        .insert("JIRA_PROJECT_KEY".to_string(), project_slug.to_string());
                    if let Some(endpoint) = &linear.endpoint {
                        server
                            .env
                            .insert("JIRA_BASE_URL".to_string(), endpoint.clone());
                    }
                    if let Some(jira) = &linear.jira {
                        if let Some(email) = &jira.email {
                            server.env.insert("JIRA_EMAIL".to_string(), email.clone());
                        }
                        if let Some(jql) = &jira.jql {
                            server.env.insert("JIRA_JQL".to_string(), jql.clone());
                        }
                        if let Some(path) = &jira.transition_fields_file {
                            server
                                .env
                                .insert("JIRA_TRANSITION_FIELDS_FILE".to_string(), path.clone());
                        }
                        if let Some(path) = &jira.create_fields_file {
                            server
                                .env
                                .insert("JIRA_CREATE_FIELDS_FILE".to_string(), path.clone());
                        }
                        if let Some(issue_type) = &jira.issue_type {
                            server
                                .env
                                .insert("JIRA_ISSUE_TYPE".to_string(), issue_type.clone());
                        }
                        server.env.insert(
                            "JIRA_AUTH_MODE".to_string(),
                            match jira.auth {
                                crate::jira::JiraAuth::Basic => "basic",
                                crate::jira::JiraAuth::Bearer => "bearer",
                            }
                            .to_string(),
                        );
                        server.env.insert(
                            "JIRA_API_VERSION".to_string(),
                            match jira.api_version {
                                crate::jira::JiraApiVersion::V2 => "2",
                                crate::jira::JiraApiVersion::V3 => "3",
                            }
                            .to_string(),
                        );
                    }
                }
            }
        }
    }
    scoped
}

pub(crate) fn is_native_linear_mcp_server(name: &str, server: &NativeMcpServerConfig) -> bool {
    matches!(name, "linear" | "jira")
        || matches!(
            native_mcp_command_basename(server),
            Some("fugue-linear-mcp" | "fugue-jira-mcp")
        )
}

fn native_mcp_command_basename(server: &NativeMcpServerConfig) -> Option<&str> {
    let command = server.command.first()?;
    let command = command.rsplit('/').next().unwrap_or(command);
    Some(command.rsplit('\\').next().unwrap_or(command))
}

fn opt_backend(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<MobExecutionBackend>> {
    opt_string(raw, key.rsplit('.').next().unwrap_or(key))?
        .map(|value| parse_backend(Some(&value), key))
        .transpose()
}

fn parse_backend(value: Option<&str>, field: &str) -> Result<MobExecutionBackend> {
    match value {
        None => Ok(MobExecutionBackend::NativeExecution),
        Some("native-execution" | "native_execution") => Ok(MobExecutionBackend::NativeExecution),
        Some(value) => Err(FugueError::InvalidConfig(format!(
            "{field} must be native-execution, got {value}"
        ))),
    }
}

fn native_config_backend(value: Option<&str>, field: &str) -> Result<MobExecutionBackend> {
    match value {
        None => Ok(MobExecutionBackend::NativeExecution),
        Some(value) => parse_backend(Some(value), field),
    }
}

fn parse_native_flow(value: Option<&str>, field: &str) -> Result<Option<NativeMobFlow>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let flow = match value {
        "solo" => NativeMobFlow::Solo,
        "planner-coder" | "planner_coder" => NativeMobFlow::PlannerCoder,
        "planner-coder-reviewer" | "planner_coder_reviewer" => NativeMobFlow::PlannerCoderReviewer,
        "planner-coder-reviewer-rework" | "planner_coder_reviewer_rework" => {
            NativeMobFlow::PlannerCoderReviewerRework
        }
        other => {
            return Err(FugueError::InvalidConfig(format!(
                "{field} must be solo, planner-coder, planner-coder-reviewer, or planner-coder-reviewer-rework, got {other}"
            )));
        }
    };
    Ok(Some(flow))
}

fn materialize_native_mob_templates(
    agents: &BTreeMap<String, NativeAgentProfile>,
    templates: &BTreeMap<String, NativeMobTemplate>,
    projections: &BTreeMap<String, NativeLinearProjectionConfig>,
    linear: &NativeLinearConfig,
    default_backend: MobExecutionBackend,
) -> Result<BTreeMap<String, MaterializedMobTemplate>> {
    templates
        .iter()
        .map(|(template_name, template)| {
            let backend = template
                .backend
                .as_deref()
                .map(|value| parse_backend(Some(value), &format!("mob_templates.{template_name}.backend")))
                .transpose()?
                .unwrap_or(default_backend);
            if backend != default_backend {
                return Err(FugueError::InvalidConfig(format!(
                    "mob_templates.{template_name}.backend must match the Stage 6 rollout backend {}",
                    default_backend.canonical_name()
                )));
            }
            let members = template
                .members
                .iter()
                .map(|member| {
                    let profile = member.profile_name();
                    let agent = agents.get(profile).ok_or_else(|| {
                        FugueError::InvalidConfig(format!(
                            "mob_templates.{template_name}.members references unknown agent profile {profile:?}"
                        ))
                    })?;
                    Ok(MaterializedMobMember {
                        name: member.member_name().to_string(),
                        profile: profile.to_string(),
                        agent: agent.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let flow = parse_native_flow(
                template.flow.as_deref(),
                &format!("mob_templates.{template_name}.flow"),
            )?;
            let linear_projection =
                materialize_linear_projection(template_name, template, flow, projections, linear)?;
            Ok((
                template_name.clone(),
                MaterializedMobTemplate {
                    backend,
                    members,
                    flow,
                    linear_projection,
                    max_review_cycles: template.max_review_cycles,
                },
            ))
        })
        .collect()
}

fn materialize_linear_projection(
    template_name: &str,
    template: &NativeMobTemplate,
    flow: Option<NativeMobFlow>,
    projections: &BTreeMap<String, NativeLinearProjectionConfig>,
    linear: &NativeLinearConfig,
) -> Result<Option<MaterializedLinearProjection>> {
    let projection_id = template.linear_projection.as_deref();
    if let Some(projection_id) = projection_id {
        let projection = projections.get(projection_id).ok_or_else(|| {
            FugueError::InvalidConfig(format!(
                "mob_templates.{template_name}.linear_projection references missing linear_projections.{projection_id}"
            ))
        })?;
        return Ok(Some(MaterializedLinearProjection {
            id: projection_id.to_string(),
            node_lanes: projection_node_lanes(projection, linear),
            node_phase_labels: projection_phase_labels(projection),
            aggregate: projection_aggregate(projection, linear),
            terminal_lanes: projection_terminal_lanes(projection, linear),
        }));
    }
    if flow.is_some() {
        return Err(FugueError::InvalidConfig(format!(
            "mob_templates.{template_name}.linear_projection is required for native flow {}",
            flow.map(|flow| flow.canonical_name())
                .unwrap_or("unspecified")
        )));
    }
    Ok(None)
}

fn projection_node_lanes(
    projection: &NativeLinearProjectionConfig,
    linear: &NativeLinearConfig,
) -> BTreeMap<String, String> {
    let Some(active_lane) = linear.lanes.active.as_ref() else {
        return projection.node_lanes.clone();
    };
    projection
        .node_lanes
        .keys()
        .map(|node| (node.clone(), active_lane.clone()))
        .collect()
}

fn projection_phase_labels(projection: &NativeLinearProjectionConfig) -> BTreeMap<String, String> {
    let mut labels = projection.node_phase_labels.clone();
    for node in projection.node_lanes.keys() {
        labels.entry(node.clone()).or_insert_with(|| {
            format!(
                "fugue:phase:{}",
                node.replace('_', "-").to_ascii_lowercase()
            )
        });
    }
    labels
}

fn projection_aggregate(
    projection: &NativeLinearProjectionConfig,
    linear: &NativeLinearConfig,
) -> NativeLinearProjectionAggregate {
    let Some(active_lane) = linear.lanes.active.as_ref() else {
        return projection.aggregate.clone();
    };
    NativeLinearProjectionAggregate {
        precedence: vec![active_lane.clone()],
        fallback: Some(active_lane.clone()),
    }
}

fn projection_terminal_lanes(
    projection: &NativeLinearProjectionConfig,
    linear: &NativeLinearConfig,
) -> NativeLinearProjectionTerminals {
    let Some(active_lane) = linear.lanes.active.as_ref() else {
        return projection.terminal_lanes.clone();
    };
    let mut terminals = projection.terminal_lanes.clone();
    if projection
        .node_lanes
        .values()
        .any(|lane| normalize_state(lane) == normalize_state(&terminals.failed))
    {
        terminals.failed = active_lane.clone();
    }
    terminals
}

fn validate_linear_config(linear: &NativeLinearConfig) -> Result<()> {
    if linear.kind.as_deref() == Some("jira") {
        // Existing Jira workflows may use one intake/backlog status and one
        // terminal status for several semantic outcomes. Runtime outcomes remain typed.
        let lanes = &linear.lanes;
        let mut seen = BTreeMap::new();
        validate_linear_lane_anchor(&mut seen, "ready", &lanes.ready)?;
        if normalize_state(&lanes.parked) != normalize_state(&lanes.ready) {
            validate_linear_lane_anchor(&mut seen, "parked", &lanes.parked)?;
        }
        let mut terminal_names = BTreeSet::new();
        for (role, value) in [
            ("success", &lanes.success),
            ("canceled", &lanes.canceled),
            ("duplicate", &lanes.duplicate),
        ] {
            if terminal_names.insert(normalize_state(value)) {
                validate_linear_lane_anchor(&mut seen, role, value)?;
            }
        }
        for (role, value) in [
            ("active", &lanes.active),
            ("review_ready", &lanes.review_ready),
            ("rework", &lanes.rework),
            ("do_review", &lanes.do_review),
            ("deploy", &lanes.deploy),
            ("blocked", &lanes.blocked),
        ] {
            if let Some(value) = value {
                validate_linear_lane_anchor(&mut seen, role, value)?;
            }
        }
        return Ok(());
    }
    validate_linear_lane_anchors(&linear.lanes)
}

fn validate_linear_lane_anchors(lanes: &NativeLinearLanes) -> Result<()> {
    let meanings = [
        ("parked", lanes.parked.as_str()),
        ("ready", lanes.ready.as_str()),
        ("success", lanes.success.as_str()),
        ("canceled", lanes.canceled.as_str()),
        ("duplicate", lanes.duplicate.as_str()),
    ];
    let mut seen = BTreeMap::<String, (&str, String)>::new();
    for (meaning, state) in meanings {
        validate_linear_lane_anchor(&mut seen, meaning, state)?;
    }
    if let Some(review_ready) = &lanes.review_ready {
        validate_linear_lane_anchor(&mut seen, "review_ready", review_ready)?;
    }
    if let Some(rework) = &lanes.rework {
        validate_linear_lane_anchor(&mut seen, "rework", rework)?;
    }
    if let Some(active) = &lanes.active {
        validate_linear_lane_anchor(&mut seen, "active", active)?;
    }
    if let Some(do_review) = &lanes.do_review {
        validate_linear_lane_anchor(&mut seen, "do_review", do_review)?;
    }
    if let Some(deploy) = &lanes.deploy {
        validate_linear_lane_anchor(&mut seen, "deploy", deploy)?;
    }
    if let Some(blocked) = &lanes.blocked {
        validate_linear_lane_anchor(&mut seen, "blocked", blocked)?;
    }
    Ok(())
}

fn validate_linear_lane_anchor<'a>(
    seen: &mut BTreeMap<String, (&'a str, String)>,
    meaning: &'a str,
    state: &str,
) -> Result<()> {
    if state.trim().is_empty() {
        return Err(FugueError::InvalidConfig(format!(
            "linear.lanes.{meaning} must be non-empty"
        )));
    }
    let normalized = normalize_state(state);
    if let Some((existing_meaning, existing_state)) =
        seen.insert(normalized, (meaning, state.to_string()))
    {
        return Err(FugueError::InvalidConfig(format!(
            "linear.lanes.{meaning} {state:?} conflicts with linear.lanes.{existing_meaning} {existing_state:?}"
        )));
    }
    Ok(())
}

fn validate_linear_projections(
    projections: &BTreeMap<String, NativeLinearProjectionConfig>,
) -> Result<()> {
    for (name, projection) in projections {
        if name.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "linear_projections contains an empty projection name".to_string(),
            ));
        }
        if projection.node_lanes.is_empty() {
            return Err(FugueError::InvalidConfig(format!(
                "linear_projections.{name}.node_lanes must not be empty"
            )));
        }
        for (node, lane) in &projection.node_lanes {
            if node.trim().is_empty() || lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "linear_projections.{name}.node_lanes contains an empty node or lane"
                )));
            }
        }
        for lane in &projection.aggregate.precedence {
            if lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "linear_projections.{name}.aggregate.precedence contains an empty lane"
                )));
            }
        }
        if projection
            .aggregate
            .fallback
            .as_deref()
            .is_some_and(|lane| lane.trim().is_empty())
        {
            return Err(FugueError::InvalidConfig(format!(
                "linear_projections.{name}.aggregate.fallback must be non-empty when set"
            )));
        }
        for (terminal, lane) in [
            ("succeeded", projection.terminal_lanes.succeeded.as_str()),
            ("failed", projection.terminal_lanes.failed.as_str()),
            ("cancelled", projection.terminal_lanes.cancelled.as_str()),
        ] {
            if lane.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "linear_projections.{name}.terminal_lanes.{terminal} must be non-empty"
                )));
            }
        }
    }
    Ok(())
}

fn validate_labels(labels: &[String], by_label: &BTreeMap<String, String>) -> Result<()> {
    let mut declared = BTreeSet::new();
    for label in labels {
        if label.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "linear.labels contains an empty label".to_string(),
            ));
        }
        let normalized = normalize_state(label);
        if !declared.insert(normalized) {
            return Err(FugueError::InvalidConfig(format!(
                "linear.labels contains duplicate label {label:?}"
            )));
        }
    }
    for label in by_label.keys() {
        if !declared.contains(&normalize_state(label)) {
            return Err(FugueError::InvalidConfig(format!(
                "selection.by_label references undeclared Linear label {label:?}"
            )));
        }
    }
    Ok(())
}

fn validate_agent_profiles(
    agents: &BTreeMap<String, NativeAgentProfile>,
    mcp_servers: &BTreeMap<String, NativeMcpServerConfig>,
    tracker_kind: Option<&str>,
) -> Result<()> {
    if agents.is_empty() {
        return Err(FugueError::InvalidConfig(
            "agents must declare at least one profile".to_string(),
        ));
    }
    for (name, agent) in agents {
        if name.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "agents contains an empty profile name".to_string(),
            ));
        }
        if let Some(provider) = &agent.provider {
            serde_json::from_value::<meerkat_core::Provider>(Value::String(provider.clone()))
                .map_err(|error| {
                    FugueError::InvalidConfig(format!("agents.{name}.provider: {error}"))
                })?;
        }
        if let Some(params) = &agent.provider_params {
            serde_json::from_value::<meerkat_core::lifecycle::run_primitive::ProviderParamsOverride>(Value::Object(params.clone()))
                .map_err(|error| FugueError::InvalidConfig(format!("agents.{name}.provider_params: {error}")))?;
        }
        if let Some(schema) = &agent.output_schema {
            serde_json::from_value::<meerkat_core::MeerkatSchema>(Value::Object(schema.clone()))
                .map_err(|error| {
                    FugueError::InvalidConfig(format!("agents.{name}.output_schema: {error}"))
                })?;
        }
        for (field, value) in [
            ("max_tokens", agent.max_tokens),
            ("structured_output_retries", agent.structured_output_retries),
        ] {
            if value.is_some_and(|value| value > u64::from(u32::MAX)) {
                return Err(FugueError::InvalidConfig(format!(
                    "agents.{name}.{field} exceeds u32"
                )));
            }
        }
        for tool in &agent.tools {
            if matches!(tool.as_str(), "linear" | "jira" | "github") {
                return Err(FugueError::InvalidConfig(format!(
                    "agents.{name}.tools uses {tool:?}, but repo tools must be packaged as MCP servers and referenced as mcp:{tool}"
                )));
            }
            if let Some(server) = tool.strip_prefix("mcp:")
                && !mcp_servers.contains_key(server)
                && !(tracker_kind == Some("fugue-ims") && server == "linear")
            {
                return Err(FugueError::InvalidConfig(format!(
                    "agents.{name}.tools references unknown MCP server {server:?}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_mob_templates(
    agents: &BTreeMap<String, NativeAgentProfile>,
    templates: &BTreeMap<String, NativeMobTemplate>,
    projections: &BTreeMap<String, NativeLinearProjectionConfig>,
    linear: &NativeLinearConfig,
    default_backend: MobExecutionBackend,
) -> Result<()> {
    if templates.is_empty() {
        return Err(FugueError::InvalidConfig(
            "mob_templates must declare at least one template".to_string(),
        ));
    }
    for (template_name, template) in templates {
        if template_name.trim().is_empty() {
            return Err(FugueError::InvalidConfig(
                "mob_templates contains an empty template name".to_string(),
            ));
        }
        if let Some(backend_value) = template.backend.as_deref() {
            let template_backend = parse_backend(
                Some(backend_value),
                &format!("mob_templates.{template_name}.backend"),
            )?;
            if template_backend != default_backend {
                return Err(FugueError::InvalidConfig(format!(
                    "mob_templates.{template_name}.backend must match the Stage 6 rollout backend {}",
                    default_backend.canonical_name()
                )));
            }
        }
        let flow = parse_native_flow(
            template.flow.as_deref(),
            &format!("mob_templates.{template_name}.flow"),
        )?;
        materialize_linear_projection(template_name, template, flow, projections, linear)?;
        if matches!(flow, Some(NativeMobFlow::PlannerCoderReviewer))
            && template.max_review_cycles.unwrap_or(0) == 0
        {
            return Err(FugueError::InvalidConfig(format!(
                "mob_templates.{template_name}.max_review_cycles must be positive for rework loops"
            )));
        }
        if template.members.is_empty() {
            return Err(FugueError::InvalidConfig(format!(
                "mob_templates.{template_name}.members must not be empty"
            )));
        }
        let mut member_names = BTreeSet::new();
        for member in &template.members {
            let member_name = member.member_name();
            let profile_name = member.profile_name();
            if member_name.trim().is_empty() || profile_name.trim().is_empty() {
                return Err(FugueError::InvalidConfig(format!(
                    "mob_templates.{template_name}.members contains an empty member/profile reference"
                )));
            }
            if !member_names.insert(member_name.to_string()) {
                return Err(FugueError::InvalidConfig(format!(
                    "mob_templates.{template_name}.members contains duplicate member {member_name:?}"
                )));
            }
            if !agents.contains_key(profile_name) {
                return Err(FugueError::InvalidConfig(format!(
                    "mob_templates.{template_name}.members references unknown agent profile {profile_name:?}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_selection(
    selection: &NativeSelectionConfig,
    templates: &BTreeMap<String, NativeMobTemplate>,
) -> Result<()> {
    if !templates.contains_key(&selection.default_template) {
        return Err(FugueError::InvalidConfig(format!(
            "selection.default_template references unknown mob template {:?}",
            selection.default_template
        )));
    }
    for template in selection.by_label.values() {
        if !templates.contains_key(template) {
            return Err(FugueError::InvalidConfig(format!(
                "selection.by_label references unknown mob template {template:?}"
            )));
        }
    }
    Ok(())
}

fn to_json_object<T: Serialize>(value: &T) -> Result<Map<String, Value>> {
    serde_json::to_value(value)
        .map_err(|err| FugueError::InvalidConfig(err.to_string()))?
        .as_object()
        .cloned()
        .ok_or_else(|| {
            FugueError::InvalidConfig(
                "native config section must serialize to an object".to_string(),
            )
        })
}

fn insert_profile_default(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if !map.contains_key(key)
        && let Some(value) = value
    {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn insert_profile_default_u64(map: &mut Map<String, Value>, key: &str, value: Option<u64>) {
    if !map.contains_key(key)
        && let Some(value) = value
    {
        map.insert(key.to_string(), Value::from(value));
    }
}

fn insert_profile_default_object(
    map: &mut Map<String, Value>,
    key: &str,
    value: Option<Map<String, Value>>,
) {
    if !map.contains_key(key)
        && let Some(value) = value
    {
        map.insert(key.to_string(), Value::Object(value));
    }
}

pub fn render_prompt(template: &str, issue: &Issue, attempt: Option<u32>) -> Result<String> {
    let body = if template.trim().is_empty() {
        "You are working on an issue from Linear."
    } else {
        template
    };
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env.add_template("prompt", body)
        .map_err(|err| FugueError::TemplateParseError(err.to_string()))?;
    let template = env
        .get_template("prompt")
        .map_err(|err| FugueError::TemplateParseError(err.to_string()))?;
    template
        .render(context! {
            issue => issue,
            attempt => attempt,
        })
        .map(|rendered| rendered.trim().to_string())
        .map_err(|err| FugueError::TemplateRenderError(err.to_string()))
}

fn section_object<'a>(
    root: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Map<String, Value>>> {
    match root.get(key) {
        None => Ok(None),
        Some(Value::Object(object)) => Ok(Some(object)),
        Some(_) => Err(FugueError::InvalidConfig(format!(
            "{key} must be an object"
        ))),
    }
}

fn opt_value<'a>(raw: Option<&'a Map<String, Value>>, key: &str) -> Option<&'a Value> {
    raw.and_then(|obj| obj.get(key))
}

fn opt_object_field(
    raw: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<Map<String, Value>>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_type(key, "an object")),
    }
}

fn opt_string(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<String>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_type(key, "a string")),
    }
}

fn opt_u64(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<u64>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| invalid_type(key, "a non-negative integer")),
        Some(_) => Err(invalid_type(key, "a non-negative integer")),
    }
}

fn opt_usize(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<usize>> {
    opt_u64(raw, key)?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| FugueError::InvalidConfig(format!("{key} is too large")))
        })
        .transpose()
}

fn opt_i64(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<i64>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .map(Some)
            .ok_or_else(|| invalid_type(key, "an integer")),
        Some(_) => Err(invalid_type(key, "an integer")),
    }
}

fn opt_bool(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<bool>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(invalid_type(key, "a boolean")),
    }
}

fn opt_string_list(raw: Option<&Map<String, Value>>, key: &str) -> Result<Option<Vec<String>>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or_else(|| invalid_type(key, "a list of strings"))
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(_) => Err(invalid_type(key, "a list of strings")),
    }
}

fn opt_string_map(
    raw: Option<&Map<String, Value>>,
    key: &str,
) -> Result<Option<BTreeMap<String, String>>> {
    match opt_value(raw, key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(values)) => values
            .iter()
            .map(|(map_key, value)| {
                value
                    .as_str()
                    .map(|value| (map_key.clone(), value.to_string()))
                    .ok_or_else(|| invalid_type(key, "a string map"))
            })
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Some),
        Some(_) => Err(invalid_type(key, "a string map")),
    }
}

fn invalid_type(key: &str, expected: &str) -> FugueError {
    FugueError::InvalidConfig(format!("{key} must be {expected}"))
}

fn resolve_env_string(value: &str) -> Option<String> {
    if let Some(var) = value.strip_prefix('$')
        && !var.is_empty()
        && var
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return env::var(var).ok().filter(|value| !value.is_empty());
    }
    Some(value.to_string())
}

fn path_field(
    raw: Option<&Map<String, Value>>,
    key: &str,
    workflow_dir: &Path,
) -> Result<Option<PathBuf>> {
    opt_string(raw, key)?
        .map(|value| resolve_path_value(&value, workflow_dir))
        .transpose()
}

fn resolve_path_value(value: &str, workflow_dir: &Path) -> Result<PathBuf> {
    let resolved = resolve_env_string(value).ok_or_else(|| {
        FugueError::InvalidConfig(format!("path value {value} resolved to an empty env var"))
    })?;
    let expanded = expand_tilde(&resolved);
    Ok(absolute_clean_path(workflow_dir, &expanded))
}

fn expand_tilde(value: &str) -> PathBuf {
    if value == "~" {
        env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(value))
    } else if let Some(rest) = value.strip_prefix("~/") {
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(rest))
            .unwrap_or_else(|| PathBuf::from(value))
    } else {
        PathBuf::from(value)
    }
}

pub fn absolute_clean_path(base: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    clean_path(&joined)
}

fn clean_path(path: &Path) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                cleaned.pop();
            }
            other => cleaned.push(other.as_os_str()),
        }
    }
    cleaned
}

fn parse_auth_binding(value: &Value) -> Result<AuthBindingRef> {
    if let Some(s) = value.as_str() {
        if let Some(resolved) = resolve_env_string(s) {
            if resolved == s {
                return Err(FugueError::InvalidConfig(
                    "meerkat.auth_binding must be an object, not a string".to_string(),
                ));
            }
            let parsed: Value = match serde_json::from_str(&resolved) {
                Ok(value) => value,
                Err(json_err) => {
                    let yaml_value: serde_yaml::Value = serde_yaml::from_str(&resolved).map_err(|yaml_err| {
                        FugueError::InvalidConfig(format!(
                            "meerkat.auth_binding env value is not JSON ({json_err}) or YAML ({yaml_err})"
                        ))
                    })?;
                    serde_json::to_value(yaml_value).map_err(|err| {
                        FugueError::InvalidConfig(format!(
                            "meerkat.auth_binding env YAML could not be converted: {err}"
                        ))
                    })?
                }
            };
            return parse_auth_binding(&parsed);
        }
        return Err(FugueError::InvalidConfig(
            "meerkat.auth_binding must be an object, not a string".to_string(),
        ));
    }
    serde_json::from_value(value.clone()).map_err(|error| {
        FugueError::InvalidConfig(format!("invalid meerkat.auth_binding: {error}"))
    })
}

fn parse_preload_skills(value: &Value) -> Result<Vec<SkillKey>> {
    let values = value.as_array().ok_or_else(|| {
        FugueError::InvalidConfig("meerkat.preload_skills must be a list".to_string())
    })?;
    values
        .iter()
        .map(|item| {
            let object = item.as_object().ok_or_else(|| {
                FugueError::InvalidConfig(
                    "meerkat.preload_skills entries must be objects".to_string(),
                )
            })?;
            let source_uuid =
                required_string_value(object, "source_uuid", "meerkat.preload_skills")?;
            let skill_name = required_string_value(object, "skill_name", "meerkat.preload_skills")?;
            Ok(SkillKey {
                source_uuid,
                skill_name,
            })
        })
        .collect()
}

fn parse_skill_refs(value: &Value) -> Result<Vec<SkillRef>> {
    let values = value.as_array().ok_or_else(|| {
        FugueError::InvalidConfig("meerkat.skill_refs must be a list".to_string())
    })?;
    values
        .iter()
        .map(|item| {
            let object = item.as_object().ok_or_else(|| {
                FugueError::InvalidConfig(
                    "meerkat.skill_refs entries must be tagged objects".to_string(),
                )
            })?;
            if object.get("kind").and_then(Value::as_str) != Some("structured") {
                return Err(FugueError::InvalidConfig(
                    "meerkat.skill_refs entries must have kind=structured".to_string(),
                ));
            }
            let source_uuid = required_string_value(object, "source_uuid", "meerkat.skill_refs")?;
            let skill_name = required_string_value(object, "skill_name", "meerkat.skill_refs")?;
            Ok(SkillRef::Structured {
                source_uuid,
                skill_name,
            })
        })
        .collect()
}

fn required_string_value(object: &Map<String, Value>, key: &str, owner: &str) -> Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| FugueError::InvalidConfig(format!("{owner}.{key} must be a string")))
}

fn append_flag(command: &mut String, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        command.push(' ');
        command.push_str(flag);
        command.push(' ');
        command.push_str(&shell_quote(value));
    }
}

fn append_flag_path(command: &mut String, flag: &str, value: Option<&Path>) {
    if let Some(value) = value.and_then(Path::to_str) {
        append_flag(command, flag, Some(value));
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn insert_opt_string(params: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        params.insert(key.to_string(), Value::String(value));
    }
}

fn insert_opt_u64(params: &mut Map<String, Value>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        params.insert(key.to_string(), Value::from(value));
    }
}

fn insert_opt_bool(params: &mut Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        params.insert(key.to_string(), Value::Bool(value));
    }
}

fn insert_opt_object(
    params: &mut Map<String, Value>,
    key: &str,
    value: Option<Map<String, Value>>,
) {
    if let Some(value) = value {
        params.insert(key.to_string(), Value::Object(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Issue, IssueAttachment};

    fn tracker_binding_workflow() -> String {
        include_str!("../fugue.jira.example.yaml")
            .replace("$JIRA_BASE_URL", "https://first.example.com")
            .replace("$JIRA_API_TOKEN", "fixture-token")
            .replace("$JIRA_EMAIL", "fixture@example.com")
    }

    #[test]
    fn tracker_binding_rejects_identity_change_on_reload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fugue.yaml");
        let original = tracker_binding_workflow();
        fs::write(&path, &original).unwrap();
        let mut manager = WorkflowConfigManager::load(path.clone()).unwrap();
        let original_context = manager.effective().tracker_context_id();
        for replacement in [
            original.replace("https://first.example.com", "https://second.example.com"),
            original.replace("project_slug: EXAMPLE", "project_slug: OTHER"),
        ] {
            fs::write(&path, replacement).unwrap();
            manager.last_modified = None;
            assert!(!manager.reload_if_changed());
            assert_eq!(manager.effective().tracker_context_id(), original_context);
            assert!(manager.last_reload_error().is_some());
        }
        fs::write(&path, original.replace("fixture-token", "rotated-token")).unwrap();
        manager.last_modified = None;
        assert!(manager.reload_if_changed());
        assert_eq!(manager.effective().tracker_context_id(), original_context);
        assert_eq!(
            manager.effective().tracker.api_key.as_deref(),
            Some("rotated-token")
        );
        assert!(manager.last_reload_error().is_none());
    }

    #[test]
    fn jira_example_materializes_scope_authentication_and_existing_statuses() {
        let source = include_str!("../fugue.jira.example.yaml")
            .replace("$JIRA_BASE_URL", "https://jira.example.com")
            .replace("$JIRA_API_TOKEN", "fixture-token")
            .replace("$JIRA_EMAIL", "fixture@example.com");
        let path = PathBuf::from("/tmp/fugue.yaml");
        let definition = parse_native_workflow(&path, &source).expect("Jira example");
        let config = ServiceConfig::from_definition(path, &definition).expect("service config");
        config
            .validate_dispatch()
            .expect("Jira dispatch configuration");
        assert_eq!(config.tracker.kind, "jira");
        assert_eq!(
            config
                .tracker
                .jira
                .as_ref()
                .expect("Jira config")
                .jql
                .as_deref(),
            Some("labels = \"fugue\"")
        );
        assert_eq!(
            config.mob.mcp_servers["jira"].env["JIRA_PROJECT_KEY"],
            "EXAMPLE"
        );
        assert_eq!(
            config.mob.mcp_servers["jira"].env["FUGUE_TRACKER_KIND"],
            "jira"
        );
        assert_eq!(config.linear.lanes.active.as_deref(), Some("In Progress"));
        assert!(
            crate::linear_lane_state::legacy_linear_lane_repair(&config, "In Progress").is_none()
        );
        let repo = &config
            .kapellmeister
            .shadow_mode
            .authority
            .safe_selectors
            .repositories[0];
        assert_eq!(repo.hostname(), "git.example.com");
    }

    #[test]
    fn repository_allowlists_distinguish_github_hosts() {
        let enterprise: GitHubRepoRef = serde_json::from_value(
            json!({"host":"git.example.com","owner":"example","name":"app"}),
        )
        .expect("enterprise repo");
        let public: GitHubRepoRef =
            serde_json::from_value(json!({"owner":"example","name":"app"})).expect("public repo");
        let config = DeployLaneConfig {
            allowed_repositories: vec![enterprise.clone()],
            ..Default::default()
        };
        assert!(config.allows_repository(&enterprise));
        assert!(!config.allows_repository(&public));
    }

    #[test]
    fn native_config_defaults_apply() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(config.polling.interval_ms, 30_000);
        assert_eq!(config.agent.max_turns, 20);
        assert_eq!(config.meerkat.command, "rkat-rpc");
        assert_eq!(config.mob.backend, MobExecutionBackend::NativeExecution);
        assert!(config.mob.native.enabled);
        assert!(!config.mob.native.shadow);
        assert_eq!(
            config.meerkat.model.as_deref(),
            Some(default_agent_model()),
            "meerkat.model should use the Meerkat catalog default when unspecified"
        );
        assert_eq!(config.tracker.active_states, vec!["Todo", "Active"]);
    }

    #[test]
    fn final_review_config_parses_defaults_and_custom_lanes() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "final_review": {
                    "enabled": true,
                    "mode": "manual",
                    "lane": "Repo Gate",
                    "rework_lane": "Needs Repo Rework",
                    "blocked_lane": "Needs Human",
                    "success_lane": "Merged",
                    "merge": {"strategy": "rebase", "delete_branch": false},
                    "reviewers": [
                        {"id": "architecture", "profile": "final_architecture_reviewer", "required": true},
                        {"id": "security", "profile": "final_security_reviewer", "required": false}
                    ]
                }
            }).as_object().unwrap().clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert!(config.final_review.enabled);
        assert_eq!(config.final_review.mode, FinalReviewMode::Manual);
        assert_eq!(config.final_review.lane, "Repo Gate");
        assert_eq!(config.final_review.rework_lane, "Needs Repo Rework");
        assert_eq!(config.final_review.blocked_lane, "Needs Human");
        assert_eq!(config.final_review.success_lane, "Merged");
        assert_eq!(
            config.final_review.merge.strategy,
            FinalReviewMergeStrategy::Rebase
        );
        assert!(!config.final_review.merge.delete_branch);
        assert!(!config.final_review.reviewers[1].required);
    }

    #[test]
    fn final_review_disabled_defaults_materialize_example_lane_names_only_in_config() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert!(!config.final_review.enabled);
        assert_eq!(config.final_review.mode, FinalReviewMode::Manual);
        assert_eq!(config.final_review.lane, "Final Review & Merge");
        assert_eq!(config.final_review.rework_lane, "Active");
        assert_eq!(config.final_review.blocked_lane, "Blocked");
        assert_eq!(config.final_review.success_lane, "Done");
        assert_eq!(
            config.final_review.merge.strategy,
            FinalReviewMergeStrategy::Squash
        );
        assert!(config.final_review.reviewers.is_empty());
    }

    #[test]
    fn deploy_lane_config_parses_repo_limited_policy() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "deploy": {
                    "enabled": true,
                    "action_lane": "Deploy",
                    "procedure": ".fugue/deploy.md",
                    "ready_lane": "Ready for review",
                    "rework_lane": "Rework",
                    "blocked_lane": "Blocked",
                    "success_lane": "Done",
                    "allowed_repositories": [
                        {"owner": "lukacf", "name": "meerkat-fugue"}
                    ]
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert!(config.deploy.enabled);
        assert_eq!(config.deploy.lane, "Deploy");
        assert_eq!(config.deploy.procedure, ".fugue/deploy.md");
        assert_eq!(config.deploy.allowed_repositories.len(), 1);
        assert!(config.deploy.require_ci_success);
        assert!(config.deploy.require_fresh_head);
        assert!(config.deploy.require_no_blocking_workgraph);
        assert!(config.deploy.require_ota_update);
        assert!(config.deploy.allows_repository(&GitHubRepoRef {
            host: None,
            owner: GitHubOwner("LUKACF".to_string()),
            name: GitHubRepoName("meerkat-fugue".to_string()),
        }));
        assert!(!config.deploy.allows_repository(&GitHubRepoRef {
            host: None,
            owner: GitHubOwner("lukacf".to_string()),
            name: GitHubRepoName("other".to_string()),
        }));
    }

    #[test]
    fn deploy_success_cannot_share_the_blocked_lane() {
        let config = DeployLaneConfig {
            success_lane: "Closed".into(),
            blocked_lane: " closed ".into(),
            ..Default::default()
        };
        assert!(
            config
                .validate_static()
                .unwrap_err()
                .to_string()
                .contains("deploy.success_lane and deploy.blocked_lane must differ")
        );
    }

    #[test]
    fn deploy_lane_config_requires_repository_when_enabled() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "deploy": {"enabled": true}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let err = ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
            .unwrap_err()
            .to_string();

        assert!(err.contains("deploy.enabled requires at least one allowed repository"));
    }

    #[test]
    fn independent_review_config_parses_exact_three_reviewer_policy() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "independent_review": {
                    "enabled": true,
                    "lane": "Review",
                    "success_lane": "Ready for review",
                    "rework_lane": "Rework",
                    "blocked_lane": "Blocked",
                    "orchestrator_profile": "review_orchestrator",
                    "reviewer_count": 3,
                    "timeout_ms": 1200000,
                    "findings_comment_marker": "Fugue independent review findings"
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert!(config.independent_review.enabled);
        assert_eq!(config.independent_review.lane, "Review");
        assert_eq!(
            config.independent_review.orchestrator_profile,
            "review_orchestrator"
        );
        assert_eq!(config.independent_review.reviewer_count, 3);
        assert_eq!(config.independent_review.timeout_ms, 1_200_000);
        assert!(config.independent_review.require_all_green);
    }

    #[test]
    fn independent_review_reviewer_policy_is_explicit_and_preserves_old_defaults() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "independent_review": {"enabled": true, "reviewer_count": 2}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(config.independent_review.reviewer_count, 2);
        for count in [1, 2, 3, 255] {
            let policy = IndependentReviewConfig {
                reviewer_count: count,
                enabled: true,
                ..Default::default()
            };
            assert!(policy.validate_static().is_ok());
        }
        assert!(
            IndependentReviewConfig {
                reviewer_count: 0,
                ..Default::default()
            }
            .validate_static()
            .is_err()
        );
        assert_eq!(
            serde_json::from_value::<IndependentReviewConfig>(json!({"enabled":true}))
                .unwrap()
                .reviewer_count,
            3
        );
        assert_eq!(
            serde_json::from_value::<FinalReviewConfig>(json!({}))
                .unwrap()
                .minimum_reviewers,
            3
        );
        assert!(
            FinalReviewConfig {
                minimum_reviewers: 0,
                ..Default::default()
            }
            .validate_static()
            .is_err()
        );
    }

    #[test]
    fn review_only_dispatch_requires_explicit_repository_scope_but_not_deployment() {
        let native = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            include_str!("../fugue.jira.example.yaml"),
        )
        .unwrap();
        for review in ["independent_review", "final_review"] {
            let mut raw = json!({
                "tracker":{"kind":"fugue-ims", "project_slug":"example/app", "endpoint":"/tmp/review-policy.sqlite"},
                "deploy":{"enabled":false}
            });
            raw["mob"] = native.config["mob"].clone();
            raw[review] = if review == "independent_review" {
                json!({"enabled":true})
            } else {
                json!({"enabled":true,"reviewers":[{"id":"domain","profile":"domain","required":true}]})
            };
            let definition = WorkflowDefinition {
                config: raw.as_object().unwrap().clone(),
                prompt_template: "test".into(),
            };
            let config =
                ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
                    .unwrap();
            assert!(
                config
                    .validate_dispatch()
                    .unwrap_err()
                    .to_string()
                    .contains("deploy.allowed_repositories")
            );
            raw["deploy"]["allowed_repositories"] = json!([{"owner":"example","name":"app"}]);
            let definition = WorkflowDefinition {
                config: raw.as_object().unwrap().clone(),
                prompt_template: "test".into(),
            };
            let config =
                ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
                    .unwrap();
            assert!(!config.deploy.enabled);
            config.validate_dispatch().unwrap();
        }
    }

    #[test]
    fn native_config_materializes_deploy_and_review_control_lanes() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
linear:
  labels: []
  lanes:
    rework: Rework
    do_review: Review
    deploy: Deploy
    review_ready: Ready for review
    blocked: Blocked
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
deploy:
  enabled: true
  allowed_repositories:
    - owner: lukacf
      name: meerkat-fugue
independent_review:
  enabled: true
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.linear.lanes.rework.as_deref(), Some("Rework"));
        assert_eq!(config.linear.lanes.do_review.as_deref(), Some("Review"));
        assert_eq!(config.linear.lanes.deploy.as_deref(), Some("Deploy"));
        assert!(config.tracker.intake_states.contains(&"Rework".to_string()));
        assert!(config.tracker.active_states.contains(&"Review".to_string()));
        assert!(config.tracker.active_states.contains(&"Deploy".to_string()));
        assert!(
            config
                .linear
                .required_states
                .iter()
                .any(|state| state.name == "Review")
        );
        assert!(
            config
                .linear
                .required_states
                .iter()
                .any(|state| state.name == "Deploy")
        );
    }

    #[test]
    fn ota_update_config_parses_defaults_and_validates_enabled_commands() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "ota_update": {
                    "enabled": true,
                    "repo_path": ".",
                    "build_command": "cargo build --release",
                    "health_check_command": "cargo test --no-run",
                    "lock_path": "./ota.lock",
                    "trigger_token_env": "FUGUE_OTA_TRIGGER_TOKEN",
                    "deploy_command": "systemctl restart meerkat-fugue"
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let ota_update = config.ota_update.expect("ota_update configured");
        assert!(ota_update.enabled);
        assert_eq!(ota_update.branch, "main");
        assert_eq!(ota_update.remote, "origin");
        assert_eq!(ota_update.timeout_ms, 600_000);
        assert!(ota_update.require_clean);
        assert_eq!(ota_update.repo_path, PathBuf::from("/tmp"));
        assert_eq!(
            ota_update.health_check_command.as_deref(),
            Some("cargo test --no-run")
        );
        assert_eq!(ota_update.lock_path, Some(PathBuf::from("/tmp/ota.lock")));
        assert_eq!(
            ota_update.trigger_token_env.as_deref(),
            Some("FUGUE_OTA_TRIGGER_TOKEN")
        );

        let invalid = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "ota_update": {"enabled": true, "build_command": "cargo build"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let err = ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &invalid)
            .unwrap_err()
            .to_string();
        assert!(err.contains("ota_update.deploy_command"));
    }

    #[test]
    fn explicit_meerkat_model_overrides_default() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "meerkat": {"model": "claude-sonnet-4-6"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(config.meerkat.model.as_deref(), Some("claude-sonnet-4-6"));
    }

    #[test]
    fn server_meta_console_defaults_when_status_server_enabled() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {"port": 0}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let server = config.server.expect("server config should be present");

        assert_eq!(server.port, Some(0));
        assert!(server.meta_console.enabled);
        assert_eq!(server.meta_console.path, "/console");
        assert_eq!(server.meta_console.rpc_timeout_ms, 30_000);
        assert!(server.meta_console.basic_auth.is_none());
    }

    #[test]
    fn server_meta_console_supports_basic_auth() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {
                    "port": 0,
                    "meta_console": {
                        "basic_auth": {
                            "username_env": "FUGUE_CONSOLE_USER",
                            "password_env": "FUGUE_CONSOLE_PASSWORD",
                            "realm": "Fugue Console"
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
        let server = config.server.expect("server config should be present");
        let auth = server
            .meta_console
            .basic_auth
            .expect("basic auth should be configured");

        assert_eq!(auth.username_env, "FUGUE_CONSOLE_USER");
        assert_eq!(auth.password_env, "FUGUE_CONSOLE_PASSWORD");
        assert_eq!(auth.realm, "Fugue Console");
    }

    #[test]
    fn server_meta_console_basic_auth_rejects_invalid_realm() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {
                    "port": 0,
                    "meta_console": {
                        "basic_auth": {"realm": "bad\"realm"}
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let err = ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
            .unwrap_err()
            .to_string();

        assert!(err.contains("server.meta_console.basic_auth.realm"));
    }

    #[test]
    fn server_meta_console_rejects_zero_rpc_timeout() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "server": {
                    "port": 0,
                    "meta_console": {"rpc_timeout_ms": 0}
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };

        let err = ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
            .unwrap_err()
            .to_string();

        assert!(err.contains("server.meta_console.rpc_timeout_ms"));
    }

    #[test]
    fn invalid_typed_config_values_fail() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "hooks": {"timeout_ms": -1},
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).is_err()
        );

        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "agent": {"max_turns": "twenty"},
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).is_err()
        );
    }

    #[test]
    fn non_object_core_sections_fail() {
        for workspace_value in [json!([]), Value::Null] {
            let definition = WorkflowDefinition {
                config: json!({
                    "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                    "workspace": workspace_value,
                })
                .as_object()
                .unwrap()
                .clone(),
                prompt_template: "hi".to_string(),
            };
            assert!(
                ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
                    .is_err()
            );
        }
    }

    #[test]
    fn meerkat_object_fields_validate_shape() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "meerkat": {
                    "output_schema": {"type": "object"},
                    "provider_params": null,
                    "flow_tool_overlay": {"allow": ["shell"]}
                },
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert!(config.meerkat.output_schema.is_some());
        assert!(config.meerkat.provider_params.is_none());
        assert!(config.meerkat.flow_tool_overlay.is_some());

        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "meerkat": {"provider_params": "bad"},
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).is_err()
        );
    }

    #[test]
    fn per_state_concurrency_entries_are_normalized() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "agent": {
                    "max_concurrent_agents_by_state": {
                        "Todo": 2
                    }
                },
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(
            config.agent.max_concurrent_agents_by_state.get("todo"),
            Some(&2)
        );
    }

    #[test]
    fn invalid_per_state_concurrency_entries_fail() {
        for invalid_value in [json!(0), json!("many")] {
            let definition = WorkflowDefinition {
                config: json!({
                    "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                    "agent": {
                        "max_concurrent_agents_by_state": {
                            "Todo": invalid_value
                        }
                    },
                })
                .as_object()
                .unwrap()
                .clone(),
                prompt_template: "hi".to_string(),
            };
            assert!(
                ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition)
                    .is_err()
            );
        }
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "agent": {"max_concurrent_agents_by_state": []},
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).is_err()
        );
    }

    #[test]
    fn strict_prompt_render_fails_unknown_variable() {
        let issue = Issue {
            id: "id".to_string(),
            identifier: "ABC-1".to_string(),
            title: "Title".to_string(),
            description: None,
            priority: None,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: vec![],
            blocked_by: vec![],
            attachments: vec![],
            created_at: None,
            updated_at: None,
        };
        assert!(render_prompt("{{ missing.value }}", &issue, None).is_err());
        assert_eq!(
            render_prompt("{{ issue.identifier }}", &issue, Some(2)).unwrap(),
            "ABC-1"
        );
    }

    #[test]
    fn kapellmeister_defaults_to_shadow_only_rust_authoritative() {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(
            config.kapellmeister.shadow_mode.authority.mode,
            KapellmeisterAuthorityMode::ShadowOnly
        );
        assert!(config.kapellmeister.shadow_mode.authority.read_only_tools);
        assert!(
            !config
                .kapellmeister
                .shadow_mode
                .authority
                .permits(crate::tools::FugueToolOperation::LinearClaimIssue)
        );
    }

    #[test]
    fn guarded_authority_requires_explicit_safe_selectors() {
        let broad = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "kapellmeister": {"authority_mode": "guarded_labeled"}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &broad).is_err());

        let wildcard = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "kapellmeister": {
                    "authority_mode": "guarded_labeled",
                    "safe_selectors": {"linear_labels": ["*"]}
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        assert!(
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &wildcard).is_err()
        );

        let guarded = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "kapellmeister": {
                    "authority_mode": "guarded_labeled",
                    "safe_selectors": {
                        "repositories": [{"owner": "lukacf", "name": "meerkat-fugue"}],
                        "linear_labels": ["kapellmeister-safe"]
                    }
                }
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &guarded).unwrap();

        assert_eq!(
            config.kapellmeister.shadow_mode.authority.mode,
            KapellmeisterAuthorityMode::GuardedLabeled
        );
        assert_eq!(
            config
                .kapellmeister
                .shadow_mode
                .authority
                .safe_selectors
                .linear_labels,
            vec!["kapellmeister-safe"]
        );
        assert!(
            config
                .kapellmeister
                .shadow_mode
                .authority
                .permits(crate::tools::FugueToolOperation::LinearMoveIssue),
            "guarded selectors allow explicitly permitted projection tools"
        );
    }

    #[test]
    fn native_fugue_yaml_can_enable_kapellmeister_shadow_mode() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
linear:
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
kapellmeister:
  shadow_mode:
    enabled: true
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert!(config.kapellmeister.shadow_mode.enabled);
        assert!(
            !config
                .kapellmeister
                .shadow_mode
                .authority
                .permits(crate::tools::FugueToolOperation::GitHubCreatePr)
        );
    }

    #[test]
    fn native_fugue_yaml_materializes_meta_console_server_config() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
linear:
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
server:
  port: 0
  meta_console:
    enabled: false
    path: /console
    asset_dir: /tmp/meta-console/dist
    identity_provider: fugue-runtime
    rpc_timeout_ms: 1234
    basic_auth:
      username_env: FUGUE_CONSOLE_USER
      password_env: FUGUE_CONSOLE_PASSWORD
      realm: Fugue Console
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let server = config.server.expect("server config should be present");

        assert_eq!(server.port, Some(0));
        assert!(!server.meta_console.enabled);
        assert_eq!(server.meta_console.path, "/console");
        assert_eq!(
            server.meta_console.asset_dir,
            Some(PathBuf::from("/tmp/meta-console/dist"))
        );
        assert_eq!(
            server.meta_console.identity_provider.as_deref(),
            Some("fugue-runtime")
        );
        assert_eq!(server.meta_console.rpc_timeout_ms, 1234);
        let auth = server
            .meta_console
            .basic_auth
            .expect("basic auth should materialize");
        assert_eq!(auth.username_env, "FUGUE_CONSOLE_USER");
        assert_eq!(auth.password_env, "FUGUE_CONSOLE_PASSWORD");
        assert_eq!(auth.realm, "Fugue Console");
    }

    #[test]
    fn stage7a_retirement_contract_classifies_native_config_as_native_only() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.mob.backend, MobExecutionBackend::NativeExecution);
        assert!(config.mob.native.enabled);
    }

    #[test]
    fn native_runtime_disable_is_rejected() {
        let err = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
mob:
  native:
    enabled: false
linear:
  api_key: token
  project_slug: project
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("Fugue only supports native Meta execution"));
    }

    #[test]
    fn native_minimal_config_defaults_to_native_execution_path() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  labels:
    - small-fix
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: planner_coder_reviewer
  by_label:
    small-fix: planner_coder_reviewer
agents:
  planner:
    model: gpt-5.5
    system_prompt: Plan the work.
  coder:
    model: claude-sonnet-4-6
    system_prompt: Code carefully.
  reviewer:
    model: gpt-5.5
    system_prompt: Review the work.
mob_templates:
  planner_coder_reviewer:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 3
    members:
      - planner
      - coder
      - reviewer
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert_eq!(definition.prompt_template, "Work on {{ issue.identifier }}");
        assert_eq!(config.tracker.kind, "linear");
        assert_eq!(config.tracker.active_states, vec!["Todo", "Active"]);
        assert_eq!(config.tracker.intake_states, vec!["Todo"]);
        assert_eq!(config.tracker.in_flight_states, vec!["Active"]);
        assert_eq!(
            config.tracker.terminal_states,
            vec!["Done", "Canceled", "Duplicate"]
        );
        assert_eq!(config.meerkat.command, "rkat-rpc");
        assert_eq!(config.mob.backend, MobExecutionBackend::NativeExecution);
        assert!(config.mob.native.enabled);
        assert!(!config.mob.native.shadow);
        assert_eq!(
            config
                .mob
                .templates
                .get("planner_coder_reviewer")
                .unwrap()
                .backend,
            MobExecutionBackend::NativeExecution
        );
        let projection = config
            .mob
            .templates
            .get("planner_coder_reviewer")
            .and_then(|template| template.linear_projection.as_ref())
            .unwrap();
        assert!(projection.node_lanes.values().all(|lane| lane == "Active"));
        assert_eq!(projection.aggregate.precedence, vec!["Active".to_string()]);
        assert_eq!(projection.aggregate.fallback.as_deref(), Some("Active"));
        assert_eq!(projection.terminal_lanes.failed, "Active");
        assert_eq!(config.meerkat.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(
            config.meerkat.system_prompt.as_deref(),
            Some("Plan the work.")
        );
    }

    #[test]
    fn native_tracker_lanes_project_phases_to_active_with_phase_labels() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
tracker_lanes:
  parked: Backlog
  intake: Queue
  active: Active
  review_action: Review
  deploy_action: Deploy
  blocked: Blocked
  success: Done
  canceled: Canceled
linear:
  api_key: token
  project_slug: project
  labels:
    - "fugue:template:meerkat-fugue"
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    node_phase_labels:
      planner: fugue:phase:planning
      coder: fugue:phase:implementing
      reviewer: fugue:phase:reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: planner_coder_reviewer
  by_label:
    "fugue:template:meerkat-fugue": planner_coder_reviewer
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  planner_coder_reviewer:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 3
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
deploy:
  enabled: true
  action_lane: Deploy
  procedure: .fugue/deploy.md
  allowed_repositories:
    - owner: lukacf
      name: meerkat-fugue
"#,
        )
        .unwrap();

        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let template = config.mob.templates.get("planner_coder_reviewer").unwrap();
        let projection = template.linear_projection.as_ref().unwrap();

        assert_eq!(config.tracker.intake_states, vec!["Queue"]);
        assert_eq!(config.tracker.in_flight_states, vec!["Active"]);
        assert!(config.tracker.active_states.contains(&"Review".to_string()));
        assert!(config.tracker.active_states.contains(&"Deploy".to_string()));
        assert_eq!(config.linear.lanes.active.as_deref(), Some("Active"));
        assert_eq!(config.linear.lanes.do_review.as_deref(), Some("Review"));
        assert_eq!(config.linear.lanes.deploy.as_deref(), Some("Deploy"));
        assert_eq!(
            projection
                .node_lanes
                .values()
                .collect::<BTreeSet<_>>()
                .len(),
            1
        );
        assert!(projection.node_lanes.values().all(|lane| lane == "Active"));
        assert_eq!(
            projection
                .node_phase_labels
                .get("coder")
                .map(String::as_str),
            Some("fugue:phase:implementing")
        );
        assert_eq!(config.deploy.procedure, ".fugue/deploy.md");
    }

    #[test]
    fn native_execution_instructions_include_downloaded_attachment_paths() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  labels: []
selection:
  default_template: implementation_mob
agents:
  coder: {}
mob_templates:
  implementation_mob:
    members: [coder]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        let template = config.mob.templates.get("implementation_mob").unwrap();
        let mut issue = Issue {
            id: "issue-id".to_string(),
            identifier: "LUC-631".to_string(),
            title: "Attachments".to_string(),
            description: None,
            priority: None,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: Vec::new(),
            blocked_by: Vec::new(),
            attachments: vec![IssueAttachment {
                id: Some("att-1".to_string()),
                title: Some("evidence.txt".to_string()),
                url: "https://uploads.linear.app/evidence.txt".to_string(),
                local_path: Some(PathBuf::from(
                    "/tmp/workspaces/LUC-631/.fugue/attachments/LUC-631/001-evidence.txt",
                )),
            }],
            created_at: None,
            updated_at: None,
        };

        let instructions = config
            .render_native_execution_instructions(
                &issue,
                None,
                Path::new("/tmp/workspaces/LUC-631"),
                &MobTemplateId("implementation_mob".to_string()),
                template,
            )
            .unwrap();

        assert!(instructions.contains("Issue attachments downloaded for agents:"));
        assert!(instructions.contains("evidence.txt (`att-1`)"));
        assert!(
            instructions
                .contains("/tmp/workspaces/LUC-631/.fugue/attachments/LUC-631/001-evidence.txt")
        );
        issue.attachments.clear();
        let no_attachment_instructions = config
            .render_native_execution_instructions(
                &issue,
                None,
                Path::new("/tmp/workspaces/LUC-631"),
                &MobTemplateId("implementation_mob".to_string()),
                template,
            )
            .unwrap();
        assert!(!no_attachment_instructions.contains("Issue attachments downloaded for agents"));
    }

    #[test]
    fn native_execution_templates_materialize_planner_coder_reviewer_contracts() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
backend: native-execution
linear:
  labels: [needs-review]
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: implementation_mob
  by_label:
    needs-review: implementation_mob
agents:
  planner:
    model: gpt-5.5
    system_prompt: Plan the work.
  coder:
    model: claude-sonnet-4-6
    tools: [shell, git]
  reviewer:
    provider: openai
    system_prompt: Review the patch.
mob_templates:
  implementation_mob:
    backend: native-execution
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members:
      - name: planner
        profile: planner
      - name: coder
        profile: coder
      - name: reviewer
        profile: reviewer
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.mob.backend, MobExecutionBackend::NativeExecution);
        assert!(
            config.mob.native.enabled,
            "native execution is enabled by default for Stage 6 configs"
        );
        assert!(!config.mob.native.shadow);
        let template = config.mob.templates.get("implementation_mob").unwrap();
        assert_eq!(template.backend, MobExecutionBackend::NativeExecution);
        assert_eq!(template.flow, Some(NativeMobFlow::PlannerCoderReviewer));
        assert_eq!(template.max_review_cycles, Some(2));
        assert_eq!(
            template
                .members
                .iter()
                .map(|member| member.name.as_str())
                .collect::<Vec<_>>(),
            vec!["planner", "coder", "reviewer"]
        );
        assert_eq!(
            template.members[1].agent.model.as_deref(),
            Some("claude-sonnet-4-6")
        );
    }

    #[test]
    fn native_named_linear_projection_derives_tracker_and_required_states() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  team: LUC
  provisioning:
    mode: create_missing
  lanes:
    parked: Icebox
    ready: Ready
    active: Doing
    success: Shipped
    canceled: Dropped
    duplicate: Duplicate
linear_projections:
  quality_gate:
    node_lanes:
      planner: Design
      coder: Build
      reviewer: Gate
    aggregate:
      precedence: [Gate, Build, Design]
      fallback: Gate
    terminal_lanes:
      succeeded: Shipped
      failed: Gate
      cancelled: Dropped
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: quality_gate
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        let template = config.mob.templates.get("standard").unwrap();
        let projection = template.linear_projection.as_ref().unwrap();
        assert_eq!(projection.id, "quality_gate");
        assert_eq!(
            projection.node_lanes.get("planner").map(String::as_str),
            Some("Doing")
        );
        assert_eq!(config.tracker.intake_states, vec!["Ready"]);
        assert_eq!(config.tracker.in_flight_states, vec!["Doing"]);
        assert_eq!(config.tracker.active_states, vec!["Ready", "Doing"]);
        assert_eq!(
            config.tracker.terminal_states,
            vec!["Shipped", "Dropped", "Duplicate"]
        );
        assert_eq!(config.linear.team.as_deref(), Some("LUC"));
        assert_eq!(
            config.linear.provisioning.mode,
            NativeLinearProvisioningMode::CreateMissing
        );
        assert_eq!(
            config
                .linear
                .required_states
                .iter()
                .map(|state| (state.name.as_str(), state.state_type))
                .collect::<Vec<_>>(),
            vec![
                ("Icebox", LinearStateType::Backlog),
                ("Ready", LinearStateType::Unstarted),
                ("Doing", LinearStateType::Started),
                ("Shipped", LinearStateType::Completed),
                ("Dropped", LinearStateType::Canceled),
                ("Duplicate", LinearStateType::Duplicate),
            ]
        );
    }

    #[test]
    fn native_linear_attention_lanes_are_not_cleanup_terminal_lifecycle() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  team: LUC
  lanes:
    parked: Icebox
    ready: Ready
    active: Doing
    review_ready: Ready for review
    blocked: Blocked
    success: Shipped
    canceled: Dropped
    duplicate: Duplicate
linear_projections:
  quality_gate:
    node_lanes:
      planner: Design
      coder: Build
      reviewer: Gate
    aggregate:
      precedence: [Gate, Build, Design]
      fallback: Gate
    terminal_lanes:
      succeeded: Shipped
      failed: Gate
      cancelled: Dropped
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: quality_gate
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.tracker.in_flight_states, vec!["Doing"]);
        assert_eq!(
            config.tracker.terminal_states,
            vec!["Shipped", "Dropped", "Duplicate"]
        );
        assert_eq!(
            config
                .linear
                .required_states
                .iter()
                .map(|state| (state.name.as_str(), state.state_type))
                .collect::<Vec<_>>(),
            vec![
                ("Icebox", LinearStateType::Backlog),
                ("Ready", LinearStateType::Unstarted),
                ("Doing", LinearStateType::Started),
                ("Ready for review", LinearStateType::Started),
                ("Blocked", LinearStateType::Started),
                ("Shipped", LinearStateType::Completed),
                ("Dropped", LinearStateType::Canceled),
                ("Duplicate", LinearStateType::Duplicate),
            ]
        );
    }

    #[test]
    fn native_linear_states_are_rejected_after_clean_cut() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  states:
    intake: [Todo]
    active: [Plan, Build, Gate]
    complete: [Done]
    canceled: [Canceled]
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        );
        let err = definition.unwrap_err().to_string();
        assert!(err.contains("unknown field `states`"));
    }

    #[test]
    fn native_explicit_handoff_lanes_remain_configured_lanes() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  team: LUC
  provisioning:
    mode: create_missing
  lanes:
    review_ready: Human Review
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.tracker.in_flight_states, vec!["Active"]);
        assert_eq!(config.tracker.active_states, vec!["Todo", "Active"]);
        assert_eq!(
            config.tracker.terminal_states,
            vec!["Done", "Canceled", "Duplicate",]
        );
        assert_eq!(
            config
                .linear
                .required_states
                .iter()
                .map(|state| (state.name.as_str(), state.state_type))
                .collect::<Vec<_>>(),
            vec![
                ("Backlog", LinearStateType::Backlog),
                ("Todo", LinearStateType::Unstarted),
                ("Active", LinearStateType::Started),
                ("Human Review", LinearStateType::Started),
                ("Done", LinearStateType::Completed),
                ("Canceled", LinearStateType::Canceled),
                ("Duplicate", LinearStateType::Duplicate),
            ]
        );
    }

    #[test]
    fn native_final_review_lane_is_provisioned_when_gate_enabled() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
  team: LUC
  provisioning:
    mode: create_missing
  lanes:
    review_ready: Ready for review
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: standard
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  standard:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members: [planner, coder, reviewer]
final_review:
  enabled: true
  mode: manual
  lane: Final Review & Merge
  rework_lane: Planning
  blocked_lane: Blocked
  success_lane: Done
  reviewers:
    - id: architecture
      profile: final_architecture_reviewer
      required: true
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert!(
            config
                .linear
                .required_states
                .iter()
                .any(|state| state.name == "Final Review & Merge"
                    && state.state_type == LinearStateType::Started),
            "manual final-review lane must be provisioned as a started/attention lane"
        );
    }

    #[test]
    fn native_template_with_non_default_flow_requires_projection() {
        let err = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project
selection:
  default_template: pair
agents:
  planner: {}
  coder: {}
mob_templates:
  pair:
    flow: planner-coder
    members: [planner, coder]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("mob_templates.pair.linear_projection is required"));
    }

    #[test]
    fn native_repo_tools_must_be_mcp_servers() {
        let invalid = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  labels: []
selection:
  default_template: implementation_mob
agents:
  planner:
    tools: [linear]
mob_templates:
  implementation_mob:
    max_review_cycles: 2
    members: [planner]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(invalid.contains("must be packaged as MCP servers"));

        let unknown_mcp = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  labels: []
selection:
  default_template: implementation_mob
agents:
  planner:
    tools: [mcp:linear]
mob_templates:
  implementation_mob:
    max_review_cycles: 2
    members: [planner]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(unknown_mcp.contains("unknown MCP server"));

        let valid = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  labels: []
selection:
  default_template: implementation_mob
mcp_servers:
  linear:
    command: [fugue-linear-mcp]
agents:
  planner:
    tools: [mcp:linear]
mob_templates:
  implementation_mob:
    max_review_cycles: 2
    members: [planner]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &valid).unwrap();
        let template = config.mob.templates.get("implementation_mob").unwrap();
        assert_eq!(
            template.members[0].agent.tools,
            vec!["mcp:linear", "mcp:playwright"]
        );
    }

    #[test]
    fn native_playwright_mcp_is_default_for_all_issue_agents() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  labels: []
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: implementation_mob
agents:
  planner:
    tools: [mob, workgraph]
  coder:
    tools: [mob, shell, git, workgraph]
  reviewer:
    tools: [mob, shell, git, workgraph]
mob_templates:
  implementation_mob:
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members: [planner, coder, reviewer]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(
            config
                .mob
                .mcp_servers
                .get("playwright")
                .map(|server| server.command.as_slice()),
            Some(
                &[
                    "npx".to_string(),
                    "-y".to_string(),
                    "@playwright/mcp@latest".to_string(),
                    "--headless".to_string(),
                ][..]
            )
        );
        let template = config.mob.templates.get("implementation_mob").unwrap();
        assert!(template.members.iter().all(|member| {
            member
                .agent
                .tools
                .iter()
                .any(|tool| tool == "mcp:playwright")
        }));
    }

    #[test]
    fn fugue_ims_allows_linear_shaped_tools_without_real_linear_mcp_server() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  kind: fugue-ims
  endpoint: sqlite:///tmp/fugue-ims.sqlite
  project_slug: fugue-ims
selection:
  default_template: implementation_mob
agents:
  issue_lead:
    tools: [mob, mcp:linear]
mob_templates:
  implementation_mob:
    members: [issue_lead]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .expect("fugue-ims should use host-provided Linear-shaped tools without real Linear MCP");
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();
        assert!(config.mob.mcp_servers.contains_key("playwright"));
        assert_eq!(
            config
                .mob
                .templates
                .get("implementation_mob")
                .unwrap()
                .members[0]
                .agent
                .tools,
            vec!["mob", "mcp:linear", "mcp:playwright"]
        );
    }

    #[test]
    fn native_mcp_linear_server_inherits_project_slug_for_project_scoped_tools() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
linear:
  api_key: token
  project_slug: project-from-fugue-yaml
  labels: []
selection:
  default_template: implementation_mob
mcp_servers:
  linear:
    command: [fugue-linear-mcp]
    env:
      FUGUE_LINEAR_ISSUE_IDENTIFIER: LUC-262
  linear_projection:
    command: [/opt/fugue/bin/fugue-linear-mcp]
    env:
      FUGUE_LINEAR_MCP_AUTHORITY: projection
  github:
    command: [fugue-github-mcp]
agents:
  planner:
    tools: [mcp:linear, mcp:github]
mob_templates:
  implementation_mob:
    max_review_cycles: 2
    members: [planner]
worker:
  prompt_template: "Work on {{ issue.identifier }}"
  native_launch_context_template: "native {{ issue.identifier }} {{ worktree }}"
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(
            config
                .mob
                .mcp_servers
                .get("linear")
                .and_then(|server| server.env.get("LINEAR_PROJECT_SLUG"))
                .map(String::as_str),
            Some("project-from-fugue-yaml"),
            "native per-worktree Linear MCP config must pass the fugue.yaml project slug for project-scoped tools"
        );
        assert_eq!(
            config
                .mob
                .mcp_servers
                .get("linear")
                .and_then(|server| server.env.get("FUGUE_LINEAR_ISSUE_IDENTIFIER"))
                .map(String::as_str),
            Some("LUC-262"),
            "project slug injection must preserve existing issue-scoped Linear MCP env"
        );
        assert_eq!(
            config
                .mob
                .mcp_servers
                .get("linear_projection")
                .and_then(|server| server.env.get("LINEAR_PROJECT_SLUG"))
                .map(String::as_str),
            Some("project-from-fugue-yaml"),
            "projection-authority Linear MCP servers must inherit the project slug too"
        );
        assert_eq!(
            config
                .mob
                .mcp_servers
                .get("linear_projection")
                .and_then(|server| server.env.get("FUGUE_LINEAR_MCP_AUTHORITY"))
                .map(String::as_str),
            Some("projection"),
            "project slug injection must preserve projection authority env"
        );
        assert!(
            !config
                .mob
                .mcp_servers
                .get("github")
                .is_some_and(|server| server.env.contains_key("LINEAR_PROJECT_SLUG")),
            "Linear project scope must not leak into non-Linear MCP servers"
        );
    }

    #[test]
    fn native_fugue_yaml_preserves_configured_meerkat_command() {
        let definition = parse_native_workflow(
            Path::new("/tmp/fugue.yaml"),
            r#"
version: 1
backend: native-execution
linear:
  labels: []
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: implementation_mob
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  implementation_mob:
    backend: native-execution
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 2
    members: [planner, coder, reviewer]
meerkat:
  command: /opt/fugue/bin/rkat-rpc
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#,
        )
        .unwrap();
        let config =
            ServiceConfig::from_definition(PathBuf::from("/tmp/fugue.yaml"), &definition).unwrap();

        assert_eq!(config.meerkat.command, "/opt/fugue/bin/rkat-rpc");
    }

    #[test]
    fn invalid_native_rework_template_fails_static_validation() {
        let invalid = r#"
version: 1
backend: native-execution
linear:
  labels: []
linear_projections:
  planner_coder_reviewer:
    node_lanes:
      planner: Planning
      coder: Implementing
      reviewer: Reviewing
    aggregate:
      precedence: [Reviewing, Implementing, Planning]
      fallback: Reviewing
    terminal_lanes:
      succeeded: Done
      failed: Reviewing
      cancelled: Canceled
selection:
  default_template: mob
agents:
  planner: {}
  coder: {}
  reviewer: {}
mob_templates:
  mob:
    backend: native-execution
    flow: planner-coder-reviewer
    linear_projection: planner_coder_reviewer
    max_review_cycles: 0
    members: [planner, coder, reviewer]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#;
        let err = parse_native_workflow(Path::new("/tmp/fugue.yaml"), invalid)
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_review_cycles must be positive"));
    }

    #[test]
    fn native_prompt_file_is_config_relative() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.txt");
        let context_path = dir.path().join("native-context.txt");
        fs::write(&prompt_path, "Hello {{ issue.identifier }}\n").unwrap();
        fs::write(
            &context_path,
            "Configured context for {{ issue.identifier }} in {{ worktree }}\n",
        )
        .unwrap();
        let workflow_path = dir.path().join("fugue.yaml");
        let contents = r#"
linear:
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members:
      - coder
worker:
  prompt_file: prompt.txt
  native_launch_context_file: native-context.txt
"#;
        let definition = parse_native_workflow(&workflow_path, contents).unwrap();
        assert_eq!(definition.prompt_template, "Hello {{ issue.identifier }}");
        assert_eq!(
            definition
                .config
                .get("worker_policy")
                .and_then(|value| value.get("native_launch_context_template"))
                .and_then(|value| value.as_str()),
            Some("Configured context for {{ issue.identifier }} in {{ worktree }}")
        );
    }

    #[test]
    fn native_validation_rejects_bad_references_and_backend() {
        let base = r#"
linear:
  labels: [declared]
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#;
        assert!(
            parse_native_workflow(
                Path::new("/tmp/fugue.yaml"),
                &base.replace("members: [coder]", "members: [ghost]")
            )
            .is_err()
        );
        assert!(
            parse_native_workflow(
                Path::new("/tmp/fugue.yaml"),
                &base.replace("default_template: solo", "default_template: ghost")
            )
            .is_err()
        );
        let unknown_label = r#"
linear:
  labels: [declared]
selection:
  default_template: solo
  by_label:
    missing: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#;
        assert!(parse_native_workflow(Path::new("/tmp/fugue.yaml"), unknown_label).is_err());
        assert!(
            parse_native_workflow(
                Path::new("/tmp/fugue.yaml"),
                &base.replace("linear:\n", "backend: native_mob\nlinear:\n")
            )
            .is_err()
        );
    }

    #[test]
    fn native_validation_rejects_removed_linear_states_and_prompt_conflicts() {
        let removed_states = r#"
linear:
  labels: []
  states:
    intake: [Todo]
    active: [todo]
    human_review: [Human Review]
    merge_ready: [Merging]
    complete: [Done]
    canceled: [Canceled]
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#;
        let removed_states_error =
            parse_native_workflow(Path::new("/tmp/fugue.yaml"), removed_states)
                .unwrap_err()
                .to_string();
        assert!(removed_states_error.contains("unknown field `states`"));

        let prompt_conflict = r#"
linear:
  labels: []
selection:
  default_template: solo
agents:
  coder: {}
mob_templates:
  solo:
    members: [coder]
worker:
  prompt_template: hi
  prompt_file: prompt.txt
  native_launch_context_template: native {{ issue.identifier }} {{ worktree }}
"#;
        assert!(parse_native_workflow(Path::new("/tmp/fugue.yaml"), prompt_conflict).is_err());
    }

    #[test]
    fn rejects_string_auth_binding_and_unknown_skill_refs() {
        assert!(parse_auth_binding(&Value::String("realm:binding".into())).is_err());
        assert!(parse_skill_refs(&json!(["unknown-skill"])).is_err());
        assert!(parse_preload_skills(&json!(["unknown-skill"])).is_err());
    }
    #[test]
    fn auth_binding_uses_native_structural_contract() {
        let value = json!({"realm": "production", "binding": "coding", "profile": "review"});
        let binding = parse_auth_binding(&value).expect("binding");
        assert_eq!(serde_json::to_value(binding).expect("serialize"), value);
        assert!(parse_auth_binding(&json!({"realm": "production"})).is_err());
    }

    #[test]
    fn native_profiles_reject_unknown_provider_parameters() {
        let profile: NativeAgentProfile = serde_json::from_value(json!({
            "model": "gpt-5.5", "provider": "openai",
            "provider_params": {"not_a_provider_parameter": true}
        }))
        .expect("profile shape");
        let error = validate_agent_profiles(
            &BTreeMap::from([("worker".to_string(), profile)]),
            &BTreeMap::new(),
            None,
        )
        .expect_err("unknown provider parameter must fail before dispatch");
        assert!(error.to_string().contains("agents.worker.provider_params"));
    }
    #[test]
    fn native_session_capacity_is_configurable_and_nonzero() {
        let root = tempfile::tempdir().expect("root");
        assert_eq!(
            MeerkatConfig::from_raw(None, root.path())
                .expect("defaults")
                .max_sessions,
            4096
        );
        let configured = json!({"max_sessions": 8192});
        assert_eq!(
            MeerkatConfig::from_raw(configured.as_object(), root.path())
                .expect("configured")
                .max_sessions,
            8192
        );
        let invalid = json!({"max_sessions": 0});
        assert!(MeerkatConfig::from_raw(invalid.as_object(), root.path()).is_err());
    }
    #[test]
    fn legacy_configuration_names_fail_with_migration_guidance() {
        let root = tempfile::tempdir().expect("root");
        let meerkat = json!({"connection_ref": {"realm": "prod", "binding": "coding"}});
        assert!(
            MeerkatConfig::from_raw(meerkat.as_object(), root.path())
                .expect_err("old auth setting must not be silently ignored")
                .to_string()
                .contains("auth_binding")
        );
        let server = json!({"mobkit_console": {"enabled": true}});
        assert!(
            ServerConfig::from_raw(server.as_object())
                .expect_err("old console setting must not be silently ignored")
                .to_string()
                .contains("meta_console")
        );
    }
}
