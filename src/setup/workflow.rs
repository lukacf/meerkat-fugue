//! Generate the queue and repository-team configurations from one setup intent.
//! Portable configuration contains credential references, never credentials.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::connections::{Connections, ModelCredential, ModelProvider, TrackerConnection};
use crate::{
    config::*,
    ensemble::config::{EnsembleConfig, RepoId, RepositoryConfig},
    kapellmeister::{KapellmeisterAuthorityMode, KapellmeisterSafeSelectors},
    tools::{GitHubHost, GitHubOwner, GitHubRepoName, GitHubRepoRef},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSetup {
    #[serde(default)]
    pub repository_url: Option<String>,
    #[serde(default)]
    pub allow_no_ci_checks: bool,
    pub owner: String,
    pub repository: String,
    /// Existing checkout, or a managed clone when absent.
    pub checkout: Option<PathBuf>,
    pub tracker_project: Option<String>,
    pub tracker_team: Option<String>,
    pub jira_issue_type: Option<String>,
    pub jira_jql: Option<String>,
    /// Optional Jira transition fields, retained only in private workspace state.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub jira_transition_fields:
        BTreeMap<String, BTreeMap<String, Box<serde_json::value::RawValue>>>,
    pub lanes: TrackerLaneRoles,
}

pub struct GeneratedWorkspace {
    pub workflow: NativeFugueConfig,
    pub ensemble: EnsembleConfig,
    pub checkout: PathBuf,
    pub repository: GitHubRepoRef,
}

impl WorkflowSetup {
    pub fn validate(&self) -> Result<()> {
        if let Some(url) = &self.repository_url {
            let parsed = super::repository::RepositoryAddress::parse(url)?;
            if parsed.owner != self.owner || parsed.repository != self.repository {
                bail!("The repository URL and repository selection disagree");
            }
        }
        for value in [&self.owner, &self.repository] {
            if value.is_empty()
                || value.len() > 100
                || value == "."
                || value == ".."
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                bail!("Enter the repository owner and name, without a URL or path");
            }
        }
        if self
            .checkout
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            bail!("The checkout must be an absolute path");
        }
        for value in [
            &self.lanes.parked,
            &self.lanes.intake,
            &self.lanes.active,
            &self.lanes.review_action,
            &self.lanes.deploy_action,
            &self.lanes.blocked,
            &self.lanes.success,
            &self.lanes.canceled,
        ] {
            if value.trim().is_empty() || value.len() > 200 || value.contains(['\n', '\r', '\0']) {
                bail!("Choose a status for each workflow role");
            }
        }
        Ok(())
    }

    pub fn render(
        &self,
        connections: &Connections,
        root: &Path,
        port: u16,
        bin_dir: &Path,
    ) -> Result<GeneratedWorkspace> {
        self.validate()?;
        connections.validate()?;
        let model = connections
            .model
            .as_ref()
            .context("Connect a model first")?;
        let github = connections
            .github
            .as_ref()
            .context("Connect GitHub first")?;
        if let Some(url) = &self.repository_url {
            let parsed = super::repository::RepositoryAddress::parse(url)?;
            if !parsed.host.eq_ignore_ascii_case(&github.host) {
                bail!("Save access for the repository's hostname before preparing it");
            }
        }
        let tracker = connections
            .tracker
            .as_ref()
            .context("Choose an issue tracker first")?;
        let jira = matches!(tracker, TrackerConnection::Jira { .. });
        if !jira && self.lanes.parked.eq_ignore_ascii_case(&self.lanes.intake) {
            bail!("Draft and ready statuses must differ");
        }
        let root = std::path::absolute(root)?;
        let repository = GitHubRepoRef {
            host: Some(GitHubHost::try_from(github.host.clone()).map_err(anyhow::Error::msg)?),
            owner: GitHubOwner(self.owner.clone()),
            name: GitHubRepoName(self.repository.clone()),
        };
        let checkout = self.checkout.clone().unwrap_or_else(|| {
            root.join("checkouts")
                .join(&self.owner)
                .join(&self.repository)
        });
        let binding = match &model.credential {
            ModelCredential::Binding { auth_binding } => Some(auth_binding.clone()),
            _ => None,
        };
        let provider = match model.provider {
            ModelProvider::Openai => "openai",
            ModelProvider::Anthropic => "anthropic",
            ModelProvider::Gemini => "gemini",
        };
        let mut config: NativeFugueConfig =
            serde_yaml::from_str(include_str!("../../fugue.jira.example.yaml"))?;
        let lanes = &self.lanes;
        // A Jira workflow may have fewer statuses than Fugue has semantic
        // outcomes. Keep the handoff status distinct from action triggers.
        config.tracker_lanes = (!jira).then(|| lanes.clone());
        config.linear.lanes = NativeLinearLanes {
            parked: lanes.parked.clone(),
            ready: lanes.intake.clone(),
            active: Some(lanes.active.clone()),
            review_ready: jira.then(|| lanes.review_action.clone()),
            rework: None,
            do_review: (!jira).then(|| lanes.review_action.clone()),
            deploy: Some(lanes.deploy_action.clone()),
            blocked: if jira
                && [
                    &lanes.parked,
                    &lanes.intake,
                    &lanes.active,
                    &lanes.review_action,
                    &lanes.deploy_action,
                    &lanes.success,
                    &lanes.canceled,
                ]
                .iter()
                .any(|state| state.eq_ignore_ascii_case(&lanes.blocked))
            {
                None
            } else {
                Some(lanes.blocked.clone())
            },
            success: lanes.success.clone(),
            canceled: lanes.canceled.clone(),
            duplicate: if matches!(tracker, TrackerConnection::Jira { .. }) {
                lanes.canceled.clone()
            } else {
                "Duplicate".into()
            },
        };
        config.linear.project_slug = Some(match tracker {
            TrackerConnection::Local => format!("{}/{}", self.owner, self.repository),
            _ => self
                .tracker_project
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .context("Choose a tracker project")?
                .clone(),
        });
        config.linear.team = self.tracker_team.clone();
        config.linear.labels = Vec::new();
        config.linear.provisioning.mode = NativeLinearProvisioningMode::Validate;
        config.mcp_servers.remove("jira");
        let tracker_tool = match tracker {
            TrackerConnection::Local => {
                config.linear.kind = Some("fugue-ims".into());
                config.linear.endpoint =
                    Some(format!("sqlite://{}", root.join("issues.sqlite").display()));
                config.linear.api_key = None;
                config.linear.jira = None;
                "linear"
            }
            TrackerConnection::Linear { .. } => {
                config.linear.kind = Some("linear".into());
                config.linear.endpoint = None;
                config.linear.jira = None;
                config.linear.api_key = Some("$LINEAR_API_KEY".into());
                config.mcp_servers.insert(
                    "linear".into(),
                    NativeMcpServerConfig {
                        command: vec![bin_dir.join("fugue-linear-mcp").display().to_string()],
                        url: None,
                        env: BTreeMap::from([("LINEAR_API_KEY".into(), "$LINEAR_API_KEY".into())]),
                    },
                );
                "linear"
            }
            TrackerConnection::Jira { site, .. } => {
                if self
                    .jira_jql
                    .as_deref()
                    .is_none_or(|jql| jql.trim().is_empty())
                {
                    bail!("Set a Jira filter for the tickets Fugue may work on");
                }
                config.linear.kind = Some("jira".into());
                config.linear.endpoint = Some(site.clone());
                config.linear.api_key = Some("$JIRA_API_TOKEN".into());
                config.linear.jira = Some(crate::jira::JiraConfig {
                    email: Some("$JIRA_EMAIL".into()),
                    jql: self.jira_jql.clone(),
                    issue_type: self.jira_issue_type.clone(),
                    ..Default::default()
                });
                config.mcp_servers.insert(
                    "jira".into(),
                    NativeMcpServerConfig {
                        command: vec![bin_dir.join("fugue-jira-mcp").display().to_string()],
                        url: None,
                        env: BTreeMap::from([("JIRA_API_TOKEN".into(), "$JIRA_API_TOKEN".into())]),
                    },
                );
                "jira"
            }
        };
        for agent in config.agents.values_mut() {
            agent.model = Some(model.model.clone());
            agent.provider = Some(provider.into());
            agent.auth_binding = binding.clone();
            for tool in &mut agent.tools {
                if tool == "mcp:jira" {
                    *tool = format!("mcp:{tracker_tool}");
                }
            }
        }
        for projection in config.linear_projections.values_mut() {
            for lane in projection.node_lanes.values_mut() {
                *lane = lanes.active.clone();
            }
            projection.aggregate.precedence = vec![lanes.active.clone()];
            projection.aggregate.fallback = Some(lanes.active.clone());
            projection.terminal_lanes = NativeLinearProjectionTerminals {
                succeeded: lanes.review_action.clone(),
                failed: lanes.blocked.clone(),
                cancelled: lanes.canceled.clone(),
            };
        }
        config.mcp_servers.insert(
            "github".into(),
            NativeMcpServerConfig {
                command: vec![bin_dir.join("fugue-github-mcp").display().to_string()],
                url: None,
                env: BTreeMap::from([
                    ("FUGUE_GITHUB_HOST".into(), github.host.clone()),
                    ("FUGUE_GITHUB_OWNER".into(), self.owner.clone()),
                    ("FUGUE_GITHUB_REPO".into(), self.repository.clone()),
                ]),
            },
        );
        config.workspace = Some(NativeWorkspaceConfig {
            root: Some(root.join("worktrees").display().to_string()),
        });
        // Repository identity is validated above; no user-authored shell is accepted.
        config.hooks = Some(NativeHooksConfig {
            after_create: Some(format!(
                "git -c credential.helper= -c 'credential.https://{}.helper=!gh auth git-credential' clone https://{}/{}/{}.git .\ngit config credential.https://{}.helper '!gh auth git-credential'",
                github.host, github.host, self.owner, self.repository, github.host
            )),
            before_run: Some("git fetch --all --prune".into()),
            after_run: None,
            before_remove: None,
            timeout_ms: Some(120000),
        });
        config.deploy = Some(DeployLaneConfig {
            enabled: true,
            lane: lanes.deploy_action.clone(),
            action_lane: Some(lanes.deploy_action.clone()),
            procedure: "merge-only".into(),
            success_lane: lanes.success.clone(),
            ready_lane: lanes.review_action.clone(),
            rework_lane: lanes.intake.clone(),
            blocked_lane: lanes.blocked.clone(),
            allowed_repositories: vec![repository.clone()],
            require_ota_update: false,
            allow_no_ci_checks: self.allow_no_ci_checks,
            ..Default::default()
        });
        config.worker.prompt_template = Some(include_str!("worker.md").into());
        config.meerkat = Some(NativeMeerkatConfig {
            repository_runtime: Some(crate::config::RepositoryRuntimeBinding {
                repository: RepoId::try_from(self.repository.clone())
                    .map_err(anyhow::Error::msg)?,
                state_path: root.join("repository-teams").join(&self.repository),
            }),
            model: Some(model.model.clone()),
            provider: Some(provider.into()),
            auth_binding: binding.clone(),
            state_root: Some(root.join("runtime").display().to_string()),
            ..Default::default()
        });
        config.kapellmeister = Some(NativeKapellmeisterConfig {
            shadow_mode: Some(NativeKapellmeisterShadowModeConfig {
                enabled: Some(false),
                state_path: None,
                prompt: None,
            }),
            authority_mode: Some(KapellmeisterAuthorityMode::GuardedLabeled),
            safe_selectors: Some(KapellmeisterSafeSelectors {
                repositories: vec![repository.clone()],
                linear_labels: vec![],
            }),
        });
        config.server = Some(NativeServerConfig {
            port: Some(port),
            meta_console: Some(NativeMetaConsoleServerConfig {
                enabled: Some(true),
                ..Default::default()
            }),
        });
        config.polling = Some(NativePollingConfig {
            interval_ms: Some(10000),
        });
        // Validate before checkout/network work, while errors can be reported
        // from typed configuration without exposing subprocess response bodies.
        parse_native_workflow(&root.join("fugue.yaml"), &serde_yaml::to_string(&config)?)?;
        let mut ensemble = EnsembleConfig::for_repositories(&[], "My workspace".into())?;
        ensemble.model = model.model.clone();
        ensemble.auth_binding = binding;
        ensemble.state_root = root.join("repository-teams");
        let id = RepoId::try_from(self.repository.clone()).map_err(anyhow::Error::msg)?;
        ensemble.repositories.insert(
            id,
            RepositoryConfig {
                path: checkout.clone(),
                description: Some(format!("{}/{}", self.owner, self.repository)),
            },
        );
        Ok(GeneratedWorkspace {
            workflow: config,
            ensemble,
            checkout,
            repository,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::connections::{GitHubConnection, GitHubCredential, ModelConnection};

    #[test]
    fn classic_jira_workflow_keeps_review_handoff_separate_from_actions() {
        let directory = tempfile::tempdir().unwrap();
        let connections = Connections {
            model: Some(ModelConnection {
                provider: ModelProvider::Openai,
                model: crate::config::default_agent_model().into(),
                credential: ModelCredential::Environment,
            }),
            github: Some(GitHubConnection {
                host: "git.example.com".into(),
                credential: GitHubCredential::GhCli,
            }),
            tracker: Some(serde_json::from_str(r#"{"kind":"jira","site":"https://example.atlassian.net","email":"test@example.com","api_token":"test-token"}"#).unwrap()),
        };
        let request = WorkflowSetup {
            allow_no_ci_checks: false,
            repository_url: None,
            owner: "example".into(),
            repository: "app".into(),
            checkout: None,
            tracker_project: Some("EXAMPLE".into()),
            tracker_team: None,
            jira_issue_type: Some("Task".into()),
            jira_jql: Some("reporter = currentUser()".into()),
            jira_transition_fields: BTreeMap::new(),
            lanes: TrackerLaneRoles {
                parked: "Open".into(),
                intake: "Open".into(),
                active: "In Progress".into(),
                review_action: "Resolved".into(),
                deploy_action: "Reopened".into(),
                blocked: "In Progress".into(),
                success: "Closed".into(),
                canceled: "Closed".into(),
            },
        };
        let rendered = request
            .render(
                &connections,
                directory.path(),
                8788,
                Path::new("/opt/fugue/bin"),
            )
            .unwrap();
        let workflow = serde_yaml::to_string(&rendered.workflow).unwrap();
        let path = directory.path().join("fugue.yaml");
        let definition = parse_native_workflow(&path, &workflow).unwrap();
        let mut config = ServiceConfig::from_definition(path, &definition).unwrap();
        config.tracker.api_key = Some("test-token".into());
        config.tracker.jira.as_mut().unwrap().email = Some("test@example.com".into());
        config.validate_dispatch().unwrap();
        assert_eq!(config.linear.lanes.do_review, None);
        assert_eq!(
            config.linear.lanes.review_ready.as_deref(),
            Some("Resolved")
        );
        assert_eq!(config.linear.lanes.blocked, None);
        assert_eq!(config.deploy.lane, "Reopened");
        assert_eq!(config.deploy.success_lane, "Closed");
        assert!(!config.deploy.allow_no_ci_checks);
        assert!(config.linear.required_states.iter().all(|state| {
            ["Open", "In Progress", "Resolved", "Reopened", "Closed"].contains(&state.name.as_str())
        }));
        let mut invalid = request;
        invalid.lanes.deploy_action = "In Progress".into();
        let error = invalid
            .render(
                &connections,
                directory.path(),
                8788,
                Path::new("/opt/fugue/bin"),
            )
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("conflicts"), "{error}");
        assert!(!error.contains("test-token"));
    }

    #[tokio::test]
    async fn blank_local_setup_generates_one_consistent_native_workspace() {
        let directory = tempfile::tempdir().unwrap();
        let connections = Connections {
            model: Some(ModelConnection {
                provider: ModelProvider::Openai,
                model: "gpt-5.5".into(),
                credential: ModelCredential::Environment,
            }),
            github: Some(GitHubConnection {
                host: "git.example.com".into(),
                credential: GitHubCredential::GhCli,
            }),
            tracker: Some(TrackerConnection::Local),
        };
        let request = WorkflowSetup {
            allow_no_ci_checks: false,
            repository_url: None,
            owner: "example".into(),
            repository: "application".into(),
            checkout: None,
            tracker_project: None,
            tracker_team: None,
            jira_issue_type: None,
            jira_jql: None,
            jira_transition_fields: BTreeMap::new(),
            lanes: TrackerLaneRoles::default(),
        };
        let rendered = request
            .render(
                &connections,
                directory.path(),
                8788,
                Path::new("/opt/fugue/bin"),
            )
            .unwrap();
        let workflow = serde_yaml::to_string(&rendered.workflow).unwrap();
        let ensemble = serde_yaml::to_string(&rendered.ensemble).unwrap();
        let path = directory.path().join("fugue.yaml");
        let definition = parse_native_workflow(&path, &workflow).unwrap();
        let config = ServiceConfig::from_definition(path, &definition).unwrap();
        config.validate_dispatch().unwrap();
        crate::linear_adapter::IssueAdapter::from_config(&config.tracker)
            .unwrap()
            .ensure_projection_lanes(&config.tracker, &config.linear)
            .await
            .unwrap();
        assert_eq!(config.tracker.kind, "fugue-ims");
        assert_eq!(
            config
                .kapellmeister
                .shadow_mode
                .authority
                .safe_selectors
                .repositories,
            vec![rendered.repository]
        );
        assert!(config.mob.templates.values().all(|template| {
            template
                .members
                .iter()
                .all(|member| member.agent.model.as_deref() == Some("gpt-5.5"))
        }));
        assert!(!workflow.contains("api-token"));
        assert!(ensemble.contains(rendered.checkout.to_str().unwrap()));
    }
}
