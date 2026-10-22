use crate::config::ServiceConfig;
use crate::domain::{Issue, WorkspaceInfo, sanitize_workspace_key};
use crate::error::{FugueError, Result};
use crate::tools::{
    CleanWorkspaceInput, CreateWorkspaceInput, FugueToolError, FugueToolOutput, FugueToolResult,
    GitHubOwner, GitHubRepoName, GitHubRepoRef, GitRef, IdempotencyKey, LinearIssueId,
    LinearIssueIdentifier, LinearIssueRef, RunRepositorySetupInput, WorkspaceKey,
};
use crate::workspace::WorkspaceManager;
use crate::workspace_tools::WorkspaceToolHost;

#[derive(Debug, Clone)]
pub struct FugueToolLayer {
    workspace: WorkspaceToolHost,
}

impl FugueToolLayer {
    pub fn new(config: &ServiceConfig) -> Self {
        Self {
            workspace: WorkspaceToolHost::for_service(config),
        }
    }

    pub async fn create_workspace_for_issue(&self, issue: &Issue) -> Result<WorkspaceInfo> {
        let workspace_key = workspace_key_for_issue(issue);
        let result = self
            .workspace
            .create_workspace(CreateWorkspaceInput {
                repo: workspace_provenance_repo(),
                issue: LinearIssueRef::Identifier {
                    identifier: LinearIssueIdentifier(issue.identifier.clone()),
                },
                base_ref: workspace_provenance_base_ref(),
                workspace_key: WorkspaceKey(workspace_key.clone()),
                reuse_existing: true,
                idempotency_key: IdempotencyKey(format!("workspace.create:{workspace_key}")),
            })
            .await;

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCreateWorkspace(output),
                ..
            } => Ok(WorkspaceInfo {
                path: output.workspace.path.into(),
                workspace_key: output.workspace.workspace_key.0,
                created_now: output.created,
            }),
            FugueToolResult::Err { error, .. } => Err(tool_error_to_fugue_error(error)),
            other => Err(FugueError::Workspace(format!(
                "workspace.create_workspace returned unexpected output: {other:?}"
            ))),
        }
    }

    pub async fn run_repository_setup_for_issue(&self, issue: &Issue) -> Result<()> {
        let workspace_key = workspace_key_for_issue(issue);
        let result = self
            .workspace
            .run_repository_setup(RunRepositorySetupInput {
                workspace_key: WorkspaceKey(workspace_key.clone()),
                setup_profile: None,
                idempotency_key: IdempotencyKey(format!("workspace.setup:{workspace_key}")),
            })
            .await;

        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceRunRepositorySetup(_),
                ..
            } => Ok(()),
            FugueToolResult::Err { error, .. } => Err(tool_error_to_fugue_error(error)),
            other => Err(FugueError::Workspace(format!(
                "workspace.run_repository_setup returned unexpected output: {other:?}"
            ))),
        }
    }

    pub async fn clean_workspace_for_issue_identifier(
        &self,
        issue_identifier: &str,
    ) -> Result<bool> {
        self.clean_workspace_key(&sanitize_workspace_key(issue_identifier), false)
            .await
    }

    pub async fn clean_workspace_for_issue(&self, issue: &Issue) -> Result<bool> {
        self.clean_workspace_key(&workspace_key_for_issue(issue), false)
            .await
    }

    async fn clean_workspace_key(
        &self,
        workspace_key: &str,
        require_clean_git: bool,
    ) -> Result<bool> {
        let result = self
            .workspace
            .clean_workspace(CleanWorkspaceInput {
                workspace_key: WorkspaceKey(workspace_key.to_string()),
                require_clean_git,
                idempotency_key: IdempotencyKey(format!("workspace.clean:{workspace_key}")),
            })
            .await;
        match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCleanWorkspace(output),
                ..
            } => Ok(output.cleaned),
            FugueToolResult::Err { error, .. } => Err(tool_error_to_fugue_error(error)),
            other => Err(FugueError::Workspace(format!(
                "workspace.clean_workspace returned unexpected output: {other:?}"
            ))),
        }
    }
}

pub fn workspace_key_for_issue(issue: &Issue) -> String {
    sanitize_workspace_key(&issue.identifier)
}

pub fn workspace_provenance_repo() -> GitHubRepoRef {
    GitHubRepoRef {
        host: None,
        owner: GitHubOwner("local".to_string()),
        name: GitHubRepoName("hook-managed-workspace".to_string()),
    }
}

pub fn workspace_provenance_base_ref() -> GitRef {
    GitRef("hook-managed".to_string())
}

pub fn tool_error_to_fugue_error(error: FugueToolError) -> FugueError {
    FugueError::Workspace(format!(
        "Fugue tool {} failed with {:?}: {} (retry={:?}, idempotency_key={})",
        error.operation,
        error.code,
        error.message,
        error.retry,
        error
            .idempotency_key
            .as_ref()
            .map(|key| key.0.as_str())
            .unwrap_or("<none>")
    ))
}

#[allow(dead_code)]
pub fn linear_issue_ref_for_issue(issue: &Issue) -> LinearIssueRef {
    if issue.id.is_empty() {
        LinearIssueRef::Identifier {
            identifier: LinearIssueIdentifier(issue.identifier.clone()),
        }
    } else {
        LinearIssueRef::Id {
            id: LinearIssueId(issue.id.clone()),
        }
    }
}

pub async fn clean_workspace_for_issue_identifier_best_effort(
    config: &ServiceConfig,
    issue_identifier: &str,
) {
    let tool_layer = FugueToolLayer::new(config);
    if let Err(err) = tool_layer
        .clean_workspace_for_issue_identifier(issue_identifier)
        .await
    {
        tracing::warn!(workspace_key = %sanitize_workspace_key(issue_identifier), error = %err, "workspace cleanup tool failed_ignored");
    }
}

pub async fn after_run_if_workspace_exists(config: &ServiceConfig, issue_identifier: &str) {
    let manager = WorkspaceManager::for_service(config);
    let Ok(path) = manager.path_for_issue(issue_identifier) else {
        return;
    };
    if manager.validate_agent_cwd(&path).is_ok() {
        manager.after_run_best_effort(&path).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ServiceConfig, WorkflowDefinition};
    use chrono::Utc;
    use serde_json::json;

    fn issue(identifier: &str) -> Issue {
        Issue {
            id: "issue-id".to_string(),
            identifier: identifier.to_string(),
            title: "Fix it".to_string(),
            description: None,
            priority: None,
            state: "Todo".to_string(),
            branch_name: None,
            url: None,
            labels: vec![],
            blocked_by: vec![],
            attachments: vec![],
            created_at: Some(Utc::now()),
            updated_at: None,
        }
    }

    fn config_with_workspace(root: &std::path::Path) -> ServiceConfig {
        let definition = WorkflowDefinition {
            config: json!({
                "tracker": {"kind": "linear", "api_key": "token", "project_slug": "project"},
                "workspace": {"root": root.display().to_string()},
                "hooks": {"timeout_ms": 10_000}
            })
            .as_object()
            .unwrap()
            .clone(),
            prompt_template: "hi".to_string(),
        };
        ServiceConfig::from_definition(root.join("fugue.yaml"), &definition).unwrap()
    }

    #[tokio::test]
    async fn agent_workspace_create_routes_through_workspace_tool_with_provenance() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspaces");
        let config = config_with_workspace(&root);
        let layer = FugueToolLayer::new(&config);

        let first = layer
            .create_workspace_for_issue(&issue("ABC-1"))
            .await
            .unwrap();
        let second = layer
            .create_workspace_for_issue(&issue("ABC-1"))
            .await
            .unwrap();

        assert!(first.created_now);
        assert!(!second.created_now);
        assert_eq!(first.path, second.path);
        assert!(first.path.exists());
        let provenance = temp
            .path()
            .join("workspaces")
            .join(".ABC-1.fugue-workspace.json");
        assert!(
            provenance.exists(),
            "workspace tool must persist provenance"
        );
    }

    #[tokio::test]
    async fn issue_workspace_hooks_receive_only_their_repository_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspaces");
        let mut config = config_with_workspace(&root);
        config.meerkat.runtime_connections = Some(std::sync::Arc::new(
            serde_json::from_value(json!({
                "github": {"host": "git.example.com", "credential": {
                    "method": "token", "token": "workspace-token-sentinel"
                }},
                "tracker": {"kind": "jira", "site": "https://jira.example.com",
                    "email": "operator@example.com", "api_token": "tracker-token-sentinel"}
            }))
            .unwrap(),
        ));
        let check = "test \"$GH_HOST\" = git.example.com && test \"$GH_ENTERPRISE_TOKEN\" = workspace-token-sentinel && test -z \"$GH_TOKEN\" && test -z \"$JIRA_API_TOKEN\" && test -z \"$JIRA_EMAIL\"";
        config.hooks.after_create = Some(format!("{check} && touch created-with-repository-auth"));
        config.hooks.before_run = Some(format!("{check} && touch prepared-with-repository-auth"));
        config.hooks.after_run = Some(format!("{check} && touch finished-with-repository-auth"));
        let layer = FugueToolLayer::new(&config);
        let issue = issue("ABC-1");
        let workspace = layer.create_workspace_for_issue(&issue).await.unwrap();
        layer.run_repository_setup_for_issue(&issue).await.unwrap();
        after_run_if_workspace_exists(&config, &issue.identifier).await;
        assert!(
            workspace
                .path
                .join("created-with-repository-auth")
                .is_file()
        );
        assert!(
            workspace
                .path
                .join("prepared-with-repository-auth")
                .is_file()
        );
        assert!(
            workspace
                .path
                .join("finished-with-repository-auth")
                .is_file()
        );
    }

    #[tokio::test]
    async fn workspace_tool_structured_error_is_preserved_for_agent_failures() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspaces");
        let config = config_with_workspace(&root);
        let layer = FugueToolLayer::new(&config);
        std::fs::create_dir_all(root.join("ABC-1")).unwrap();

        let err = layer
            .create_workspace_for_issue(&issue("ABC-1"))
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("workspace.create_workspace"));
        assert!(message.contains("PreconditionFailed"));
        assert!(message.contains("workspace.create:ABC-1"));
        assert!(message.contains("provenance"));
    }

    #[tokio::test]
    async fn terminal_cleanup_routes_through_workspace_tool_and_is_replay_safe() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspaces");
        let config = config_with_workspace(&root);
        let layer = FugueToolLayer::new(&config);
        let issue = issue("ABC-1");
        let _ = layer.create_workspace_for_issue(&issue).await.unwrap();

        assert!(layer.clean_workspace_for_issue(&issue).await.unwrap());
        assert!(!layer.clean_workspace_for_issue(&issue).await.unwrap());
    }
}
