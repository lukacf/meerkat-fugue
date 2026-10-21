use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::{HooksConfig, WorkspaceConfig};
use crate::error::FugueError;
use crate::tools::{
    BranchName, CleanWorkspaceInput, CleanWorkspaceOutput, CreateWorkspaceInput,
    CreateWorkspaceOutput, FugueToolError, FugueToolErrorCode, FugueToolOperation, FugueToolOutput,
    FugueToolResult, GitHubRepoRef, GitRef, GitSha, InspectWorkspaceInput, InspectWorkspaceOutput,
    LinearIssueRef, RepositorySetupStatus, RetryAdvice, RunRepositorySetupInput,
    RunRepositorySetupOutput, ToolExecutionMetadata, WorkspaceKey, WorkspaceSnapshot,
};
use crate::workspace::{WorkspaceInspection, WorkspaceManager};

#[derive(Debug, Clone)]
pub struct WorkspaceToolHost {
    manager: WorkspaceManager,
}

impl WorkspaceToolHost {
    pub fn new(workspace: &WorkspaceConfig, hooks: &HooksConfig) -> Self {
        Self {
            manager: WorkspaceManager::new(workspace, hooks),
        }
    }

    pub async fn create_workspace(&self, input: CreateWorkspaceInput) -> FugueToolResult {
        let operation = FugueToolOperation::WorkspaceCreateWorkspace;
        let started_at = Utc::now();
        let result = self
            .manager
            .inspect_workspace_key(&input.workspace_key.0)
            .and_then(|existing| match existing {
                Some(inspection) if input.reuse_existing => Ok(WorkspaceCreate::Reused(inspection)),
                Some(_) => Err(FugueError::Workspace(format!(
                    "workspace already exists and reuse_existing=false: {}",
                    input.workspace_key.0
                ))),
                None => Ok(WorkspaceCreate::Missing),
            });
        let output = match result {
            Ok(WorkspaceCreate::Reused(inspection)) => {
                let provenance = WorkspaceProvenance::from_input(&input);
                validate_provenance(&inspection, &provenance).map(|provenance| {
                    CreateWorkspaceOutput {
                        workspace: snapshot_from_inspection(inspection, provenance.repo),
                        created: false,
                    }
                })
            }
            Ok(WorkspaceCreate::Missing) => self
                .manager
                .create_for_workspace_key(&input.workspace_key.0)
                .await
                .and_then(|info| {
                    self.manager
                        .inspect_workspace_key(&info.workspace_key)?
                        .ok_or_else(|| {
                            FugueError::Workspace(format!(
                                "created workspace disappeared before inspection: {}",
                                info.path.display()
                            ))
                        })
                        .and_then(|inspection| {
                            let provenance = WorkspaceProvenance::from_input(&input);
                            write_provenance(&inspection.path, &provenance)?;
                            Ok(CreateWorkspaceOutput {
                                workspace: snapshot_from_inspection(inspection, provenance.repo),
                                created: info.created_now,
                            })
                        })
                }),
            Err(err) => Err(err),
        };
        result_from_output(
            operation,
            Some(input.idempotency_key),
            started_at,
            output.map(FugueToolOutput::WorkspaceCreateWorkspace),
        )
    }

    pub fn inspect_workspace(&self, input: InspectWorkspaceInput) -> FugueToolResult {
        let operation = FugueToolOperation::WorkspaceInspectWorkspace;
        let started_at = Utc::now();
        let output = self
            .manager
            .inspect_workspace_key(&input.workspace_key.0)
            .and_then(|inspection| {
                let workspace = inspection
                    .map(|inspection| {
                        read_provenance(&inspection.path)
                            .map(|provenance| snapshot_from_inspection(inspection, provenance.repo))
                    })
                    .transpose()?;
                Ok(InspectWorkspaceOutput { workspace })
            });
        result_from_output(
            operation,
            None,
            started_at,
            output.map(FugueToolOutput::WorkspaceInspectWorkspace),
        )
    }

    pub async fn run_repository_setup(&self, input: RunRepositorySetupInput) -> FugueToolResult {
        let operation = FugueToolOperation::WorkspaceRunRepositorySetup;
        let started_at = Utc::now();
        if let Some(profile) = input.setup_profile.as_deref() {
            let output = Err(FugueError::InvalidConfig(format!(
                "setup_profile is not supported by this workspace host: {profile}"
            )));
            return result_from_output(
                operation,
                Some(input.idempotency_key),
                started_at,
                output.map(FugueToolOutput::WorkspaceRunRepositorySetup),
            );
        }
        let output = self
            .manager
            .run_repository_setup(&input.workspace_key.0)
            .await
            .map(|ran| RunRepositorySetupOutput {
                workspace_key: input.workspace_key.clone(),
                status: if ran {
                    RepositorySetupStatus::Succeeded
                } else {
                    RepositorySetupStatus::Skipped
                },
                exit_code: None,
                log_excerpt: None,
            });
        result_from_output(
            operation,
            Some(input.idempotency_key),
            started_at,
            output.map(FugueToolOutput::WorkspaceRunRepositorySetup),
        )
    }

    pub async fn clean_workspace(&self, input: CleanWorkspaceInput) -> FugueToolResult {
        let operation = FugueToolOperation::WorkspaceCleanWorkspace;
        let started_at = Utc::now();
        let output = self
            .manager
            .remove_workspace_key(&input.workspace_key.0, input.require_clean_git)
            .await
            .map(|cleaned| CleanWorkspaceOutput {
                cleaned,
                workspace_key: input.workspace_key.clone(),
            });
        result_from_output(
            operation,
            Some(input.idempotency_key),
            started_at,
            output.map(FugueToolOutput::WorkspaceCleanWorkspace),
        )
    }
}

enum WorkspaceCreate {
    Missing,
    Reused(WorkspaceInspection),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceProvenance {
    repo: GitHubRepoRef,
    issue: LinearIssueRef,
    base_ref: GitRef,
}

impl WorkspaceProvenance {
    fn from_input(input: &CreateWorkspaceInput) -> Self {
        Self {
            repo: input.repo.clone(),
            issue: input.issue.clone(),
            base_ref: input.base_ref.clone(),
        }
    }
}

const PROVENANCE_FILE_NAME: &str = "fugue-workspace.json";

fn provenance_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(
        ".{}.{PROVENANCE_FILE_NAME}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("workspace")
    ))
}

fn read_provenance(path: &Path) -> Result<WorkspaceProvenance, FugueError> {
    let file_path = provenance_path(path);
    let content = fs::read_to_string(&file_path).map_err(|err| {
        FugueError::Workspace(format!(
            "workspace provenance missing or unreadable at {}: {err}",
            file_path.display()
        ))
    })?;
    serde_json::from_str(&content).map_err(|err| {
        FugueError::Workspace(format!(
            "workspace provenance invalid at {}: {err}",
            file_path.display()
        ))
    })
}

fn write_provenance(path: &Path, provenance: &WorkspaceProvenance) -> Result<(), FugueError> {
    let file_path = provenance_path(path);
    let content = serde_json::to_string_pretty(provenance).map_err(|err| {
        FugueError::Workspace(format!("failed to encode workspace provenance: {err}"))
    })?;
    fs::write(&file_path, content).map_err(|err| {
        FugueError::Workspace(format!(
            "failed to write workspace provenance at {}: {err}",
            file_path.display()
        ))
    })
}

fn validate_provenance(
    inspection: &WorkspaceInspection,
    expected: &WorkspaceProvenance,
) -> Result<WorkspaceProvenance, FugueError> {
    let actual = read_provenance(&inspection.path)?;
    if &actual != expected {
        return Err(FugueError::Workspace(format!(
            "workspace provenance conflict for {}: existing repo/base_ref/issue do not match requested values",
            inspection.workspace_key
        )));
    }
    Ok(actual)
}

fn snapshot_from_inspection(
    inspection: WorkspaceInspection,
    repo: GitHubRepoRef,
) -> WorkspaceSnapshot {
    WorkspaceSnapshot {
        workspace_key: WorkspaceKey(inspection.workspace_key),
        path: inspection.path.display().to_string(),
        repo,
        current_branch: inspection.current_branch.map(BranchName),
        head_sha: inspection.head_sha.map(GitSha),
        dirty: inspection.dirty,
        exists: true,
    }
}

fn result_from_output(
    operation: FugueToolOperation,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
    started_at: chrono::DateTime<Utc>,
    result: Result<FugueToolOutput, FugueError>,
) -> FugueToolResult {
    let metadata = ToolExecutionMetadata {
        started_at: Some(started_at),
        completed_at: Some(Utc::now()),
        ..ToolExecutionMetadata::default()
    };
    match result {
        Ok(output) => FugueToolResult::Ok { output, metadata },
        Err(err) => FugueToolResult::Err {
            error: tool_error(operation, err, idempotency_key),
            metadata,
        },
    }
}

fn tool_error(
    operation: FugueToolOperation,
    err: FugueError,
    idempotency_key: Option<crate::tools::IdempotencyKey>,
) -> FugueToolError {
    let code = match &err {
        FugueError::InvalidWorkspaceCwd(_) | FugueError::InvalidConfig(_) => {
            FugueToolErrorCode::InvalidInput
        }
        FugueError::HookFailed(message) if message.contains("timed out") => {
            FugueToolErrorCode::Timeout
        }
        FugueError::HookFailed(_) => FugueToolErrorCode::ExecutionFailed,
        FugueError::Workspace(message)
            if message.contains("already exists")
                || message.contains("uncommitted git changes")
                || message.contains("provenance") =>
        {
            FugueToolErrorCode::PreconditionFailed
        }
        FugueError::Workspace(_) => FugueToolErrorCode::ExecutionFailed,
        _ => FugueToolErrorCode::ExecutionFailed,
    };
    let retry = match code {
        FugueToolErrorCode::InvalidInput | FugueToolErrorCode::PreconditionFailed => {
            RetryAdvice::DoNotRetry
        }
        FugueToolErrorCode::Timeout => RetryAdvice::SafeToRetry,
        _ => RetryAdvice::SafeToRetry,
    };
    FugueToolError {
        operation,
        code,
        message: err.to_string(),
        retry,
        idempotency_key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HooksConfig, WorkspaceConfig};
    use crate::tools::{
        GitHubOwner, GitHubRepoName, GitHubRepoRef, GitRef, IdempotencyKey, LinearIssueIdentifier,
        LinearIssueRef,
    };
    use std::fs;

    fn host(temp: &tempfile::TempDir, hooks: HooksConfig) -> WorkspaceToolHost {
        WorkspaceToolHost::new(
            &WorkspaceConfig {
                root: temp.path().join("root"),
            },
            &hooks,
        )
    }

    fn hooks() -> HooksConfig {
        HooksConfig {
            after_create: None,
            before_run: None,
            after_run: None,
            before_remove: None,
            timeout_ms: 10_000,
        }
    }

    fn create_input(workspace_key: &str, reuse_existing: bool) -> CreateWorkspaceInput {
        CreateWorkspaceInput {
            repo: GitHubRepoRef {
                host: None,
                owner: GitHubOwner("lukacf".to_string()),
                name: GitHubRepoName("meerkat-fugue".to_string()),
            },
            issue: LinearIssueRef::Identifier {
                identifier: LinearIssueIdentifier("LUC-137".to_string()),
            },
            base_ref: GitRef("origin/main".to_string()),
            workspace_key: WorkspaceKey(workspace_key.to_string()),
            reuse_existing,
            idempotency_key: IdempotencyKey(format!("create:{workspace_key}")),
        }
    }

    #[tokio::test]
    async fn create_workspace_is_idempotent_and_reuses_existing_directory() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());

        let first = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;
        let first_path = match first {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCreateWorkspace(output),
                ..
            } => {
                assert!(output.created);
                output.workspace.path
            }
            other => panic!("unexpected result: {other:?}"),
        };

        let second = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;
        match second {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCreateWorkspace(output),
                ..
            } => {
                assert!(!output.created);
                assert_eq!(output.workspace.path, first_path);
                assert!(output.workspace.exists);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_workspace_can_reject_existing_workspace_without_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;

        match tool_host
            .create_workspace(create_input("LUC-137", false))
            .await
        {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::PreconditionFailed);
                assert_eq!(error.retry, RetryAdvice::DoNotRetry);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_workspace_key_returns_structured_error() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());

        match tool_host
            .create_workspace(create_input("../escape", true))
            .await
        {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(
                    error.operation,
                    FugueToolOperation::WorkspaceCreateWorkspace
                );
                assert_eq!(error.code, FugueToolErrorCode::InvalidInput);
                assert_eq!(error.retry, RetryAdvice::DoNotRetry);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reused_workspace_rejects_mismatched_provenance() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;
        let mut conflicting = create_input("LUC-137", true);
        conflicting.repo = GitHubRepoRef {
            host: None,
            owner: GitHubOwner("other".to_string()),
            name: GitHubRepoName("repo".to_string()),
        };

        match tool_host.create_workspace(conflicting).await {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::PreconditionFailed);
                assert_eq!(error.retry, RetryAdvice::DoNotRetry);
                assert!(error.message.contains("provenance"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn inspect_workspace_returns_persisted_repo_provenance() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let input = create_input("LUC-137", true);
        let expected_repo = input.repo.clone();
        let _ = tool_host.create_workspace(input).await;

        match tool_host.inspect_workspace(InspectWorkspaceInput {
            workspace_key: WorkspaceKey("LUC-137".to_string()),
        }) {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceInspectWorkspace(output),
                ..
            } => {
                let workspace = output.workspace.expect("workspace should exist");
                assert_eq!(workspace.repo, expected_repo);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn inspect_workspace_rejects_unprovenanced_existing_directory() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        fs::create_dir_all(temp.path().join("root").join("LUC-137")).unwrap();

        match tool_host.inspect_workspace(InspectWorkspaceInput {
            workspace_key: WorkspaceKey("LUC-137".to_string()),
        }) {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::PreconditionFailed);
                assert!(error.message.contains("provenance"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_default_setup_profile_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;

        match tool_host
            .run_repository_setup(RunRepositorySetupInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                setup_profile: Some("custom".to_string()),
                idempotency_key: IdempotencyKey("setup:LUC-137".to_string()),
            })
            .await
        {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::InvalidInput);
                assert_eq!(error.retry, RetryAdvice::DoNotRetry);
                assert!(error.message.contains("setup_profile"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn setup_hook_failure_propagates_as_execution_failed() {
        let temp = tempfile::tempdir().unwrap();
        let mut hooks = hooks();
        hooks.before_run = Some("echo nope >&2; exit 7".to_string());
        let tool_host = host(&temp, hooks);
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;

        match tool_host
            .run_repository_setup(RunRepositorySetupInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                setup_profile: None,
                idempotency_key: IdempotencyKey("setup:LUC-137".to_string()),
            })
            .await
        {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(
                    error.operation,
                    FugueToolOperation::WorkspaceRunRepositorySetup
                );
                assert_eq!(error.code, FugueToolErrorCode::ExecutionFailed);
                assert!(error.message.contains("before_run"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn clean_workspace_is_replay_safe() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;

        let first = tool_host
            .clean_workspace(CleanWorkspaceInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                require_clean_git: false,
                idempotency_key: IdempotencyKey("clean:LUC-137".to_string()),
            })
            .await;
        match first {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCleanWorkspace(output),
                ..
            } => assert!(output.cleaned),
            other => panic!("unexpected result: {other:?}"),
        }

        let second = tool_host
            .clean_workspace(CleanWorkspaceInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                require_clean_git: false,
                idempotency_key: IdempotencyKey("clean:LUC-137".to_string()),
            })
            .await;
        match second {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCleanWorkspace(output),
                ..
            } => assert!(!output.cleaned),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn provenance_metadata_does_not_dirty_git_workspace_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let result = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;
        let workspace_path = match result {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCreateWorkspace(output),
                ..
            } => PathBuf::from(output.workspace.path),
            other => panic!("unexpected result: {other:?}"),
        };
        run_git(&workspace_path, &["init"]);
        run_git(
            &workspace_path,
            &["config", "user.email", "fugue@example.invalid"],
        );
        run_git(&workspace_path, &["config", "user.name", "Fugue Test"]);
        fs::write(workspace_path.join("README.md"), "# clean\n").unwrap();
        run_git(&workspace_path, &["add", "README.md"]);
        run_git(&workspace_path, &["commit", "-m", "initial"]);

        let status = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&workspace_path)
            .output()
            .unwrap();
        assert!(status.status.success());
        assert_eq!(String::from_utf8_lossy(&status.stdout), "");

        match tool_host
            .clean_workspace(CleanWorkspaceInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                require_clean_git: true,
                idempotency_key: IdempotencyKey("clean:LUC-137".to_string()),
            })
            .await
        {
            FugueToolResult::Ok {
                output: FugueToolOutput::WorkspaceCleanWorkspace(output),
                ..
            } => assert!(output.cleaned),
            other => panic!("unexpected result: {other:?}"),
        }
        assert!(!workspace_path.exists());
    }

    #[tokio::test]
    async fn clean_workspace_require_clean_git_blocks_dirty_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let tool_host = host(&temp, hooks());
        let _ = tool_host
            .create_workspace(create_input("LUC-137", true))
            .await;
        let workspace_path = temp.path().join("root").join("LUC-137");
        std::process::Command::new("git")
            .arg("init")
            .current_dir(&workspace_path)
            .output()
            .unwrap();
        fs::write(workspace_path.join("dirty.txt"), "dirty").unwrap();

        match tool_host
            .clean_workspace(CleanWorkspaceInput {
                workspace_key: WorkspaceKey("LUC-137".to_string()),
                require_clean_git: true,
                idempotency_key: IdempotencyKey("clean:LUC-137".to_string()),
            })
            .await
        {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::PreconditionFailed);
                assert!(workspace_path.exists());
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    fn run_git(workspace: &Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(workspace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    #[test]
    fn inspect_workspace_rejects_symlink_escape() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join("LUC-137")).unwrap();
        let tool_host = host(&temp, hooks());

        match tool_host.inspect_workspace(InspectWorkspaceInput {
            workspace_key: WorkspaceKey("LUC-137".to_string()),
        }) {
            FugueToolResult::Err { error, .. } => {
                assert_eq!(error.code, FugueToolErrorCode::InvalidInput);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
