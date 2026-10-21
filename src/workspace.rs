use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::Duration;

use tokio::process::Command;
use tokio::time;
use tracing::{info, warn};

use crate::config::{HooksConfig, WorkspaceConfig};
use crate::domain::{WorkspaceInfo, sanitize_workspace_key};
use crate::error::{FugueError, Result};

#[derive(Debug, Clone)]
pub struct WorkspaceInspection {
    pub path: PathBuf,
    pub workspace_key: String,
    pub current_branch: Option<String>,
    pub head_sha: Option<String>,
    pub upstream_branch: Option<String>,
    pub upstream_head_sha: Option<String>,
    pub dirty: bool,
}

#[derive(Debug, Clone)]
pub struct WorkspaceManager {
    root: PathBuf,
    hooks: HooksConfig,
}

impl WorkspaceManager {
    pub fn new(config: &WorkspaceConfig, hooks: &HooksConfig) -> Self {
        Self {
            root: config.root.clone(),
            hooks: hooks.clone(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path_for_issue(&self, issue_identifier: &str) -> Result<PathBuf> {
        let workspace_key = sanitize_workspace_key(issue_identifier);
        self.path_for_workspace_key(&workspace_key)
    }

    pub fn path_for_workspace_key(&self, workspace_key: &str) -> Result<PathBuf> {
        validate_workspace_key(workspace_key)?;
        let path = clean_path(&self.root.join(workspace_key));
        self.ensure_lexically_under_root(&path)?;
        Ok(path)
    }

    pub async fn create_for_issue(&self, issue_identifier: &str) -> Result<WorkspaceInfo> {
        let workspace_key = sanitize_workspace_key(issue_identifier);
        self.create_for_workspace_key(&workspace_key).await
    }

    pub async fn create_for_workspace_key(&self, workspace_key: &str) -> Result<WorkspaceInfo> {
        fs::create_dir_all(&self.root).map_err(|err| {
            FugueError::Workspace(format!(
                "failed to create workspace root {}: {err}",
                self.root.display()
            ))
        })?;
        let path = self.path_for_workspace_key(workspace_key)?;

        let created_now = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(FugueError::InvalidWorkspaceCwd(format!(
                        "workspace path is a symlink and is not trusted: {}",
                        path.display()
                    )));
                }
                if !metadata.is_dir() {
                    return Err(FugueError::Workspace(format!(
                        "workspace path exists but is not a directory: {}",
                        path.display()
                    )));
                }
                self.ensure_existing_under_root(&path)?;
                false
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&path).map_err(|err| {
                    FugueError::Workspace(format!(
                        "failed to create workspace {}: {err}",
                        path.display()
                    ))
                })?;
                self.ensure_existing_under_root(&path)?;
                true
            }
            Err(err) => {
                return Err(FugueError::Workspace(format!(
                    "failed to inspect workspace {}: {err}",
                    path.display(),
                )));
            }
        };

        if created_now && let Some(script) = &self.hooks.after_create {
            self.run_hook("after_create", script, &path, true).await?;
        }

        Ok(WorkspaceInfo {
            path,
            workspace_key: workspace_key.to_string(),
            created_now,
        })
    }

    pub fn inspect_workspace_key(
        &self,
        workspace_key: &str,
    ) -> Result<Option<WorkspaceInspection>> {
        let path = self.path_for_workspace_key(workspace_key)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(FugueError::Workspace(format!(
                    "failed to inspect workspace {}: {err}",
                    path.display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(FugueError::InvalidWorkspaceCwd(format!(
                "workspace path is a symlink and is not trusted: {}",
                path.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(FugueError::Workspace(format!(
                "workspace path exists but is not a directory: {}",
                path.display()
            )));
        }
        self.ensure_existing_under_root(&path)?;
        let git = inspect_git_status(&path)?;
        Ok(Some(WorkspaceInspection {
            path,
            workspace_key: workspace_key.to_string(),
            current_branch: git.current_branch,
            head_sha: git.head_sha,
            upstream_branch: git.upstream_branch,
            upstream_head_sha: git.upstream_head_sha,
            dirty: git.dirty,
        }))
    }

    pub async fn run_repository_setup(&self, workspace_key: &str) -> Result<bool> {
        let path = self.path_for_workspace_key(workspace_key)?;
        self.validate_agent_cwd(&path)?;
        let Some(script) = &self.hooks.before_run else {
            return Ok(false);
        };
        self.run_hook("before_run", script, &path, true).await?;
        self.validate_agent_cwd(&path)?;
        Ok(true)
    }

    pub async fn remove_workspace_key(
        &self,
        workspace_key: &str,
        require_clean_git: bool,
    ) -> Result<bool> {
        let path = self.path_for_workspace_key(workspace_key)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(FugueError::Workspace(format!(
                    "failed to inspect workspace {}: {err}",
                    path.display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            fs::remove_file(&path).map_err(|err| {
                FugueError::Workspace(format!(
                    "failed to remove workspace symlink {}: {err}",
                    path.display()
                ))
            })?;
            info!(workspace = %path.display(), "workspace symlink cleanup completed");
            return Ok(true);
        }
        if !metadata.is_dir() {
            return Err(FugueError::Workspace(format!(
                "workspace path exists but is not a directory: {}",
                path.display()
            )));
        }
        self.ensure_existing_under_root(&path)?;
        if require_clean_git && inspect_git_status(&path)?.dirty {
            return Err(FugueError::Workspace(format!(
                "workspace has uncommitted git changes: {}",
                path.display()
            )));
        }
        if let Some(script) = &self.hooks.before_remove
            && let Err(err) = self.run_hook("before_remove", script, &path, false).await
        {
            warn!(workspace = %path.display(), error = %err, "hook before_remove failed_ignored");
        }
        fs::remove_dir_all(&path).map_err(|err| {
            FugueError::Workspace(format!(
                "workspace cleanup failed {}: {err}",
                path.display()
            ))
        })?;
        info!(workspace = %path.display(), "workspace cleanup completed");
        Ok(true)
    }

    pub async fn before_run(&self, workspace: &Path) -> Result<()> {
        self.validate_agent_cwd(workspace)?;
        if let Some(script) = &self.hooks.before_run {
            self.run_hook("before_run", script, workspace, true).await?;
        }
        self.validate_agent_cwd(workspace)
    }

    pub async fn after_run_best_effort(&self, workspace: &Path) {
        if let Some(script) = &self.hooks.after_run
            && let Err(err) = self.run_hook("after_run", script, workspace, false).await
        {
            warn!(workspace = %workspace.display(), error = %err, "hook after_run failed_ignored");
        }
    }

    pub async fn remove_for_issue(&self, issue_identifier: &str) {
        let workspace_key = sanitize_workspace_key(issue_identifier);
        if let Err(err) = self.remove_workspace_key(&workspace_key, false).await {
            warn!(workspace_key = %workspace_key, error = %err, "workspace cleanup failed_ignored");
        }
    }

    pub fn validate_agent_cwd(&self, workspace: &Path) -> Result<()> {
        self.ensure_lexically_under_root(workspace)?;
        let metadata = fs::symlink_metadata(workspace).map_err(|err| {
            FugueError::InvalidWorkspaceCwd(format!(
                "failed to inspect workspace {}: {err}",
                workspace.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(FugueError::InvalidWorkspaceCwd(format!(
                "workspace path is a symlink and is not trusted: {}",
                workspace.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(FugueError::InvalidWorkspaceCwd(format!(
                "{} is not a directory",
                workspace.display()
            )));
        }
        self.ensure_existing_under_root(workspace)?;
        Ok(())
    }

    fn ensure_lexically_under_root(&self, path: &Path) -> Result<()> {
        let root = clean_path(&self.root);
        let path = clean_path(path);
        if !path.starts_with(&root) || path == root {
            return Err(FugueError::InvalidWorkspaceCwd(format!(
                "{} is outside workspace root {}",
                path.display(),
                root.display()
            )));
        }
        Ok(())
    }

    fn ensure_existing_under_root(&self, path: &Path) -> Result<()> {
        let root = fs::canonicalize(&self.root).map_err(|err| {
            FugueError::InvalidWorkspaceCwd(format!(
                "failed to canonicalize workspace root {}: {err}",
                self.root.display()
            ))
        })?;
        let path = fs::canonicalize(path).map_err(|err| {
            FugueError::InvalidWorkspaceCwd(format!(
                "failed to canonicalize workspace {}: {err}",
                path.display()
            ))
        })?;
        if !path.starts_with(&root) || path == root {
            return Err(FugueError::InvalidWorkspaceCwd(format!(
                "{} is outside workspace root {}",
                path.display(),
                root.display()
            )));
        }
        Ok(())
    }

    async fn run_hook(
        &self,
        name: &str,
        script: &str,
        workspace: &Path,
        fatal: bool,
    ) -> Result<()> {
        info!(hook = name, workspace = %workspace.display(), "hook starting");
        let child = Command::new("sh")
            .arg("-lc")
            .arg(script)
            .current_dir(workspace)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|err| FugueError::HookFailed(format!("{name}: spawn failed: {err}")))?;
        let timeout = Duration::from_millis(self.hooks.timeout_ms);
        let output = match time::timeout(timeout, child.wait_with_output()).await {
            Ok(result) => result
                .map_err(|err| FugueError::HookFailed(format!("{name}: wait failed: {err}")))?,
            Err(_) => {
                let msg = format!("{name}: timed out after {} ms", self.hooks.timeout_ms);
                if fatal {
                    return Err(FugueError::HookFailed(msg));
                }
                warn!(hook = name, workspace = %workspace.display(), "hook timeout ignored");
                return Ok(());
            }
        };
        if !output.status.success() {
            let stderr = truncate_utf8(&output.stderr, 2048);
            let stdout = truncate_utf8(&output.stdout, 2048);
            let msg = format!(
                "{name}: exited with status {}; stdout={stdout:?} stderr={stderr:?}",
                output.status
            );
            if fatal {
                return Err(FugueError::HookFailed(msg));
            }
            warn!(hook = name, workspace = %workspace.display(), message = %msg, "hook failure ignored");
        } else {
            info!(hook = name, workspace = %workspace.display(), "hook completed");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct GitStatus {
    current_branch: Option<String>,
    head_sha: Option<String>,
    upstream_branch: Option<String>,
    upstream_head_sha: Option<String>,
    dirty: bool,
}

fn inspect_git_status(workspace: &Path) -> Result<GitStatus> {
    let is_inside_work_tree =
        git_output(workspace, &["rev-parse", "--is-inside-work-tree"]).as_deref() == Some("true");
    if !is_inside_work_tree {
        return Ok(GitStatus {
            current_branch: None,
            head_sha: None,
            upstream_branch: None,
            upstream_head_sha: None,
            dirty: false,
        });
    }
    ensure_runtime_git_excludes(workspace)?;
    let head_sha = git_output(workspace, &["rev-parse", "HEAD"]);
    let current_branch = git_output(workspace, &["branch", "--show-current"])
        .and_then(|branch| (!branch.is_empty()).then_some(branch));
    let upstream_branch = git_output(
        workspace,
        &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
    )
    .and_then(|branch| (!branch.is_empty()).then_some(branch));
    let upstream_head_sha = git_output(workspace, &["rev-parse", "@{u}"])
        .and_then(|sha| (!sha.is_empty()).then_some(sha));
    let dirty = git_output(workspace, &["status", "--porcelain"])
        .map(|status| !status.is_empty())
        .unwrap_or(false);
    Ok(GitStatus {
        current_branch,
        head_sha,
        upstream_branch,
        upstream_head_sha,
        dirty,
    })
}

fn ensure_runtime_git_excludes(workspace: &Path) -> Result<()> {
    let Some(exclude_path) = git_output(workspace, &["rev-parse", "--git-path", "info/exclude"])
    else {
        return Ok(());
    };
    let exclude_path = {
        let path = Path::new(exclude_path.trim());
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            workspace.join(path)
        }
    };
    let existing = match fs::read_to_string(&exclude_path) {
        Ok(existing) => existing,
        Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
        Err(err) => {
            return Err(FugueError::Workspace(format!(
                "failed to read git exclude {}: {err}",
                exclude_path.display()
            )));
        }
    };
    let mut updated = existing.clone();
    for runtime_dir in [".rkat/", ".fugue/"] {
        if !existing.lines().any(|line| line.trim() == runtime_dir) {
            if !updated.ends_with('\n') && !updated.is_empty() {
                updated.push('\n');
            }
            updated.push_str(runtime_dir);
            updated.push('\n');
        }
    }
    if updated == existing {
        return Ok(());
    }
    if let Some(parent) = exclude_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            FugueError::Workspace(format!(
                "failed to create git exclude directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    fs::write(&exclude_path, updated).map_err(|err| {
        FugueError::Workspace(format!(
            "failed to write git exclude {}: {err}",
            exclude_path.display()
        ))
    })
}

fn git_output(workspace: &Path, args: &[&str]) -> Option<String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(workspace)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn validate_workspace_key(workspace_key: &str) -> Result<()> {
    if workspace_key.is_empty()
        || sanitize_workspace_key(workspace_key) != workspace_key
        || Path::new(workspace_key).components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir
                    | std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(FugueError::InvalidWorkspaceCwd(format!(
            "invalid workspace key: {workspace_key:?}"
        )));
    }
    Ok(())
}

fn truncate_utf8(bytes: &[u8], limit: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.chars().take(limit).collect()
}

fn clean_path(path: &Path) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                cleaned.pop();
            }
            other => cleaned.push(other.as_os_str()),
        }
    }
    cleaned
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HooksConfig, WorkspaceConfig};

    #[tokio::test]
    async fn creates_and_reuses_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let manager = WorkspaceManager::new(
            &WorkspaceConfig {
                root: temp.path().join("root"),
            },
            &HooksConfig {
                after_create: None,
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );
        let first = manager.create_for_issue("ABC 1").await.unwrap();
        assert!(first.created_now);
        assert_eq!(first.workspace_key, "ABC_1");
        let second = manager.create_for_issue("ABC 1").await.unwrap();
        assert!(!second.created_now);
        manager.validate_agent_cwd(&second.path).unwrap();
    }

    #[tokio::test]
    async fn after_create_hook_runs_once() {
        let temp = tempfile::tempdir().unwrap();
        let manager = WorkspaceManager::new(
            &WorkspaceConfig {
                root: temp.path().join("root"),
            },
            &HooksConfig {
                after_create: Some("echo hi > marker".to_string()),
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );
        let workspace = manager.create_for_issue("ABC-1").await.unwrap();
        assert!(workspace.path.join("marker").exists());
        fs::remove_file(workspace.path.join("marker")).unwrap();
        manager.create_for_issue("ABC-1").await.unwrap();
        assert!(!workspace.path.join("marker").exists());
    }

    #[tokio::test]
    async fn rejects_root_alias_workspace_keys() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        let manager = WorkspaceManager::new(
            &WorkspaceConfig { root: root.clone() },
            &HooksConfig {
                after_create: None,
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );

        assert!(manager.path_for_workspace_key(".").is_err());
        assert!(manager.path_for_workspace_key("..").is_err());
        assert!(manager.remove_workspace_key(".", false).await.is_err());
        assert!(manager.remove_workspace_key("..", false).await.is_err());
        assert!(root.exists());
        assert!(manager.path_for_workspace_key("ABC-1").is_ok());
    }

    #[tokio::test]
    async fn inspection_ignores_runtime_artifact_directories() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let manager = WorkspaceManager::new(
            &WorkspaceConfig { root },
            &HooksConfig {
                after_create: None,
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );
        let workspace = manager.create_for_issue("ABC-1").await.unwrap();
        StdCommand::new("git")
            .args(["init"])
            .current_dir(&workspace.path)
            .status()
            .unwrap();
        fs::create_dir(workspace.path.join(".rkat")).unwrap();
        fs::create_dir(workspace.path.join(".fugue")).unwrap();
        fs::write(workspace.path.join(".rkat").join("mcp.toml"), "").unwrap();
        fs::write(workspace.path.join(".fugue").join("runtime.json"), "{}").unwrap();

        let inspection = manager
            .inspect_workspace_key(&workspace.workspace_key)
            .unwrap()
            .unwrap();

        assert!(!inspection.dirty);
        let exclude = fs::read_to_string(workspace.path.join(".git/info/exclude")).unwrap();
        assert!(exclude.contains(".rkat/"));
        assert!(exclude.contains(".fugue/"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn before_run_revalidates_after_hook() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let script = format!(
            "rm -rf \"$PWD\" && ln -s {} \"$PWD\"",
            outside.path().display()
        );
        let manager = WorkspaceManager::new(
            &WorkspaceConfig { root },
            &HooksConfig {
                after_create: None,
                before_run: Some(script),
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );
        let workspace = manager.create_for_issue("ABC-1").await.unwrap();

        assert!(manager.before_run(&workspace.path).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_workspace_escape() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        fs::create_dir_all(&root).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join("ABC-1")).unwrap();

        let manager = WorkspaceManager::new(
            &WorkspaceConfig { root },
            &HooksConfig {
                after_create: None,
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: 10_000,
            },
        );

        assert!(manager.create_for_issue("ABC-1").await.is_err());
        assert!(
            manager
                .validate_agent_cwd(&manager.root().join("ABC-1"))
                .is_err()
        );
    }
}
