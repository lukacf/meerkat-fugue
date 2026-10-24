use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(test)]
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::OtaUpdateConfig;
use crate::error::{FugueError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtaUpdateStatus {
    Succeeded,
    Failed,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OtaUpdateStep {
    pub name: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub stdout_excerpt: String,
    pub stderr_excerpt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OtaUpdateReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_attempt: Option<String>,
    pub status: OtaUpdateStatus,
    pub branch: String,
    pub remote: String,
    pub repo_path: String,
    pub staging_path: Option<String>,
    pub previous_sha: Option<String>,
    pub fetched_sha: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub steps: Vec<OtaUpdateStep>,
    pub message: String,
}

pub async fn run_ota_update(config: &OtaUpdateConfig) -> Result<OtaUpdateReport> {
    run_ota_update_target(config, None).await
}

pub(crate) async fn run_ota_update_for_revision(
    config: &OtaUpdateConfig,
    expected: &crate::tools::GitSha,
) -> Result<OtaUpdateReport> {
    run_ota_update_target(config, Some(expected)).await
}

async fn run_ota_update_target(
    config: &OtaUpdateConfig,
    expected: Option<&crate::tools::GitSha>,
) -> Result<OtaUpdateReport> {
    let started_at = Utc::now();
    if !config.enabled {
        return Ok(OtaUpdateReport {
            deployment_attempt: None,
            status: OtaUpdateStatus::Disabled,
            branch: config.branch.clone(),
            remote: config.remote.clone(),
            repo_path: config.repo_path.display().to_string(),
            staging_path: None,
            previous_sha: None,
            fetched_sha: None,
            started_at,
            completed_at: Utc::now(),
            steps: Vec::new(),
            message: "OTA updates are disabled by configuration".to_string(),
        });
    }

    let repo_path = canonical_repo_path(&config.repo_path)?;
    ensure_git_repo(&repo_path)?;
    let lock_path = config.lock_path.clone().unwrap_or_else(|| {
        let identity = Sha256::digest(repo_path.as_os_str().as_encoded_bytes());
        repo_path
            .parent()
            .unwrap_or(&repo_path)
            .join(format!(".fugue-ota-{identity:x}.lock"))
    });
    let lock = match OtaUpdateLock::acquire(&lock_path) {
        Ok(lock) => lock,
        Err(OtaUpdateLockError::Busy) => {
            return Ok(report(
                config,
                &repo_path,
                None,
                started_at,
                Vec::new(),
                OtaUpdateStatus::Failed,
                &format!(
                    "another OTA update is already running; process lease is held at {}",
                    lock_path.display()
                ),
                None,
                None,
            ));
        }
        Err(OtaUpdateLockError::Io(err)) => {
            return Err(FugueError::HookFailed(format!(
                "ota_update lock acquire failed at {}: {err}",
                lock_path.display()
            )));
        }
    };

    let result = run_ota_update_locked(config, &repo_path, started_at, expected).await;
    drop(lock);
    result
}

async fn run_ota_update_locked(
    config: &OtaUpdateConfig,
    repo_path: &Path,
    started_at: DateTime<Utc>,
    expected: Option<&crate::tools::GitSha>,
) -> Result<OtaUpdateReport> {
    let mut steps = Vec::new();
    let timeout = Duration::from_millis(config.timeout_ms);
    let mut previous_sha = None;
    let mut fetched_sha = None;
    let mut staging_path = None;

    if config.require_clean {
        let dirty = match git_output(
            repo_path,
            &["status", "--porcelain"],
            timeout,
            &[],
            &mut steps,
            "check_clean_worktree",
        )
        .await
        {
            Ok(output) => output,
            Err(err) => {
                return Ok(failure_report(
                    config,
                    repo_path,
                    staging_path.as_deref(),
                    started_at,
                    steps,
                    previous_sha,
                    fetched_sha,
                    err,
                ));
            }
        };
        if !dirty.trim().is_empty() {
            return Ok(report(
                config,
                repo_path,
                staging_path.as_deref(),
                started_at,
                steps,
                OtaUpdateStatus::Failed,
                "refusing OTA update because the configured repository has uncommitted changes",
                previous_sha,
                fetched_sha,
            ));
        }
    }

    previous_sha = match git_output(
        repo_path,
        &["rev-parse", "HEAD"],
        timeout,
        &[],
        &mut steps,
        "read_current_head",
    )
    .await
    {
        Ok(sha) => non_empty_trimmed(sha),
        Err(err) => {
            return Ok(failure_report(
                config,
                repo_path,
                staging_path.as_deref(),
                started_at,
                steps,
                previous_sha,
                fetched_sha,
                err,
            ));
        }
    };

    if let Err(err) = git_output(
        repo_path,
        &["fetch", "--prune", &config.remote, &config.branch],
        timeout,
        &[],
        &mut steps,
        "fetch_latest_branch",
    )
    .await
    {
        return Ok(failure_report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            previous_sha,
            fetched_sha,
            err,
        ));
    }

    let remote_ref = format!("{}/{}", config.remote, config.branch);
    fetched_sha = match git_output(
        repo_path,
        &["rev-parse", &remote_ref],
        timeout,
        &[],
        &mut steps,
        "resolve_target_revision",
    )
    .await
    {
        Ok(sha) => non_empty_trimmed(sha),
        Err(err) => {
            return Ok(failure_report(
                config,
                repo_path,
                staging_path.as_deref(),
                started_at,
                steps,
                previous_sha,
                fetched_sha,
                err,
            ));
        }
    };

    let Some(target_sha) = fetched_sha.clone() else {
        return Ok(report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            OtaUpdateStatus::Failed,
            "git resolved an empty target revision for OTA update",
            previous_sha,
            fetched_sha,
        ));
    };

    if expected.is_some_and(|expected| expected.0 != target_sha) {
        return Ok(report(
            config,
            repo_path,
            None,
            started_at,
            steps,
            OtaUpdateStatus::Failed,
            "the fetched branch no longer matches the expected merge revision; no build or deployment was started",
            previous_sha,
            fetched_sha,
        ));
    }

    let staging = staging_path_for(repo_path);
    staging_path = Some(staging.clone());
    if staging.exists()
        && let Err(err) = fs::remove_dir_all(&staging)
    {
        return Err(FugueError::HookFailed(format!(
            "ota_update prepare_staging: failed to remove stale staging path {}: {err}",
            staging.display()
        )));
    }

    if let Err(err) = git_output(
        repo_path,
        &[
            "worktree",
            "add",
            "--detach",
            path_arg(&staging).as_str(),
            &target_sha,
        ],
        timeout,
        &[],
        &mut steps,
        "create_staging_worktree",
    )
    .await
    {
        return Ok(failure_report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            previous_sha,
            fetched_sha,
            err,
        ));
    }

    if let Err(err) = shell_output(
        &staging,
        &config.build_command,
        timeout,
        &build_env(config, repo_path, &staging, &previous_sha, &fetched_sha),
        &mut steps,
        "build",
    )
    .await
    {
        cleanup_staging(repo_path, &staging, timeout, &mut steps).await;
        return Ok(failure_report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            previous_sha,
            fetched_sha,
            err,
        ));
    }

    if let Some(health_check_command) = config.health_check_command.as_deref()
        && let Err(err) = shell_output(
            &staging,
            health_check_command,
            timeout,
            &build_env(config, repo_path, &staging, &previous_sha, &fetched_sha),
            &mut steps,
            "health_check",
        )
        .await
    {
        cleanup_staging(repo_path, &staging, timeout, &mut steps).await;
        return Ok(failure_report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            previous_sha,
            fetched_sha,
            err,
        ));
    }

    if let Err(err) = shell_output(
        &staging,
        &config.deploy_command,
        timeout,
        &build_env(config, repo_path, &staging, &previous_sha, &fetched_sha),
        &mut steps,
        "deploy",
    )
    .await
    {
        cleanup_staging(repo_path, &staging, timeout, &mut steps).await;
        return Ok(failure_report(
            config,
            repo_path,
            staging_path.as_deref(),
            started_at,
            steps,
            previous_sha,
            fetched_sha,
            err,
        ));
    }

    cleanup_staging(repo_path, &staging, timeout, &mut steps).await;

    Ok(report(
        config,
        repo_path,
        staging_path.as_deref(),
        started_at,
        steps,
        OtaUpdateStatus::Succeeded,
        "OTA update completed: fetched target revision, built and health-checked staging worktree, and ran deployment replacement command",
        previous_sha,
        fetched_sha,
    ))
}

#[allow(clippy::too_many_arguments)]
fn failure_report(
    config: &OtaUpdateConfig,
    repo_path: &Path,
    staging_path: Option<&Path>,
    started_at: DateTime<Utc>,
    steps: Vec<OtaUpdateStep>,
    previous_sha: Option<String>,
    fetched_sha: Option<String>,
    err: FugueError,
) -> OtaUpdateReport {
    report(
        config,
        repo_path,
        staging_path,
        started_at,
        steps,
        OtaUpdateStatus::Failed,
        &err.to_string(),
        previous_sha,
        fetched_sha,
    )
}

#[allow(clippy::too_many_arguments)]
fn report(
    config: &OtaUpdateConfig,
    repo_path: &Path,
    staging_path: Option<&Path>,
    started_at: DateTime<Utc>,
    steps: Vec<OtaUpdateStep>,
    status: OtaUpdateStatus,
    message: &str,
    previous_sha: Option<String>,
    fetched_sha: Option<String>,
) -> OtaUpdateReport {
    OtaUpdateReport {
        deployment_attempt: None,
        status,
        branch: config.branch.clone(),
        remote: config.remote.clone(),
        repo_path: repo_path.display().to_string(),
        staging_path: staging_path.map(|path| path.display().to_string()),
        previous_sha,
        fetched_sha,
        started_at,
        completed_at: Utc::now(),
        steps,
        message: message.to_string(),
    }
}

async fn git_output(
    repo_path: &Path,
    args: &[&str],
    timeout: Duration,
    envs: &[(String, String)],
    steps: &mut Vec<OtaUpdateStep>,
    name: &str,
) -> Result<String> {
    command_output(repo_path, "git", args, timeout, envs, steps, name).await
}

async fn command_output(
    cwd: &Path,
    program: &str,
    args: &[&str],
    timeout: Duration,
    envs: &[(String, String)],
    steps: &mut Vec<OtaUpdateStep>,
    name: &str,
) -> Result<String> {
    let command_display = display_command(program, args);
    let mut child = Command::new(program);
    child.args(args).current_dir(cwd);
    for (key, value) in envs {
        child.env(key, value);
    }
    run_child(child, command_display, timeout, steps, name).await
}

async fn shell_output(
    cwd: &Path,
    command: &str,
    timeout: Duration,
    envs: &[(String, String)],
    steps: &mut Vec<OtaUpdateStep>,
    name: &str,
) -> Result<String> {
    let mut child = Command::new("sh");
    child.arg("-lc").arg(command).current_dir(cwd);
    for (key, value) in envs {
        child.env(key, value);
    }
    run_child(child, command.to_string(), timeout, steps, name).await
}

async fn run_child(
    child: Command,
    command_display: String,
    timeout: Duration,
    steps: &mut Vec<OtaUpdateStep>,
    name: &str,
) -> Result<String> {
    let output = match crate::process::run_async(child, timeout).await {
        Ok(output) => output,
        Err(error) => {
            steps.push(OtaUpdateStep {
                name: name.to_string(),
                command: command_display,
                exit_code: None,
                stdout_excerpt: String::new(),
                stderr_excerpt: error.to_string(),
            });
            return Err(FugueError::HookFailed(format!(
                "ota_update {name}: {error}"
            )));
        }
    };
    let stdout_excerpt = truncate_utf8(&output.stdout, 4096);
    let stderr_excerpt = truncate_utf8(&output.stderr, 4096);
    steps.push(OtaUpdateStep {
        name: name.to_string(),
        command: command_display,
        exit_code: output.status.code(),
        stdout_excerpt: stdout_excerpt.clone(),
        stderr_excerpt: stderr_excerpt.clone(),
    });
    if !output.status.success() {
        return Err(FugueError::HookFailed(format!(
            "ota_update {name}: exited with status {}; stdout={stdout_excerpt:?} stderr={stderr_excerpt:?}",
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn cleanup_staging(
    repo_path: &Path,
    staging_path: &Path,
    timeout: Duration,
    steps: &mut Vec<OtaUpdateStep>,
) {
    let _ = git_output(
        repo_path,
        &[
            "worktree",
            "remove",
            "--force",
            path_arg(staging_path).as_str(),
        ],
        timeout,
        &[],
        steps,
        "cleanup_staging_worktree",
    )
    .await;
    if staging_path.exists() {
        let _ = fs::remove_dir_all(staging_path);
    }
}

fn canonical_repo_path(repo_path: &Path) -> Result<PathBuf> {
    fs::canonicalize(repo_path).map_err(|err| {
        FugueError::InvalidConfig(format!(
            "ota_update.repo_path {} is not accessible: {err}",
            repo_path.display()
        ))
    })
}

fn ensure_git_repo(repo_path: &Path) -> Result<()> {
    if repo_path.join(".git").exists() {
        return Ok(());
    }
    Err(FugueError::InvalidConfig(format!(
        "ota_update.repo_path {} must point at a git checkout",
        repo_path.display()
    )))
}

fn build_env(
    config: &OtaUpdateConfig,
    live_repo_path: &Path,
    staging_path: &Path,
    previous_sha: &Option<String>,
    fetched_sha: &Option<String>,
) -> Vec<(String, String)> {
    let mut envs = vec![
        ("FUGUE_OTA_BRANCH".to_string(), config.branch.clone()),
        ("FUGUE_OTA_REMOTE".to_string(), config.remote.clone()),
        (
            "FUGUE_OTA_LIVE_REPO_PATH".to_string(),
            live_repo_path.display().to_string(),
        ),
        (
            "FUGUE_OTA_STAGING_PATH".to_string(),
            staging_path.display().to_string(),
        ),
    ];
    if let Some(sha) = previous_sha {
        envs.push(("FUGUE_OTA_PREVIOUS_SHA".to_string(), sha.clone()));
    }
    if let Some(sha) = fetched_sha {
        envs.push(("FUGUE_OTA_FETCHED_SHA".to_string(), sha.clone()));
        envs.push(("FUGUE_OTA_TARGET_SHA".to_string(), sha.clone()));
    }
    envs
}

fn display_command(program: &str, args: &[&str]) -> String {
    let mut rendered = shell_quote(program);
    for arg in args {
        rendered.push(' ');
        rendered.push_str(&shell_quote(arg));
    }
    rendered
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn truncate_utf8(bytes: &[u8], max: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.chars().take(max).collect()
}

fn path_arg(path: &Path) -> String {
    path.display().to_string()
}

fn non_empty_trimmed(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn staging_path_for(repo_path: &Path) -> PathBuf {
    let parent = repo_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    parent.join(format!(
        ".fugue-ota-staging-{}-{now_nanos}",
        std::process::id()
    ))
}

struct OtaUpdateLock {
    _lease: crate::file_lease::FileLease,
}

impl OtaUpdateLock {
    fn acquire(path: &Path) -> std::result::Result<Self, OtaUpdateLockError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(OtaUpdateLockError::Io)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(OtaUpdateLockError::Io)?;
        let lease = crate::file_lease::FileLease::acquire(
            file.try_clone().map_err(OtaUpdateLockError::Io)?,
        )
        .map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => OtaUpdateLockError::Busy,
            std::fs::TryLockError::Error(error) => OtaUpdateLockError::Io(error),
        })?;
        file.set_len(0).map_err(OtaUpdateLockError::Io)?;
        writeln!(
            file,
            "pid={} acquired_at={}",
            std::process::id(),
            Utc::now()
        )
        .map_err(OtaUpdateLockError::Io)?;
        Ok(Self { _lease: lease })
    }
}

enum OtaUpdateLockError {
    Busy,
    Io(std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn git(path: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn sh(path: &Path, script: &str) {
        let status = std::process::Command::new("sh")
            .arg("-lc")
            .arg(script)
            .current_dir(path)
            .status()
            .unwrap();
        assert!(status.success(), "script failed: {script}");
    }

    struct OtaFixture {
        _temp: tempfile::TempDir,
        live: PathBuf,
        deployed: PathBuf,
        lock_path: PathBuf,
    }

    fn ota_fixture() -> OtaFixture {
        let temp = tempfile::tempdir().unwrap();
        let remote = temp.path().join("remote.git");
        let seed = temp.path().join("seed");
        let live = temp.path().join("live");
        fs::create_dir(&seed).unwrap();
        git(&seed, &["init", "-b", "main"]);
        sh(
            &seed,
            "git config user.email test@example.com && git config user.name Test",
        );
        fs::write(seed.join("version.txt"), "one").unwrap();
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-m", "one"]);
        git(temp.path(), &["init", "--bare", remote.to_str().unwrap()]);
        git(&remote, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(
            &seed,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(&seed, &["push", "-u", "origin", "main"]);
        git(
            temp.path(),
            &[
                "clone",
                "--branch",
                "main",
                remote.to_str().unwrap(),
                live.to_str().unwrap(),
            ],
        );
        fs::write(seed.join("version.txt"), "two").unwrap();
        git(&seed, &["add", "."]);
        git(&seed, &["commit", "-m", "two"]);
        git(&seed, &["push", "origin", "main"]);
        let deployed = temp.path().join("deployed.txt");
        let lock_path = temp.path().join("ota.lock");
        OtaFixture {
            _temp: temp,
            live,
            deployed,
            lock_path,
        }
    }

    fn base_config(fixture: &OtaFixture) -> OtaUpdateConfig {
        OtaUpdateConfig {
            enabled: true,
            repo_path: fixture.live.clone(),
            branch: "main".to_string(),
            remote: "origin".to_string(),
            build_command: "test -f version.txt".to_string(),
            health_check_command: Some("grep two version.txt".to_string()),
            deploy_command: format!(
                "cp version.txt {}",
                shell_quote(&fixture.deployed.display().to_string())
            ),
            timeout_ms: 60_000,
            require_clean: true,
            lock_path: Some(fixture.lock_path.clone()),
            trigger_token_env: None,
        }
    }

    #[test]
    fn staging_path_for_never_places_staging_under_live_repo() {
        let nested_repo = PathBuf::from("/tmp/fugue-live-repo");
        let nested_staging = staging_path_for(&nested_repo);
        assert_eq!(nested_staging.parent(), Some(Path::new("/tmp")));
        assert!(!nested_staging.starts_with(&nested_repo));

        let no_parent_repo = PathBuf::from("repo");
        let no_parent_staging = staging_path_for(&no_parent_repo);
        assert_eq!(
            no_parent_staging.parent(),
            Some(std::env::temp_dir().as_path())
        );
        assert!(!no_parent_staging.starts_with(&no_parent_repo));
    }

    #[tokio::test]
    async fn revision_mismatch_stops_before_build_or_deploy() {
        let fixture = ota_fixture();
        let config = base_config(&fixture);
        let old = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&fixture.live)
            .output()
            .unwrap();
        assert!(old.status.success());
        let expected = crate::tools::GitSha(String::from_utf8(old.stdout).unwrap().trim().into());
        let report = run_ota_update_for_revision(&config, &expected)
            .await
            .unwrap();
        assert_eq!(report.status, OtaUpdateStatus::Failed);
        assert!(report.message.contains("expected merge revision"));
        assert!(!report.steps.iter().any(|step| matches!(
            step.name.as_str(),
            "build" | "deploy" | "create_staging_worktree"
        )));
        assert!(!fixture.deployed.exists());
        assert_eq!(
            fs::read_to_string(fixture.live.join("version.txt")).unwrap(),
            "one"
        );
        assert!(OtaUpdateLock::acquire(&fixture.lock_path).is_ok());
    }

    #[tokio::test]
    async fn ota_update_builds_staging_and_runs_deploy_command() {
        let fixture = ota_fixture();
        let config = base_config(&fixture);

        let report = run_ota_update(&config).await.unwrap();

        assert_eq!(report.status, OtaUpdateStatus::Succeeded);
        assert_eq!(report.fetched_sha.as_ref().map(String::len), Some(40));
        assert_eq!(fs::read_to_string(&fixture.deployed).unwrap(), "two");
        assert_eq!(
            fs::read_to_string(fixture.live.join("version.txt")).unwrap(),
            "one"
        );
        assert!(
            report
                .steps
                .iter()
                .any(|step| step.name == "fetch_latest_branch")
        );
        assert!(
            report
                .steps
                .iter()
                .any(|step| step.name == "create_staging_worktree")
        );
        assert!(report.steps.iter().any(|step| step.name == "build"));
        assert!(report.steps.iter().any(|step| step.name == "health_check"));
        assert!(report.steps.iter().any(|step| step.name == "deploy"));
    }

    #[tokio::test]
    async fn issue_deployment_replays_durable_receipt_without_running_the_command_again() {
        let fixture = ota_fixture();
        let mut ota = base_config(&fixture);
        ota.deploy_command = format!(
            "printf 'deployed\\n' >> {}",
            shell_quote(&fixture.deployed.display().to_string())
        );
        let mut config = crate::config::ServiceConfig::from_definition(fixture._temp.path().join("fugue.yaml"),
            &crate::config::WorkflowDefinition {
                config: serde_json::json!({"tracker":{"kind":"fugue-ims", "endpoint":fixture._temp.path().join("issues.sqlite3"),"project_slug":"example/app"}}).as_object().unwrap().clone(), prompt_template:"test".into(),
            }).unwrap();
        config.ota_update = Some(ota);
        config.meerkat.state_root = Some(fixture._temp.path().join("state"));
        crate::fugue_ims::create_issue_for_config(
            &config.tracker,
            crate::fugue_ims::CreateFugueIssue {
                key: crate::ims::ImsIssueKey("TEST-1".into()),
                repo: "example/app".into(),
                title: "Deploy receipt fixture".into(),
                description: None,
                stage: crate::ims::ImsStageName("Deploy".into()),
                labels: Vec::new(),
            },
        )
        .unwrap();
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(fixture._temp.path().join("seed"))
            .output()
            .unwrap();
        assert!(output.status.success());
        let merged = crate::tools::GitSha(String::from_utf8(output.stdout).unwrap().trim().into());
        let mut request: crate::deploy_gate::DeployStartRequest = serde_json::from_value(serde_json::json!({
            "issue":{"kind":"identifier","identifier":"TEST-1"},
            "pull_request":{"repo":{"owner":"example","name":"app"}, "number":1, "title":"Deploy", "url":"https://github.com/example/app/pull/1", "state":"merged", "draft":false,"head_branch":"feature","base_branch":"main","head_sha":"a".repeat(40)},
            "recorded_head_sha":"a".repeat(40), "ci_status":"success", "unresolved_workgraph_blockers":0,"idempotency_key":"deploy-1"
        })).unwrap();
        let first = crate::deployment_receipts::run(&config, &request, &merged)
            .await
            .unwrap()
            .outcome
            .unwrap();
        assert_eq!(first.status, OtaUpdateStatus::Succeeded);
        assert_eq!(first.fetched_sha.as_deref(), Some(merged.0.as_str()));
        for key in ["deploy-1", "different-key"] {
            request.idempotency_key.0 = key.into();
            let repeated = crate::deployment_receipts::run(&config, &request, &merged)
                .await
                .unwrap()
                .outcome
                .unwrap();
            assert_eq!(repeated, first);
        }
        assert_eq!(fs::read_to_string(&fixture.deployed).unwrap(), "deployed\n");
        request.idempotency_key.0 = "operator-retry".into();
        request.retry_deployment = Some(crate::DeploymentRetry {
            attempt_id: first.deployment_attempt.clone().unwrap(),
            reason: "Operator inspected the service and explicitly requested another deployment"
                .into(),
        });
        let retried = crate::deployment_receipts::run(&config, &request, &merged)
            .await
            .unwrap()
            .outcome
            .unwrap();
        assert_ne!(retried.deployment_attempt, first.deployment_attempt);
        assert_eq!(
            crate::deployment_receipts::run(&config, &request, &merged)
                .await
                .unwrap()
                .outcome
                .unwrap(),
            retried
        );
        assert_eq!(
            fs::read_to_string(&fixture.deployed).unwrap(),
            "deployed\ndeployed\n"
        );
        assert_eq!(
            fs::read_to_string(fixture.live.join("version.txt")).unwrap(),
            "one"
        );
    }

    #[tokio::test]
    async fn ota_update_lock_recovery_handles_stale_files_and_default_clean_checkout() {
        for use_default in [false, true] {
            let fixture = ota_fixture();
            let mut config = base_config(&fixture);
            if use_default {
                config.lock_path = None;
            } else {
                fs::write(&fixture.lock_path, "pid=old-process").unwrap();
            }
            let result = run_ota_update(&config).await.unwrap();
            assert_eq!(
                result.status,
                OtaUpdateStatus::Succeeded,
                "{}",
                result.message
            );
            assert_eq!(fs::read_to_string(&fixture.deployed).unwrap(), "two");
        }
    }

    #[tokio::test]
    async fn ota_update_rejects_concurrent_lock() {
        let fixture = ota_fixture();
        let _owner = OtaUpdateLock::acquire(&fixture.lock_path)
            .unwrap_or_else(|_| panic!("acquire fixture lease"));
        let config = base_config(&fixture);

        let report = run_ota_update(&config).await.unwrap();

        assert_eq!(report.status, OtaUpdateStatus::Failed);
        assert!(report.message.contains("already running"));
        assert!(!fixture.deployed.exists());
    }

    #[tokio::test]
    async fn ota_update_build_failure_does_not_deploy_or_mutate_live_checkout() {
        let fixture = ota_fixture();
        let mut config = base_config(&fixture);
        config.build_command = "false".to_string();

        let report = run_ota_update(&config).await.unwrap();

        assert_eq!(report.status, OtaUpdateStatus::Failed);
        assert!(report.steps.iter().any(|step| step.name == "build"));
        assert!(!fixture.deployed.exists());
        assert_eq!(
            fs::read_to_string(fixture.live.join("version.txt")).unwrap(),
            "one"
        );
    }

    #[tokio::test]
    async fn ota_update_health_failure_does_not_deploy_or_mutate_live_checkout() {
        let fixture = ota_fixture();
        let mut config = base_config(&fixture);
        config.health_check_command = Some("grep impossible version.txt".to_string());

        let report = run_ota_update(&config).await.unwrap();

        assert_eq!(report.status, OtaUpdateStatus::Failed);
        assert!(report.steps.iter().any(|step| step.name == "health_check"));
        assert!(!fixture.deployed.exists());
        assert_eq!(
            fs::read_to_string(fixture.live.join("version.txt")).unwrap(),
            "one"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ota_update_shell_child_is_cleaned_up_when_future_is_cancelled() {
        let fixture = ota_fixture();
        let pid_file = fixture.live.parent().unwrap().join("grandchild.pid");
        let command = format!(
            "sleep 30 & echo $! > {}; wait",
            shell_quote(&pid_file.display().to_string())
        );
        let cwd = fixture.live.clone();
        let handle = tokio::spawn(async move {
            let mut steps = Vec::new();
            shell_output(
                &cwd,
                &command,
                Duration::from_secs(60),
                &[],
                &mut steps,
                "cancelled_test",
            )
            .await
        });

        let pid = wait_for_pid_file(&pid_file).await;
        handle.abort();
        let _ = handle.await;

        for _ in 0..30 {
            if !process_exists(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("cancelled ota_update shell left child process {pid} running");
    }

    #[cfg(unix)]
    async fn wait_for_pid_file(path: &Path) -> u32 {
        for _ in 0..50 {
            if let Ok(contents) = fs::read_to_string(path)
                && let Ok(pid) = contents.trim().parse::<u32>()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for pid file {}", path.display());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ota_timeout_and_cancellation_stop_term_resistant_late_effects() {
        for cancel in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ready = temp.path().join("ready.pid");
            let effect = temp.path().join("late-effect");
            let command = format!(
                "sh -c 'trap \"\" TERM; echo $$ > \"$1\"; sleep 1; echo leaked > \"$2\"' helper {} {} & wait",
                shell_quote(&ready.display().to_string()),
                shell_quote(&effect.display().to_string())
            );
            let cwd = temp.path().to_owned();
            let task = tokio::spawn(async move {
                shell_output(
                    &cwd,
                    &command,
                    if cancel {
                        Duration::from_secs(30)
                    } else {
                        Duration::from_millis(300)
                    },
                    &[],
                    &mut Vec::new(),
                    "resistant_child",
                )
                .await
            });
            wait_for_pid_file(&ready).await;
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(
                    task.await
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("timed out")
                );
            }
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(
                !effect.exists(),
                "cleanup must finish before operation ownership is released (cancel={cancel})"
            );
        }
    }

    #[cfg(unix)]
    fn process_exists(pid: u32) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}
