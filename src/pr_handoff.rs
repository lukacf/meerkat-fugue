//! Durable issue-to-PR association. This records identity, never approval or CI
//! state. GitHub and the native review gates remain authoritative at action time.
use std::{path::PathBuf, time::Duration};

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::{
    config::ServiceConfig,
    domain::Issue,
    error::{FugueError, Result},
    meta_runtime::meta_runtime_state_path,
    tools::{BranchName, GitHubPullRequestNumber, GitHubPullRequestSnapshot, GitHubRepoRef},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrHandoff {
    pub number: GitHubPullRequestNumber,
    pub head_branch: BranchName,
}

fn storage_error(error: impl std::fmt::Display) -> FugueError {
    FugueError::ResponseError(format!("PR association storage: {error}"))
}

fn path(config: &ServiceConfig) -> PathBuf {
    meta_runtime_state_path(&config.meerkat).join("pr-handoffs.sqlite3")
}

fn key(config: &ServiceConfig, issue: &Issue, repo: &GitHubRepoRef) -> Result<String> {
    // An issue identifier can be reused by another tracker or project. Include
    // its configured account scope and immutable issue ID, plus the repository.
    serde_json::to_string(&(
        &config.tracker.kind,
        &config.tracker.endpoint,
        &config.tracker.project_slug,
        &issue.id,
        repo.hostname(),
        &repo.owner,
        &repo.name,
    ))
    .map_err(storage_error)
}

pub(crate) fn record(
    config: &ServiceConfig,
    issue: &Issue,
    pr: &GitHubPullRequestSnapshot,
) -> Result<()> {
    let path = path(config);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(storage_error)?;
    }
    let db = Connection::open(path).map_err(storage_error)?;
    db.busy_timeout(Duration::from_secs(5))
        .map_err(storage_error)?;
    db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS pr_handoffs (scope TEXT PRIMARY KEY, association TEXT NOT NULL);")
        .map_err(storage_error)?;
    let association = serde_json::to_string(&PrHandoff {
        number: pr.number,
        head_branch: pr.head_branch.clone(),
    })
    .map_err(storage_error)?;
    db.execute(
        "INSERT INTO pr_handoffs (scope, association) VALUES (?1, ?2) ON CONFLICT(scope) DO UPDATE SET association = excluded.association WHERE association != excluded.association",
        rusqlite::params![key(config, issue, &pr.repo)?, association],
    ).map_err(storage_error)?;
    Ok(())
}

pub(crate) fn load(
    config: &ServiceConfig,
    issue: &Issue,
    repo: &GitHubRepoRef,
) -> Result<Option<PrHandoff>> {
    let path = path(config);
    if !path.try_exists().map_err(storage_error)? {
        return Ok(None);
    }
    let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(storage_error)?;
    db.busy_timeout(Duration::from_secs(5))
        .map_err(storage_error)?;
    let saved: Option<String> = db
        .query_row(
            "SELECT association FROM pr_handoffs WHERE scope = ?1",
            [key(config, issue, repo)?],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage_error)?;
    saved
        .map(|body| serde_json::from_str(&body).map_err(storage_error))
        .transpose()
}
