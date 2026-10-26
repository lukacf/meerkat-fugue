//! Durable receipts for external deployment effects, not agent lifecycle state.
//! A process lease excludes concurrent effects; SQLite preserves uncertain and
//! completed attempts across process loss, including lost HTTP responses.

use std::fs::{OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::ServiceConfig;
use crate::deploy_gate::DeployStartRequest;
use crate::file_lease::FileLease;
use crate::ota_update::OtaUpdateReport;
use crate::tools::{GitHubRepoRef, GitSha, IdempotencyKey, InspectIssueInput, LinearIssueId};

/// Explicit repository-operator decision to retry a prior deployment attempt.
/// Use a new request key. An uncertain attempt must be inspected before retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentRetry {
    #[schemars(length(min = 1))]
    pub attempt_id: String,
    #[schemars(length(min = 1))]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentTarget {
    tracker_context: String,
    issue_id: LinearIssueId,
    repo: GitHubRepoRef,
    pull_request: crate::tools::GitHubPullRequestNumber,
    head: GitSha,
    merged: GitSha,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentSpec {
    target: DeploymentTarget,
    procedure_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestBinding {
    spec: DeploymentSpec,
    retry: Option<DeploymentRetry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SavedOutcome {
    Report { report: Box<OtaUpdateReport> },
    Error { message: String },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeploymentError {
    #[error("another deployment is running; wait for its result before retrying")]
    Busy,
    #[error("deployment request key is empty, changed, or conflicts with a recorded attempt")]
    Conflict,
    #[error("deployment request was superseded; inspect the current attempt before retrying")]
    Superseded,
    #[error(
        "deployment attempt {attempt_id} has an uncertain outcome; inspect the deployed service and command effects before an explicit retry with a new key and retry_deployment.attempt_id"
    )]
    RecoveryRequired { attempt_id: String },
    #[error("deployment attempt {attempt_id} failed: {message}")]
    Failed { attempt_id: String, message: String },
    #[error("deployment receipt storage failed: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("deployment receipt file failed: {0}")]
    File(#[from] std::io::Error),
    #[error("deployment receipt encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("deployment receipt is missing or inconsistent")]
    Corrupt,
    #[error("deployment receipt task is unavailable")]
    Unavailable,
    #[error("cannot resolve the deployment issue: {0}")]
    Issue(String),
}

/// Keep this receipt alive through tracker projection. Replacement attempts
/// remain excluded until the caller finishes publishing this result.
pub(crate) struct DeploymentReceipt {
    pub outcome: Result<OtaUpdateReport, DeploymentError>,
    _lease: FileLease,
}

pub(crate) async fn run(
    config: &ServiceConfig,
    request: &DeployStartRequest,
    merged: &GitSha,
) -> Result<DeploymentReceipt, DeploymentError> {
    let ota = config
        .ota_update
        .as_ref()
        .ok_or(DeploymentError::Conflict)?;
    let inspected = crate::linear_adapter::IssueAdapter::from_config(&config.tracker)
        .map_err(|error| DeploymentError::Issue(error.to_string()))?
        .inspect_issue(InspectIssueInput {
            issue: request.issue.clone(),
        })
        .await
        .map_err(|error| DeploymentError::Issue(error.to_string()))?;
    let mut repo = request.pull_request.repo.clone();
    if repo.hostname() == "github.com" {
        repo.host = None;
    }
    repo.owner.0.make_ascii_lowercase();
    repo.name.0.make_ascii_lowercase();
    let spec = DeploymentSpec {
        target: DeploymentTarget {
            tracker_context: config.tracker_context_id(),
            issue_id: inspected.issue.id,
            repo,
            pull_request: request.pull_request.number,
            head: request.pull_request.head_sha.clone(),
            merged: merged.clone(),
        },
        procedure_digest: digest(&(config.deploy.procedure.as_str(), ota))?,
    };
    execute_guarded(
        crate::meta_runtime::meta_runtime_state_path(&config.meerkat),
        spec,
        request.idempotency_key.clone(),
        request.retry_deployment.clone(),
        || crate::ota_update::run_ota_update_for_revision(ota, merged),
    )
    .await
}

fn digest(value: &impl Serialize) -> Result<String, serde_json::Error> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

struct Reservation {
    id: String,
    outcome: Option<SavedOutcome>,
    fresh: bool,
}

async fn execute_guarded<F, Fut>(
    state_path: PathBuf,
    spec: DeploymentSpec,
    key: IdempotencyKey,
    retry: Option<DeploymentRetry>,
    action: F,
) -> Result<DeploymentReceipt, DeploymentError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::error::Result<OtaUpdateReport>>,
{
    if key.0.trim().is_empty()
        || retry.as_ref().is_some_and(|retry| {
            retry.attempt_id.trim().is_empty() || retry.reason.trim().is_empty()
        })
    {
        return Err(DeploymentError::Conflict);
    }
    let path = state_path.join("deployment-receipts.sqlite3");
    let claim_path = path.clone();
    let (lease, reservation) = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&state_path)?;
        let lease = FileLease::acquire(private_file(&state_path.join("deployment-receipts.lock"))?)
            .map_err(|error| match error {
                TryLockError::WouldBlock => DeploymentError::Busy,
                TryLockError::Error(error) => DeploymentError::File(error),
            })?;
        let reservation = reserve(&claim_path, &spec, &key, retry)?;
        Ok::<_, DeploymentError>((lease, reservation))
    })
    .await
    .map_err(|_| DeploymentError::Unavailable)??;
    if !reservation.fresh {
        let outcome = match reservation.outcome {
            Some(outcome) => decode_outcome(reservation.id, outcome),
            None => Err(DeploymentError::RecoveryRequired {
                attempt_id: reservation.id,
            }),
        };
        return Ok(DeploymentReceipt {
            outcome,
            _lease: lease,
        });
    }
    let outcome = match action().await {
        Ok(mut report) => {
            report.deployment_attempt = Some(reservation.id.clone());
            report
                .message
                .push_str(&format!(" Deployment attempt: {}.", reservation.id));
            SavedOutcome::Report {
                report: Box::new(report),
            }
        }
        Err(error) => SavedOutcome::Error {
            message: error.to_string(),
        },
    };
    let saved = outcome.clone();
    let id = reservation.id.clone();
    tokio::task::spawn_blocking(move || {
        let connection = open(&path)?;
        let changed = connection.execute(
            "UPDATE deployment_attempts SET outcome = ?1 WHERE id = ?2 AND outcome IS NULL",
            rusqlite::params![serde_json::to_string(&saved)?, id],
        )?;
        if changed != 1 {
            return Err(DeploymentError::Corrupt);
        }
        Ok::<_, DeploymentError>(())
    })
    .await
    .map_err(|_| DeploymentError::Unavailable)??;
    Ok(DeploymentReceipt {
        outcome: decode_outcome(reservation.id, outcome),
        _lease: lease,
    })
}

fn decode_outcome(id: String, outcome: SavedOutcome) -> Result<OtaUpdateReport, DeploymentError> {
    match outcome {
        SavedOutcome::Report { report } => Ok(*report),
        SavedOutcome::Error { message } => Err(DeploymentError::Failed {
            attempt_id: id,
            message,
        }),
    }
}

fn private_file(path: &Path) -> Result<std::fs::File, std::io::Error> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn open(path: &Path) -> Result<Connection, DeploymentError> {
    drop(private_file(path)?);
    let connection =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS deployment_attempts (
            id TEXT PRIMARY KEY, target TEXT NOT NULL, spec TEXT NOT NULL, outcome TEXT);
        CREATE TABLE IF NOT EXISTS deployment_current (
            target TEXT PRIMARY KEY, id TEXT NOT NULL REFERENCES deployment_attempts(id));
        CREATE TABLE IF NOT EXISTS deployment_requests (
            scope TEXT NOT NULL, request_key TEXT NOT NULL, binding TEXT NOT NULL,
            id TEXT NOT NULL REFERENCES deployment_attempts(id), PRIMARY KEY(scope, request_key));",
    )?;
    Ok(connection)
}

fn reserve(
    path: &Path,
    spec: &DeploymentSpec,
    key: &IdempotencyKey,
    retry: Option<DeploymentRetry>,
) -> Result<Reservation, DeploymentError> {
    let target = digest(&spec.target)?;
    let scope = digest(&(&spec.target.tracker_context, &spec.target.repo))?;
    let binding = RequestBinding {
        spec: spec.clone(),
        retry,
    };
    let mut connection = open(path)?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current: Option<(String, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT c.id, a.spec, a.outcome FROM deployment_current c
         LEFT JOIN deployment_attempts a ON a.id = c.id WHERE c.target = ?1",
            [&target],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let prior: Option<(String, String)> = tx
        .query_row(
            "SELECT id, binding FROM deployment_requests WHERE scope = ?1 AND request_key = ?2",
            rusqlite::params![scope, key.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((id, stored)) = prior {
        if serde_json::from_str::<RequestBinding>(&stored)? != binding {
            return Err(DeploymentError::Conflict);
        }
        let Some((current_id, current_spec, outcome)) = current else {
            return Err(DeploymentError::Corrupt);
        };
        if current_id != id {
            return Err(DeploymentError::Superseded);
        }
        if current_spec
            .as_deref()
            .map(serde_json::from_str::<DeploymentSpec>)
            .transpose()?
            .as_ref()
            != Some(spec)
        {
            return Err(DeploymentError::Corrupt);
        }
        return Ok(Reservation {
            id,
            outcome: outcome
                .map(|body| serde_json::from_str(&body))
                .transpose()?,
            fresh: false,
        });
    }
    let (id, outcome, fresh) = match current {
        Some((id, stored, outcome)) => {
            let stored: DeploymentSpec =
                serde_json::from_str(stored.as_deref().ok_or(DeploymentError::Corrupt)?)?;
            if digest(&stored.target)? != target {
                return Err(DeploymentError::Corrupt);
            }
            match &binding.retry {
                Some(retry) if retry.attempt_id == id => {
                    (uuid::Uuid::new_v4().to_string(), None, true)
                }
                Some(_) => return Err(DeploymentError::Superseded),
                None if stored == *spec => (
                    id,
                    outcome
                        .map(|body| serde_json::from_str(&body))
                        .transpose()?,
                    false,
                ),
                None => return Err(DeploymentError::Conflict),
            }
        }
        None if binding.retry.is_some() => return Err(DeploymentError::Superseded),
        None => (uuid::Uuid::new_v4().to_string(), None, true),
    };
    if fresh {
        tx.execute(
            "INSERT INTO deployment_attempts(id,target,spec) VALUES (?1,?2,?3)",
            rusqlite::params![id, target, serde_json::to_string(spec)?],
        )?;
        tx.execute(
            "INSERT INTO deployment_current(target,id) VALUES (?1,?2)
            ON CONFLICT(target) DO UPDATE SET id=excluded.id",
            rusqlite::params![target, id],
        )?;
    }
    tx.execute(
        "INSERT INTO deployment_requests(scope,request_key,binding,id) VALUES (?1,?2,?3,?4)",
        rusqlite::params![scope, key.0, serde_json::to_string(&binding)?, id],
    )?;
    tx.commit()?;
    Ok(Reservation { id, outcome, fresh })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ota_update::OtaUpdateStatus;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    async fn execute<F, Fut>(
        path: PathBuf,
        spec: DeploymentSpec,
        key: IdempotencyKey,
        retry: Option<DeploymentRetry>,
        action: F,
    ) -> Result<OtaUpdateReport, DeploymentError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<OtaUpdateReport>>,
    {
        execute_guarded(path, spec, key, retry, action)
            .await?
            .outcome
    }

    #[tokio::test]
    async fn receipt_holds_the_lease_until_projection_finishes() {
        let temp = tempfile::tempdir().unwrap();
        let receipt = execute_guarded(
            temp.path().into(),
            spec(),
            IdempotencyKey("first".into()),
            None,
            || async { Ok(report()) },
        )
        .await
        .unwrap();
        assert!(receipt.outcome.is_ok());
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("second".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Busy)
        ));
        drop(receipt);
        execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("first".into()),
            None,
            forbidden,
        )
        .await
        .unwrap();
    }

    fn spec() -> DeploymentSpec {
        DeploymentSpec {
            target: DeploymentTarget {
                tracker_context: "tracker-a".into(),
                issue_id: LinearIssueId("issue-a".into()),
                repo: serde_json::from_str(r#"{"owner":"example","name":"app"}"#).unwrap(),
                pull_request: crate::tools::GitHubPullRequestNumber(1),
                head: GitSha("a".repeat(40)),
                merged: GitSha("b".repeat(40)),
            },
            procedure_digest: "configured-procedure".into(),
        }
    }

    fn report() -> OtaUpdateReport {
        OtaUpdateReport {
            deployment_attempt: None,
            status: OtaUpdateStatus::Succeeded,
            branch: "main".into(),
            remote: "origin".into(),
            repo_path: "/example/app".into(),
            staging_path: None,
            previous_sha: None,
            fetched_sha: Some("b".repeat(40)),
            started_at: chrono::Utc::now(),
            completed_at: chrono::Utc::now(),
            steps: Vec::new(),
            message: "Complete".into(),
        }
    }

    async fn forbidden() -> crate::error::Result<OtaUpdateReport> {
        panic!("deployment must not execute")
    }

    #[tokio::test]
    async fn completed_receipt_survives_reopen_and_new_keys_do_not_repeat_the_effect() {
        let temp = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let action_calls = calls.clone();
        let first = execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("request".into()),
            None,
            || async move {
                action_calls.fetch_add(1, Ordering::SeqCst);
                Ok(report())
            },
        )
        .await
        .unwrap();
        for key in ["request", "another-key", "another-key"] {
            let replay = execute(
                temp.path().into(),
                spec(),
                IdempotencyKey(key.into()),
                None,
                forbidden,
            )
            .await
            .unwrap();
            assert_eq!(replay, first);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for change in ["head", "procedure", "issue"] {
            let mut changed = spec();
            match change {
                "head" => changed.target.head = GitSha("c".repeat(40)),
                "issue" => changed.target.issue_id = LinearIssueId("issue-b".into()),
                _ => changed.procedure_digest = "other-command".into(),
            }
            assert!(matches!(
                execute(
                    temp.path().into(),
                    changed,
                    IdempotencyKey("request".into()),
                    None,
                    forbidden
                )
                .await,
                Err(DeploymentError::Conflict)
            ));
        }
        let mut changed = spec();
        changed.procedure_digest = "other-command".into();
        assert!(matches!(
            execute(
                temp.path().into(),
                changed,
                IdempotencyKey("changed-procedure".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Conflict)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for file in ["deployment-receipts.sqlite3", "deployment-receipts.lock"] {
                assert_eq!(
                    std::fs::metadata(temp.path().join(file))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
    }

    #[tokio::test]
    async fn cancellation_preserves_uncertainty_and_explicit_retry_is_compare_and_set() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().to_owned();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let running = tokio::spawn(execute(
            path,
            spec(),
            IdempotencyKey("initial".into()),
            None,
            || async {
                started_tx.send(()).unwrap();
                std::future::pending().await
            },
        ));
        started_rx.await.unwrap();
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("while-running".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Busy)
        ));
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        let attempt_id = match execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("initial".into()),
            None,
            forbidden,
        )
        .await
        {
            Err(DeploymentError::RecoveryRequired { attempt_id }) => attempt_id,
            other => panic!("uncertain receipt required, got {other:?}"),
        };
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("different-key".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::RecoveryRequired { .. })
        ));
        let retry = DeploymentRetry {
            attempt_id: attempt_id.clone(),
            reason: "Operator verified that the service was not changed; retry authorized".into(),
        };
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("initial".into()),
                Some(retry.clone()),
                forbidden
            )
            .await,
            Err(DeploymentError::Conflict)
        ));
        let completed = execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("retry".into()),
            Some(retry.clone()),
            || async { Ok(report()) },
        )
        .await
        .unwrap();
        assert_eq!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("retry".into()),
                Some(retry.clone()),
                forbidden
            )
            .await
            .unwrap(),
            completed
        );
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("initial".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Superseded)
        ));
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("stale-retry".into()),
                Some(retry),
                forbidden
            )
            .await,
            Err(DeploymentError::Superseded)
        ));
    }

    #[tokio::test]
    async fn failed_effect_is_durable_and_an_inspected_failure_can_retry_with_repaired_configuration()
     {
        let temp = tempfile::tempdir().unwrap();
        let result = execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("failed".into()),
            None,
            || async {
                Err(crate::error::FugueError::HookFailed(
                    "command returned an error".into(),
                ))
            },
        )
        .await;
        let attempt_id = match result {
            Err(DeploymentError::Failed { attempt_id, .. }) => attempt_id,
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("failed".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Failed { .. })
        ));
        let mut repaired = spec();
        repaired.procedure_digest = "repaired-command".into();
        let retry = DeploymentRetry {
            attempt_id,
            reason: "Operator inspected the failed command and authorized the repaired procedure"
                .into(),
        };
        execute(
            temp.path().into(),
            repaired,
            IdempotencyKey("repaired".into()),
            Some(retry),
            || async { Ok(report()) },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn broken_current_pointer_cannot_be_replaced_with_a_fresh_attempt() {
        let temp = tempfile::tempdir().unwrap();
        execute(
            temp.path().into(),
            spec(),
            IdempotencyKey("first".into()),
            None,
            || async { Ok(report()) },
        )
        .await
        .unwrap();
        let connection = Connection::open(temp.path().join("deployment-receipts.sqlite3")).unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys=OFF; DELETE FROM deployment_attempts;")
            .unwrap();
        assert!(matches!(
            execute(
                temp.path().into(),
                spec(),
                IdempotencyKey("new-key".into()),
                None,
                forbidden
            )
            .await,
            Err(DeploymentError::Corrupt)
        ));
    }
}
