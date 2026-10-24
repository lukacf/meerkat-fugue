//! Canonical review-request intent. Native runs remain the only verdict and
//! lifecycle store. This record arbitrates competing requests before dispatch.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use meerkat_mob::FlowRoleTargets;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::native_review::{ReviewCandidate, ReviewIntent, ReviewProofError};
use crate::tools::IdempotencyKey;

#[derive(Debug, Clone)]
pub(crate) struct ReviewRequestDraft {
    pub candidate: ReviewCandidate,
    pub reviewers: FlowRoleTargets,
    pub task: String,
    pub idempotency_key: IdempotencyKey,
    pub supersedes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestRecord {
    intent: ReviewIntent,
    supersedes: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReviewRequestError {
    #[error(transparent)]
    Invalid(#[from] ReviewProofError),
    #[error("review request storage: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("review request directory: {0}")]
    Directory(#[from] std::io::Error),
    #[error("review request encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("review request key already belongs to a different assignment")]
    KeyConflict,
    #[error("review request was superseded; it cannot be admitted or authorize completion")]
    Superseded,
    #[error("review request changed; explicitly supersede the current request {current:?}")]
    CurrentChanged { current: Option<String> },
    #[error("no current review request exists for this issue and repository")]
    Missing,
    #[error("current review request record is missing or inconsistent")]
    Corrupt,
    #[error("selected reviewer is not an eligible existing repository specialist")]
    IneligibleReviewer,
    #[error(
        "review requires {required} independent specialists, but only {available} are eligible; keep review blocked or explicitly change the repository policy in setup"
    )]
    InsufficientCapacity { required: usize, available: usize },
    #[error("repository runtime or execution history is unavailable")]
    Unavailable,
    #[error(transparent)]
    Admission(#[from] crate::native_review::ReviewAdmissionError),
}

#[derive(Debug, Clone)]
pub(crate) struct ReviewRequestStore {
    path: PathBuf,
}

impl ReviewRequestStore {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Reserve current intent before any native call. Retrying the same request
    /// cannot change its assignment or restore a superseded request to current.
    pub(crate) fn reserve(
        &self,
        draft: ReviewRequestDraft,
        required_reviewers: usize,
    ) -> Result<ReviewIntent, ReviewRequestError> {
        if draft.idempotency_key.0.trim().is_empty()
            || draft
                .supersedes
                .as_deref()
                .is_some_and(|id| id.trim().is_empty())
        {
            return Err(ReviewProofError::InvalidIntent.into());
        }
        let proposed = ReviewIntent {
            schema_version: crate::native_review::SCHEMA.to_owned(),
            round: uuid::Uuid::new_v4().to_string(),
            candidate: draft.candidate,
            reviewers: draft.reviewers,
            task: draft.task,
        };
        proposed.validate(required_reviewers)?;
        let scope = scope(&proposed.candidate)?;
        let mut connection = self.open_for_write()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = current_record(&transaction, &scope)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT record FROM review_requests WHERE scope = ?1 AND request_key = ?2",
                rusqlite::params![scope, draft.idempotency_key.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            let existing: RequestRecord = serde_json::from_str(&existing)?;
            if existing.intent.candidate != proposed.candidate
                || existing.intent.reviewers != proposed.reviewers
                || existing.intent.task != proposed.task
                || existing.supersedes != draft.supersedes
            {
                return Err(ReviewRequestError::KeyConflict);
            }
            if current.as_ref().map(|record| &record.intent) != Some(&existing.intent) {
                return Err(ReviewRequestError::Superseded);
            }
            existing.intent.validate(required_reviewers)?;
            return Ok(existing.intent);
        }
        let current_id = current.map(|record| record.intent.round);
        if current_id != draft.supersedes {
            return Err(ReviewRequestError::CurrentChanged {
                current: current_id,
            });
        }
        let record = RequestRecord {
            intent: proposed,
            supersedes: draft.supersedes,
        };
        transaction.execute(
            "INSERT INTO review_requests(scope, request_key, record) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                scope,
                draft.idempotency_key.0,
                serde_json::to_string(&record)?
            ],
        )?;
        transaction.execute(
            "INSERT INTO review_current(scope, request_key) VALUES (?1, ?2)
             ON CONFLICT(scope) DO UPDATE SET request_key = excluded.request_key",
            rusqlite::params![scope, draft.idempotency_key.0],
        )?;
        transaction.commit()?;
        Ok(record.intent)
    }

    pub(crate) fn current(
        &self,
        candidate: &ReviewCandidate,
    ) -> Result<Option<ReviewIntent>, ReviewRequestError> {
        if !self.path.try_exists()? {
            return Ok(None);
        }
        let connection =
            Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(current_record(&connection, &scope(candidate)?)?.map(|record| record.intent))
    }

    pub(crate) fn require_current(&self, intent: &ReviewIntent) -> Result<(), ReviewRequestError> {
        if self.current(&intent.candidate)?.as_ref() != Some(intent) {
            return Err(ReviewRequestError::Superseded);
        }
        Ok(())
    }

    /// Keep replacement admission excluded during the final merge operation.
    /// This holds only application intent; it never writes a native verdict.
    pub(crate) fn with_current<T>(
        &self,
        intent: &ReviewIntent,
        action: impl FnOnce() -> T,
    ) -> Result<T, ReviewRequestError> {
        if !self.path.try_exists()? {
            return Err(ReviewRequestError::Missing);
        }
        let mut connection =
            Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if current_record(&transaction, &scope(&intent.candidate)?)?
            .map(|record| record.intent)
            .as_ref()
            != Some(intent)
        {
            return Err(ReviewRequestError::Superseded);
        }
        // Dropping this read-only transaction releases the admission lock even
        // when the external action returns an error or unwinds.
        let result = action();
        drop(transaction);
        Ok(result)
    }

    fn open_for_write(&self) -> Result<Connection, ReviewRequestError> {
        if let Some(parent) = self.path.parent().filter(|path| *path != Path::new("")) {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&self.path)?;
        // Concurrent first opens can both hold a read lock while switching
        // journal mode. SQLite returns BUSY immediately for that lock upgrade,
        // even with a busy handler. Retry only this initialization statement,
        // outside a transaction, with a bounded deadline.
        connection.busy_timeout(Duration::ZERO)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match connection.execute_batch("PRAGMA journal_mode=WAL;") {
                Ok(()) => break,
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if error.code == rusqlite::ErrorCode::DatabaseBusy
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        }
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS review_requests (
               scope TEXT NOT NULL, request_key TEXT NOT NULL, record TEXT NOT NULL,
               PRIMARY KEY(scope, request_key));
             CREATE TABLE IF NOT EXISTS review_current (
               scope TEXT PRIMARY KEY, request_key TEXT NOT NULL,
               FOREIGN KEY(scope, request_key) REFERENCES review_requests(scope, request_key));",
        )?;
        Ok(connection)
    }
}

fn scope(candidate: &ReviewCandidate) -> Result<String, serde_json::Error> {
    serde_json::to_string(&(
        &candidate.tracker_context,
        &candidate.issue_id,
        candidate.repo.hostname(),
        candidate.repo.owner.0.to_ascii_lowercase(),
        candidate.repo.name.0.to_ascii_lowercase(),
    ))
}

fn current_record(
    connection: &Connection,
    scope: &str,
) -> Result<Option<RequestRecord>, ReviewRequestError> {
    let body: Option<Option<String>> = connection
        .query_row(
            "SELECT r.record FROM review_current c LEFT JOIN review_requests r
         ON r.scope = c.scope AND r.request_key = c.request_key WHERE c.scope = ?1",
            [scope],
            |row| row.get(0),
        )
        .optional()?;
    match body {
        None => Ok(None),
        Some(None) => Err(ReviewRequestError::Corrupt),
        Some(Some(body)) => {
            let record: RequestRecord = serde_json::from_str(&body)?;
            if self::scope(&record.intent.candidate)? != scope {
                return Err(ReviewRequestError::Corrupt);
            }
            Ok(Some(record))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::{GitHubPullRequestNumber, GitHubRepoRef, GitSha, LinearIssueId};

    fn draft(key: &str) -> ReviewRequestDraft {
        ReviewRequestDraft {
            candidate: ReviewCandidate {
                tracker_context: "project-a".into(),
                issue_id: LinearIssueId("10001".into()),
                issue_identifier: "TEST-1".into(),
                repo: serde_json::from_value::<GitHubRepoRef>(
                    serde_json::json!({"owner":"example","name":"app"}),
                )
                .unwrap(),
                pull_request: GitHubPullRequestNumber(1),
                base_branch: crate::tools::BranchName("main".into()),
                head_sha: GitSha("a".repeat(40)),
            },
            reviewers: FlowRoleTargets::new([(
                "domain",
                vec!["review-a".into(), "review-b".into(), "review-c".into()],
            )])
            .unwrap(),
            task: "Review the candidate".into(),
            idempotency_key: IdempotencyKey(key.into()),
            supersedes: None,
        }
    }

    #[test]
    fn request_replay_survives_restart_without_changing_selection() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("requests.sqlite3");
        let store = ReviewRequestStore::new(&path);
        let first = store.reserve(draft("first"), 3).unwrap();
        assert_eq!(
            ReviewRequestStore::new(path)
                .reserve(draft("first"), 3)
                .unwrap(),
            first
        );
        for change in ["task", "head", "base", "reviewers"] {
            let mut other = draft("first");
            match change {
                "task" => other.task.push_str(" differently"),
                "head" => other.candidate.head_sha = GitSha("b".repeat(40)),
                "base" => other.candidate.base_branch.0 = "release".into(),
                _ => {
                    other.reviewers = FlowRoleTargets::new([(
                        "other-domain",
                        vec!["other-a".into(), "other-b".into(), "other-c".into()],
                    )])
                    .unwrap()
                }
            }
            assert!(matches!(
                store.reserve(other, 3),
                Err(ReviewRequestError::KeyConflict)
            ));
        }
        assert_eq!(store.current(&first.candidate).unwrap(), Some(first));
    }

    #[test]
    fn replacement_requires_compare_and_set_and_old_retries_cannot_restore_it() {
        let temp = tempfile::tempdir().unwrap();
        let store = ReviewRequestStore::new(temp.path().join("requests.sqlite3"));
        let first = store.reserve(draft("first"), 3).unwrap();
        assert!(matches!(
            store.reserve(draft("second"), 3),
            Err(ReviewRequestError::CurrentChanged { .. })
        ));
        let mut second = draft("second");
        second.supersedes = Some(first.round.clone());
        let second = store.reserve(second, 3).unwrap();
        assert_ne!(first.round, second.round);
        assert!(matches!(
            store.reserve(draft("first"), 3),
            Err(ReviewRequestError::Superseded)
        ));
        assert!(store.require_current(&first).is_err());
        assert!(
            store
                .with_current(&first, || panic!("stale request must not merge"))
                .is_err()
        );
        assert_eq!(store.with_current(&second, || "merged").unwrap(), "merged");
        assert_eq!(store.current(&second.candidate).unwrap(), Some(second));
    }

    #[test]
    fn concurrent_first_requests_initialize_one_store_and_reuse_one_intent() {
        let temp = tempfile::tempdir().unwrap();
        for attempt in 0..16 {
            let store =
                ReviewRequestStore::new(temp.path().join(format!("fresh-{attempt}.sqlite3")));
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let intents = std::thread::scope(|threads| {
                let tasks: Vec<_> = (0..8)
                    .map(|_| {
                        let store = store.clone();
                        let barrier = barrier.clone();
                        threads.spawn(move || {
                            barrier.wait();
                            store.reserve(draft("same-request"), 3)
                        })
                    })
                    .collect();
                tasks
                    .into_iter()
                    .map(|task| task.join().unwrap().unwrap())
                    .collect::<Vec<_>>()
            });
            assert!(intents.iter().all(|intent| intent == &intents[0]));
            store.require_current(&intents[0]).unwrap();
        }
    }

    #[test]
    fn concurrent_distinct_requests_have_one_winner_and_same_key_reuses_it() {
        let temp = tempfile::tempdir().unwrap();
        let store = ReviewRequestStore::new(temp.path().join("requests.sqlite3"));
        store.open_for_write().unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|threads| {
            let tasks: Vec<_> = ["a", "b"]
                .into_iter()
                .map(|key| {
                    let barrier = barrier.clone();
                    let store = store.clone();
                    threads.spawn(move || {
                        barrier.wait();
                        (key, store.reserve(draft(key), 3))
                    })
                })
                .collect();
            tasks
                .into_iter()
                .map(|task| task.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        let (key, winner) = results
            .into_iter()
            .find_map(|(key, result)| result.ok().map(|intent| (key, intent)))
            .unwrap();
        std::thread::scope(|threads| {
            let tasks: Vec<_> = (0..4)
                .map(|_| {
                    let store = store.clone();
                    threads.spawn(move || store.reserve(draft(key), 3).unwrap())
                })
                .collect();
            for task in tasks {
                assert_eq!(task.join().unwrap(), winner);
            }
        });
    }

    #[test]
    fn damaged_current_pointer_never_becomes_a_new_request() {
        let temp = tempfile::tempdir().unwrap();
        let store = ReviewRequestStore::new(temp.path().join("requests.sqlite3"));
        let first = store.reserve(draft("first"), 3).unwrap();
        let connection = Connection::open(&store.path).unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys=OFF; DELETE FROM review_requests;")
            .unwrap();
        assert!(matches!(
            store.current(&first.candidate),
            Err(ReviewRequestError::Corrupt)
        ));
        assert!(matches!(
            store.reserve(draft("new"), 3),
            Err(ReviewRequestError::Corrupt)
        ));
        assert!(
            store
                .with_current(&first, || panic!(
                    "corrupt current request cannot authorize action"
                ))
                .is_err()
        );
    }

    #[test]
    fn current_request_lock_covers_the_action_and_releases_on_error() {
        let temp = tempfile::tempdir().unwrap();
        let store = ReviewRequestStore::new(temp.path().join("requests.sqlite3"));
        let first = store.reserve(draft("first"), 3).unwrap();
        let result: Result<(), &str> = store.with_current(&first, || {
            let mut competitor = Connection::open(&store.path).unwrap();
            competitor.busy_timeout(Duration::ZERO).unwrap();
            assert!(matches!(competitor.transaction_with_behavior(TransactionBehavior::Immediate),
                Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == rusqlite::ErrorCode::DatabaseBusy));
            Err("external action failed")
        }).unwrap();
        assert!(result.is_err());
        let mut second = draft("second");
        second.supersedes = Some(first.round);
        store.reserve(second, 3).unwrap();
    }

    #[test]
    fn independent_tracker_and_repository_scopes_do_not_share_current_request() {
        let temp = tempfile::tempdir().unwrap();
        let store = ReviewRequestStore::new(temp.path().join("requests.sqlite3"));
        let first = store.reserve(draft("first"), 3).unwrap();
        for change in ["project", "issue", "repository"] {
            let mut other = draft("first");
            match change {
                "project" => other.candidate.tracker_context = "project-b".into(),
                "issue" => other.candidate.issue_id.0 = "10002".into(),
                _ => other.candidate.repo.name.0 = "other-app".into(),
            }
            let other = store.reserve(other, 3).unwrap();
            assert_ne!(other.round, first.round);
            store.require_current(&other).unwrap();
        }
        store.require_current(&first).unwrap();
    }
}
