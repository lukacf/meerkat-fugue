//! Atomic tracker observations and pending host notifications.
//!
//! This journal owns delivery intent only. Native Meerkat admission remains the
//! authority for delivery receipts and agent lifecycle.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};

use crate::error::{FugueError, Result};
use crate::linear_lane_state::{
    LinearCommentObservation, LinearCommentSnapshot, LinearCommentState, LinearLaneObservation,
    LinearLaneState,
};

const APPLICATION_ID: i64 = 0x46554753;
const SCHEMA_VERSION: i64 = 1;

mod binding;
pub use binding::{HOST_SIGNAL_BINDING_ENV, HostSignalBinding};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSignalScope {
    pub repository_path: PathBuf,
    pub runtime_path: PathBuf,
    pub mob_id: String,
    pub member: String,
    pub tracker: TrackerScope,
}

impl HostSignalScope {
    fn validate(&self) -> Result<()> {
        if !self.repository_path.is_absolute()
            || !self.runtime_path.is_absolute()
            || self.mob_id.trim().is_empty()
            || self.member.trim().is_empty()
        {
            return Err(failure(
                "scope requires absolute repository/runtime paths and a native destination",
            ));
        }
        match &self.tracker {
            TrackerScope::Jira {
                endpoint, project, ..
            }
            | TrackerScope::Linear { endpoint, project } => {
                let url = reqwest::Url::parse(endpoint)
                    .map_err(|_| failure("invalid tracker endpoint"))?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || project.trim().is_empty()
                {
                    return Err(failure(
                        "tracker scope requires a project and an endpoint without credentials, query or fragment",
                    ));
                }
            }
            TrackerScope::Internal {
                database,
                repository,
            } => {
                if !database.is_absolute() || repository.trim().is_empty() {
                    return Err(failure(
                        "internal tracker scope requires an absolute database path and repository",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Routing coordinates only. Authentication material never enters the journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrackerScope {
    Jira {
        endpoint: String,
        project: String,
        query: Option<String>,
    },
    Linear {
        endpoint: String,
        project: String,
    },
    Internal {
        database: PathBuf,
        repository: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryKey(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalHandlingMode {
    Steer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalEnvelope {
    pub content: String,
    pub mode: SignalHandlingMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSignal {
    pub key: DeliveryKey,
    pub envelope: SignalEnvelope,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DeferredSignal {
    pub key: DeliveryKey,
    pub receipt: meerkat_contracts::WireContinuationReceipt,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DrainProgress {
    pub acknowledged: u32,
    /// Preserve native Pending, Stranded and Refused outcomes for the caller.
    /// None of these outcomes consumes the saved notification.
    pub deferred: Option<DeferredSignal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationRevision(i64);

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observations {
    lanes: LinearLaneState,
    comments: LinearCommentState,
}

pub struct HostSignalJournal {
    connection: Connection,
}

pub struct ObservationUpdate<'connection> {
    transaction: Transaction<'connection>,
    observations: Observations,
    revision: ObservationRevision,
}

fn failure(error: impl std::fmt::Display) -> FugueError {
    FugueError::Workspace(format!("host notification journal: {error}"))
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(failure)
}

fn connect_existing(path: &Path) -> Result<Connection> {
    // CREATE is deliberately absent: a missing or truncated existing store is
    // not evidence of an installation with no pending work.
    let metadata = fs::symlink_metadata(path).map_err(failure)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(failure("existing journal is not a nonempty regular file"));
    }
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE).map_err(failure)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(failure)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(failure)?;
    Ok(connection)
}

fn create_if_missing(path: &Path, scope: &HostSignalScope) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failure(error)),
    }
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| failure("journal path requires an explicit parent directory"))?;
    let mut missing_directories = Vec::new();
    for directory in parent.ancestors() {
        match directory.try_exists() {
            Ok(true) => break,
            Ok(false) => missing_directories.push(directory),
            Err(error) => return Err(failure(error)),
        }
    }
    fs::create_dir_all(parent).map_err(failure)?;
    for directory in missing_directories {
        if let Some(parent) = directory.parent() {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(failure)?;
        }
    }
    // Initialize a private temporary database, then publish without replacing a
    // concurrent creator's committed store. A crash cannot publish an empty DB.
    let temporary = tempfile::NamedTempFile::new_in(parent).map_err(failure)?;
    let mut connection =
        Connection::open_with_flags(temporary.path(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(failure)?;
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .map_err(failure)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(failure)?;
    let transaction = connection.transaction().map_err(failure)?;
    transaction
        .execute_batch(
            "CREATE TABLE metadata (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            scope TEXT NOT NULL,
            observations TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0)
        );
        CREATE TABLE deliveries (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            source_event TEXT NOT NULL UNIQUE,
            delivery_key TEXT NOT NULL UNIQUE,
            envelope TEXT NOT NULL,
            accepted_delivery_id TEXT UNIQUE
        );",
        )
        .map_err(failure)?;
    transaction
        .pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(failure)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(failure)?;
    transaction
        .execute(
            "INSERT INTO metadata (singleton, scope, observations, revision) VALUES (1, ?1, ?2, 0)",
            params![encode(scope)?, encode(&Observations::default())?],
        )
        .map_err(failure)?;
    transaction.commit().map_err(failure)?;
    connection.close().map_err(|(_, error)| failure(error))?;
    temporary.as_file().sync_all().map_err(failure)?;
    match temporary.persist_noclobber(path) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(failure(error.error)),
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(failure)
}

impl HostSignalJournal {
    pub fn open(path: &Path, scope: &HostSignalScope) -> Result<Self> {
        scope.validate()?;
        create_if_missing(path, scope)?;
        let connection = connect_existing(path)?;
        let application: i64 = connection
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .map_err(failure)?;
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(failure)?;
        if application != APPLICATION_ID || version != SCHEMA_VERSION {
            return Err(failure("unrecognized journal format or schema version"));
        }
        let saved: String = connection
            .query_row(
                "SELECT scope FROM metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(failure)?;
        let saved: HostSignalScope =
            serde_json::from_str(&saved).map_err(|_| failure("saved routing scope is invalid"))?;
        if saved != *scope {
            return Err(failure(
                "journal belongs to another repository, runtime or tracker scope",
            ));
        }
        Ok(Self { connection })
    }

    pub fn revision(&self) -> Result<ObservationRevision> {
        self.connection
            .query_row(
                "SELECT revision FROM metadata WHERE singleton = 1",
                [],
                |row| row.get(0).map(ObservationRevision),
            )
            .map_err(failure)
    }

    /// Capture the revision before network reads, then compare it under the
    /// write lock. A late tracker response must not overwrite a newer internal
    /// lane move or another observer's snapshot. No transaction crosses await.
    pub fn begin_at(&mut self, revision: ObservationRevision) -> Result<ObservationUpdate<'_>> {
        let update = self.begin()?;
        if update.revision != revision {
            return Err(FugueError::HostObservationChanged);
        }
        Ok(update)
    }

    pub fn begin(&mut self) -> Result<ObservationUpdate<'_>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let (saved, revision): (String, i64) = transaction
            .query_row(
                "SELECT observations, revision FROM metadata WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(failure)?;
        let observations = serde_json::from_str(&saved)
            .map_err(|_| failure("saved tracker observations are invalid"))?;
        Ok(ObservationUpdate {
            transaction,
            observations,
            revision: ObservationRevision(revision),
        })
    }

    pub fn pending(&self, limit: u32) -> Result<Vec<PendingSignal>> {
        let mut query = self.connection.prepare(
            "SELECT delivery_key, envelope FROM deliveries WHERE accepted_delivery_id IS NULL ORDER BY sequence LIMIT ?1",
        ).map_err(failure)?;
        let rows = query
            .query_map([limit], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(failure)?;
        rows.map(|row| {
            let (key, envelope) = row.map_err(failure)?;
            let envelope: SignalEnvelope = serde_json::from_str(&envelope)
                .map_err(|_| failure("saved notification envelope is invalid"))?;
            if key.is_empty() || envelope.content.is_empty() {
                return Err(failure("saved notification key or content is empty"));
            }
            Ok(PendingSignal {
                key: DeliveryKey(key),
                envelope,
            })
        })
        .collect()
    }

    /// Call only after native keyed admission returns its canonical receipt.
    pub fn acknowledge(&mut self, key: &DeliveryKey, native_delivery_id: &str) -> Result<()> {
        if native_delivery_id.trim().is_empty() {
            return Err(failure("native delivery receipt is empty"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let accepted: Option<Option<String>> = transaction
            .query_row(
                "SELECT accepted_delivery_id FROM deliveries WHERE delivery_key = ?1",
                [&key.0],
                |row| row.get(0),
            )
            .optional()
            .map_err(failure)?;
        match accepted {
            None => return Err(failure("delivery key is not recorded in this journal")),
            Some(Some(saved)) if saved != native_delivery_id => {
                return Err(failure("conflicting native delivery receipt"));
            }
            Some(Some(_)) => return Ok(()),
            Some(None) => {}
        }
        transaction
            .execute(
                "UPDATE deliveries SET accepted_delivery_id = ?1 WHERE delivery_key = ?2",
                params![native_delivery_id, key.0],
            )
            .map_err(failure)?;
        transaction.commit().map_err(failure)
    }
}

impl ObservationUpdate<'_> {
    /// Persist the exact notification together with its observation. A new
    /// occurrence gets a new identity even when content and timestamps repeat.
    /// Periodic unchanged observations retain their existing coalescing key.
    pub fn retain_signals(
        &mut self,
        config: &crate::config::ServiceConfig,
        signals: &mut [crate::kapellmeister::KapellmeisterShadowSignal],
        new_occurrence: bool,
    ) -> Result<()> {
        for signal in signals {
            if new_occurrence {
                signal.id = crate::kapellmeister::ShadowSignalId(format!(
                    "observation-{}",
                    uuid::Uuid::new_v4()
                ));
            }
            self.enqueue_once(&signal.id.0, || {
                SignalEnvelope::for_signals(config, std::slice::from_ref(signal))
            })?;
        }
        Ok(())
    }

    pub fn observe_lane(&mut self, identifier: &str, lane: &str) -> LinearLaneObservation {
        self.observations.lanes.observe_lane(identifier, lane)
    }

    pub fn observe_comments(
        &mut self,
        identifier: &str,
        comments: Vec<LinearCommentSnapshot>,
    ) -> LinearCommentObservation {
        self.observations
            .comments
            .observe_issue_comments(identifier, comments)
    }

    /// Construct the immutable envelope only for a genuinely new source event.
    /// Repeated observations retain their original key, content and mode even
    /// after acknowledgment or a process restart.
    pub fn enqueue_once(
        &mut self,
        source_event: &str,
        envelope: impl FnOnce() -> Result<SignalEnvelope>,
    ) -> Result<DeliveryKey> {
        if source_event.is_empty() {
            return Err(failure("source event identity is empty"));
        }
        let existing: Option<String> = self
            .transaction
            .query_row(
                "SELECT delivery_key FROM deliveries WHERE source_event = ?1",
                [source_event],
                |row| row.get(0),
            )
            .optional()
            .map_err(failure)?;
        if let Some(key) = existing {
            return Ok(DeliveryKey(key));
        }
        let envelope = envelope()?;
        if envelope.content.is_empty() {
            return Err(failure("notification content is empty"));
        }
        let key = DeliveryKey(format!("fugue-signal-{}", uuid::Uuid::new_v4()));
        self.transaction
            .execute(
                "INSERT INTO deliveries (source_event, delivery_key, envelope) VALUES (?1, ?2, ?3)",
                params![source_event, key.0, encode(&envelope)?],
            )
            .map_err(failure)?;
        Ok(key)
    }

    pub fn commit(self) -> Result<()> {
        let revision = self
            .revision
            .0
            .checked_add(1)
            .ok_or_else(|| failure("observation revision exhausted"))?;
        self.transaction
            .execute(
                "UPDATE metadata SET observations = ?1, revision = ?2 WHERE singleton = 1",
                params![encode(&self.observations)?, revision],
            )
            .map_err(failure)?;
        self.transaction.commit().map_err(failure)
    }
}

#[cfg(test)]
mod tests;
