use super::*;
use crate::config::ServiceConfig;
use crate::kapellmeister::KapellmeisterShadowSignal;

pub const HOST_SIGNAL_BINDING_ENV: &str = "FUGUE_HOST_SIGNAL_BINDING";

/// Explicit repository and tracker routing supplied by the host. It contains
/// no credentials and cannot redirect an existing journal to a different mob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSignalBinding {
    pub path: PathBuf,
    pub scope: HostSignalScope,
}

impl HostSignalBinding {
    pub fn from_environment() -> Result<Option<Self>> {
        match std::env::var(HOST_SIGNAL_BINDING_ENV) {
            Ok(value) => Self::decode(&value).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(failure("invalid host signal binding")),
        }
    }

    pub fn decode(value: &str) -> Result<Self> {
        let binding: Self =
            serde_json::from_str(value).map_err(|_| failure("invalid host signal binding"))?;
        binding.scope.validate()?;
        if !binding.path.is_absolute() {
            return Err(failure("journal binding requires an absolute path"));
        }
        Ok(binding)
    }

    pub fn encode(&self) -> Result<String> {
        self.scope.validate()?;
        encode(self)
    }

    pub fn validate_tracker(&self, config: &crate::config::TrackerConfig) -> Result<()> {
        if self.scope.tracker != TrackerScope::from_config(config)? {
            return Err(failure(
                "tool tracker does not match the host signal binding",
            ));
        }
        Ok(())
    }

    pub fn record_internal_lane(&self, identifier: &str, lane: &str) -> Result<()> {
        let mut journal = self.open()?;
        let mut update = journal.begin()?;
        update.observe_lane(identifier, lane);
        update.commit()
    }

    pub fn from_config(config: &ServiceConfig) -> Result<Self> {
        let repository_path = std::path::absolute(&config.workflow_dir).map_err(failure)?;
        let runtime_path = std::path::absolute(crate::meta_runtime::meta_runtime_state_path(
            &config.meerkat,
        ))
        .map_err(failure)?;
        let tracker = TrackerScope::from_config(&config.tracker)?;
        let scope = HostSignalScope {
            repository_path: repository_path.clone(),
            runtime_path,
            mob_id: config.meerkat.mob_id().into_owned(),
            member: crate::meta_runtime::KAPELLMEISTER.to_owned(),
            tracker,
        };
        scope.validate()?;
        Ok(Self {
            path: repository_path.join(".fugue/host-signals.sqlite3"),
            scope,
        })
    }

    pub fn open(&self) -> Result<HostSignalJournal> {
        if !self.path.is_absolute() {
            return Err(failure("journal binding requires an absolute path"));
        }
        HostSignalJournal::open(&self.path, &self.scope)
    }

    /// Attempt one bounded snapshot of pending work in commit order. The sender
    /// must use this exact scope, key, content and mode with native keyed
    /// admission, and return its complete native receipt. No tracker read is
    /// required, so callers can retry during tracker outages and startup.
    ///
    /// No database connection or transaction crosses the sender's await. A
    /// failed, cancelled or unacknowledged send stays pending with its original
    /// key. Native admission, not this journal, deduplicates uncertain retries.
    /// Pending, stranded and refused receipts stop this snapshot and return
    /// their native status intact; only an Admitted receipt is acknowledged.
    /// Terminal non-admission also commits a deterministic replacement key for
    /// the next attempt. An ambiguous result never rotates the key.
    pub async fn drain_pending<F, Fut>(
        &self,
        limit: std::num::NonZeroU32,
        mut admit: F,
    ) -> Result<DrainProgress>
    where
        F: FnMut(HostSignalScope, PendingSignal) -> Fut,
        Fut: std::future::Future<Output = Result<meerkat_contracts::WireContinuationReceipt>>,
    {
        let pending = self.open()?.pending(limit.get())?;
        let mut progress = DrainProgress::default();
        for signal in pending {
            let key = signal.key.clone();
            let receipt = admit(self.scope.clone(), signal).await?;
            if receipt.delivery_id.trim().is_empty() {
                return Err(failure("native delivery receipt is empty"));
            }
            match &receipt.status {
                meerkat_contracts::WireContinuationStatus::Admitted {
                    session_id,
                    input_id,
                } => {
                    if session_id.trim().is_empty() || input_id.trim().is_empty() {
                        return Err(failure("native admission requires a session and input"));
                    }
                    self.open()?.acknowledge(&key, &receipt)?;
                    progress.acknowledged += 1;
                }
                meerkat_contracts::WireContinuationStatus::Pending => {
                    progress.deferred = Some(DeferredSignal {
                        key,
                        receipt,
                        replacement_key: None,
                    });
                    break;
                }
                meerkat_contracts::WireContinuationStatus::Stranded
                | meerkat_contracts::WireContinuationStatus::Refused { .. } => {
                    let replacement_key = Some(self.open()?.replace_unadmitted(&key, &receipt)?);
                    progress.deferred = Some(DeferredSignal {
                        key,
                        receipt,
                        replacement_key,
                    });
                    break;
                }
            }
        }
        Ok(progress)
    }
}

impl SignalEnvelope {
    pub fn for_signals(
        config: &ServiceConfig,
        signals: &[KapellmeisterShadowSignal],
    ) -> Result<Self> {
        let content = crate::kapellmeister::repo_kapellmeister_signal_message_with_lanes(
            signals,
            &config.linear.lanes.success,
            config.linear.lanes.review_ready.as_deref(),
            config
                .independent_review
                .enabled
                .then_some(config.independent_review.lane.as_str()),
        )
        .map_err(failure)?;
        Ok(Self {
            content,
            mode: SignalHandlingMode::Steer,
        })
    }
}

impl TrackerScope {
    pub fn from_config(config: &crate::config::TrackerConfig) -> Result<Self> {
        Ok(match config.kind.as_str() {
            "jira" => {
                let scope = crate::jira::routing_scope(config)?;
                TrackerScope::Jira {
                    endpoint: scope.base.to_string(),
                    project: scope.project,
                    query: scope.query,
                }
            }
            "linear" => {
                let endpoint = reqwest::Url::parse(&config.endpoint)
                    .map_err(|_| failure("invalid tracker endpoint"))?;
                TrackerScope::Linear {
                    endpoint: endpoint.to_string(),
                    project: config
                        .project_slug
                        .clone()
                        .ok_or(FugueError::MissingTrackerProjectSlug)?,
                }
            }
            "fugue-ims" => TrackerScope::Internal {
                database: std::path::absolute(crate::fugue_ims::fugue_ims_path(&config.endpoint)?)
                    .map_err(failure)?,
                repository: config
                    .project_slug
                    .clone()
                    .ok_or(FugueError::MissingTrackerProjectSlug)?,
            },
            kind => return Err(FugueError::UnsupportedTrackerKind(kind.to_string())),
        })
    }
}
