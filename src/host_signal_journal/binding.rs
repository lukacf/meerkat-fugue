use super::*;
use crate::config::ServiceConfig;
use crate::kapellmeister::KapellmeisterShadowSignal;

/// Explicit repository and tracker routing supplied by the host. It contains
/// no credentials and cannot redirect an existing journal to a different mob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSignalBinding {
    pub path: PathBuf,
    pub scope: HostSignalScope,
}

impl HostSignalBinding {
    pub fn from_config(config: &ServiceConfig) -> Result<Self> {
        let repository_path = std::path::absolute(&config.workflow_dir).map_err(failure)?;
        let runtime_path = std::path::absolute(crate::meta_runtime::meta_runtime_state_path(
            &config.meerkat,
        ))
        .map_err(failure)?;
        let tracker = match config.tracker.kind.as_str() {
            "jira" => {
                let scope = crate::jira::routing_scope(&config.tracker)?;
                TrackerScope::Jira {
                    endpoint: scope.base.to_string(),
                    project: scope.project,
                    query: scope.query,
                }
            }
            "linear" => {
                let endpoint = reqwest::Url::parse(&config.tracker.endpoint)
                    .map_err(|_| failure("invalid tracker endpoint"))?;
                TrackerScope::Linear {
                    endpoint: endpoint.to_string(),
                    project: config
                        .tracker
                        .project_slug
                        .clone()
                        .ok_or(FugueError::MissingTrackerProjectSlug)?,
                }
            }
            "fugue-ims" => TrackerScope::Internal {
                database: std::path::absolute(crate::fugue_ims::fugue_ims_path(
                    &config.tracker.endpoint,
                )?)
                .map_err(failure)?,
                repository: config
                    .tracker
                    .project_slug
                    .clone()
                    .ok_or(FugueError::MissingTrackerProjectSlug)?,
            },
            kind => return Err(FugueError::UnsupportedTrackerKind(kind.to_string())),
        };
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
