//! Persistent native repository teams and their representative federation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use meerkat_core::service::{CreateSessionRequest, SessionBuildOptions, SessionError};
use meerkat_core::{AgentToolDispatcher, ContentInput};
use meerkat_meta::identity_first::{
    AgentAddressability, AgentIdentity, DispatchInput, DurableAgentSpec, IdentityBootstrapMode,
    IdentityLifecycleState, ManagedPeerEdge, MutableRosterProvider, TopologyContext, TopologyError,
    TopologyProvider, TurnOutcome, TurnOutput, TurnTracking,
};
use meerkat_meta::{
    SameProcessTopologyCoordinator, SessionHook, TopologyAction, TopologyBilateralApplyRequest,
    TopologyControlMode, TopologyControlPolicy, TopologyEdge, TopologyEndpoint, TopologyMutation,
    TopologyOperationStatus, UnifiedRuntime,
};
use meerkat_mob::{MobDefinition, MobRuntimeMode, Profile, ProfileBinding, ToolConfig};
use meerkat_tools::builtin::shell::ShellConfig;
use meerkat_tools::{BuiltinToolConfig, CompositeDispatcher, MemoryTaskStore, ToolPolicyLayer};
use serde::{Deserialize, Serialize};

use super::config::{EnsembleConfig, EnsembleStateGuard, PeerConfig, RepoId};
use super::discovery::{RepoBrief, discover_repository};
use super::repository_tools::RepositoryTools;
use super::team::{Capability, MemberId, REPRESENTATIVE, TeamManifest};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnsembleRole {
    Representative,
    Planner,
    Coder,
    Reviewer,
}

#[cfg(test)]
impl EnsembleRole {
    pub const ALL: [Self; 4] = [
        Self::Representative,
        Self::Planner,
        Self::Coder,
        Self::Reviewer,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Representative => "representative",
            Self::Planner => "planner",
            Self::Coder => "coder",
            Self::Reviewer => "reviewer",
        }
    }

    fn identity(self, repo: &RepoId) -> Result<AgentIdentity> {
        Ok(AgentIdentity::parse(&format!("{repo}:{}", self.as_str()))?)
    }

    fn charter(self) -> &'static str {
        match self {
            Self::Representative => {
                "You are the repository representative. Coordinate your persistent planner, coder and reviewer through native peer messages. Own the user's objective and return concise evidence-backed outcomes. Reach other repositories ONLY through their representatives. Ask the peer directory for exact addresses. Delegate implementation to the coder and seek independent reviewer evidence before claiming completion. Relay questions with enough context and reply to the sender; avoid acknowledgement loops. You have read-only repository access."
            }
            Self::Planner => {
                "You are the repository planner. Study architecture and repository instructions, form concrete acceptance criteria and small implementation plans, and track dependencies in WorkGraph. Discuss tradeoffs with the coder and reviewer using peer messages. Share an actionable plan and evidence with your representative. You have read-only repository access; delegate execution to the coder."
            }
            Self::Coder => {
                "You are the repository coder. Implement agreed objectives in this repository, follow its instructions, run meaningful checks, and communicate exact changes and test results to the reviewer and representative. Inspect git status before editing and preserve unrelated work. You have repository-rooted shell and patch tools. Do not commit, push, publish, merge, or deploy unless explicitly requested by the user. Seek review before claiming completion."
            }
            Self::Reviewer => {
                "You are the independent repository reviewer. Inspect changes and evidence against the objective and acceptance criteria. Report actionable defects with file locations, or explicitly state what you verified and remaining limitations. Ask the coder to run checks when execution is needed; your repository access is read-only. Send your conclusion to the representative and coder. Never approve based only on another agent's claim."
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct EnsembleStatus {
    pub repositories: Vec<RepositoryStatus>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Serialize)]
pub struct RepositoryStatus {
    pub repo: RepoId,
    pub mob_id: String,
    pub brief: RepoBrief,
    pub members: Vec<MemberStatus>,
    pub preparation: PreparationState,
    pub discovery: super::team::DiscoveryProvenance,
}

#[derive(Debug, Serialize)]
pub struct MemberStatus {
    pub member_id: String,
    pub study_state: StudyState,
    pub label: String,
    pub focus: String,
    pub capability: Capability,
    /// Native profile capability with a mounted durable store and completed
    /// study admission. This does not claim that the member is currently busy.
    pub memory_enabled: bool,
    pub ownership_paths: Vec<PathBuf>,
    pub identity: String,
    pub runtime_alias: Option<String>,
    pub session_id: Option<String>,
    pub state: IdentityLifecycleState,
}

#[derive(Debug, Serialize)]
pub struct EnsembleAccepted {
    pub repo: RepoId,
    pub identity: String,
    pub session_id: String,
    pub interaction_id: String,
}

struct RepositoryRuntime {
    runtime: Arc<UnifiedRuntime>,
    brief: RepoBrief,
    team: TeamManifest,
    studies: Arc<tokio::sync::Mutex<StudyJournal>>,
    study_path: PathBuf,
    grants: BTreeMap<String, MemberGrant>,
    event_drain: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    handoff_notifications: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    handoff_failure: std::sync::OnceLock<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationState {
    Preparing,
    Ready,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StudyState {
    NotRequired,
    Pending,
    Completed,
}

#[derive(Clone)]
struct MemberGrant {
    capability: Capability,
    studied: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StudyRecord {
    interaction_id: String,
    completed: bool,
    #[serde(default)]
    brief: Option<String>,
    #[serde(default)]
    inspected_files: BTreeMap<PathBuf, String>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StudyJournal {
    members: BTreeMap<MemberId, StudyRecord>,
}

fn member_identity(repo: &RepoId, member: &str) -> Result<AgentIdentity> {
    Ok(AgentIdentity::parse(&format!("{repo}:{member}"))?)
}

fn save_studies(path: &Path, journal: &StudyJournal) -> Result<()> {
    use std::io::Write;
    let parent = path.parent().context("study journal has no parent")?;
    let temporary = parent.join(format!(
        ".studies-{}.json",
        meerkat_core::types::SessionId::new()
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    serde_json::to_writer_pretty(&mut file, journal)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temporary, path)?;
    Ok(())
}

pub struct EnsembleRuntime {
    config: EnsembleConfig,
    repositories: BTreeMap<RepoId, RepositoryRuntime>,
    lifecycle: tokio::sync::RwLock<bool>,
    peer_coordinators: Vec<SameProcessTopologyCoordinator>,
    _state_guard: EnsembleStateGuard,
}

impl EnsembleRuntime {
    pub async fn build(config: EnsembleConfig) -> Result<Self> {
        Box::pin(Self::build_inner(config, None)).await
    }

    #[cfg(test)]
    pub(crate) async fn build_with_client(
        config: EnsembleConfig,
        client: Arc<dyn meerkat::LlmClient>,
    ) -> Result<Self> {
        Box::pin(Self::build_inner(config, Some(client))).await
    }

    async fn build_inner(
        mut config: EnsembleConfig,
        client: Option<Arc<dyn meerkat::LlmClient>>,
    ) -> Result<Self> {
        config.resolve_and_validate(&std::env::current_dir()?)?;
        let state_guard = config.claim_state()?;
        let mut this = Self {
            config,
            repositories: BTreeMap::new(),
            lifecycle: tokio::sync::RwLock::new(false),
            peer_coordinators: Vec::new(),
            _state_guard: state_guard,
        };
        let result = Box::pin(this.initialize(client)).await;
        if let Err(error) = result {
            return match this.shutdown().await {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("partial build cleanup failed: {cleanup:#}")))
                }
            };
        }
        Ok(this)
    }

    async fn initialize(&mut self, client: Option<Arc<dyn meerkat::LlmClient>>) -> Result<()> {
        let connection_config = if client.is_some() {
            meerkat::Config::default()
        } else {
            super::team_discovery::isolated_connection_config(
                meerkat::Config::global_config_path().as_deref(),
            )
            .await?
        };
        for (id, repository) in &self.config.repositories {
            let brief = discover_repository(&repository.path)?;
            let state = self.config.state_root.join(id.as_str());
            std::fs::create_dir_all(&state)?;
            let team_path = state.join("team.json");
            let team = match TeamManifest::load(&team_path, &repository.path)? {
                Some(team) => team,
                None => {
                    eprintln!("Discovering specialists for {id} ({})", self.config.model);
                    let team = super::team_discovery::discover_team(
                        &repository.path,
                        &state,
                        &self.config.state_root,
                        &brief,
                        &self.config.model,
                        self.config.auth_binding.clone(),
                    )
                    .await?;
                    self.config.validate_team_capacity(id, &team)?;
                    team.save(&team_path, &repository.path)?;
                    team
                }
            };
            self.config.validate_team_capacity(id, &team)?;
            let study_path = state.join("studies.json");
            let mut studies: StudyJournal = match std::fs::read(&study_path) {
                Ok(bytes) => serde_json::from_slice(&bytes).context("read study journal")?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    StudyJournal::default()
                }
                Err(error) => return Err(error.into()),
            };
            let mut grants = BTreeMap::new();
            grants.insert(
                REPRESENTATIVE.to_owned(),
                MemberGrant {
                    capability: Capability::ReadOnly,
                    studied: Arc::new(AtomicBool::new(true)),
                },
            );
            for (member, spec) in &team.specialists {
                let record = studies
                    .members
                    .entry(member.clone())
                    .or_insert_with(|| StudyRecord {
                        interaction_id: meerkat_core::types::SessionId::new().to_string(),
                        completed: false,
                        brief: None,
                        inspected_files: BTreeMap::new(),
                    });
                grants.insert(
                    member.to_string(),
                    MemberGrant {
                        capability: spec.capability,
                        studied: Arc::new(AtomicBool::new(record.completed)),
                    },
                );
            }
            save_studies(&study_path, &studies)?;
            let studies = Arc::new(tokio::sync::Mutex::new(studies));
            let (storage, provenance) =
                meerkat_meta::mob_composition_manifest::persistent_mob_storage(
                    state.join("mob.sqlite3"),
                )?;
            let desired_definition = definition(id, &self.config.model, &grants);
            let handoff_runtime: super::handoff::RuntimeSlot = Arc::new(std::sync::OnceLock::new());
            if let Some(previous) = storage.created_definition_snapshot().await? {
                let canonical = previous.definition();
                let legacy_profiles = ["coder", "planner", "representative", "reviewer"];
                let is_legacy = canonical.profiles.len() == legacy_profiles.len()
                    && legacy_profiles
                        .iter()
                        .all(|name| canonical.profiles.contains_key(*name));
                let mut migrated = canonical.clone();
                if is_legacy {
                    migrated.profiles = desired_definition.profiles.clone();
                    // Retiring fixed workers also retires only their exact
                    // host-generated flows; customized flow drift stays visible.
                    for (flow_id, flow) in super::handoff::flows(legacy_profiles.into_iter()) {
                        if migrated.flows.get(&flow_id) == Some(&flow) {
                            migrated.flows.remove(&flow_id);
                        }
                    }
                } else {
                    // Earlier Ensemble definitions omitted native memory. Only
                    // this host-owned capability changes for an existing team;
                    // model, roster and other policy drift still fail closed.
                    for binding in migrated.profiles.values_mut() {
                        if let ProfileBinding::Inline(profile) = binding {
                            profile.tools.memory = true;
                        }
                    }
                }
                // Add only our missing native handoff flows. Existing flow drift
                // remains visible and fails definition validation.
                for (flow_id, flow) in &desired_definition.flows {
                    migrated
                        .flows
                        .entry(flow_id.clone())
                        .or_insert_with(|| flow.clone());
                }
                if canonical != &desired_definition && migrated == desired_definition {
                    // The native epoch CAS preserves history and makes the new profile
                    // composition authoritative before identity reconciliation retires
                    // the former generic workers. Unrelated drift still fails closed.
                    storage
                        .update_definition(previous.epoch(), desired_definition.clone())
                        .await
                        .context("upgrade repository team definition")?;
                }
            }
            let mut roster = Vec::new();
            for name in grants.keys() {
                let (label, charter) = if name == REPRESENTATIVE {
                    (
                        "Repository representative".to_owned(),
                        representative_instructions(&team),
                    )
                } else {
                    let member = MemberId::try_from(name.clone()).map_err(anyhow::Error::msg)?;
                    let spec = team
                        .specialists
                        .get(&member)
                        .context("missing specialist spec")?;
                    (
                        spec.label.clone(),
                        format!(
                            "{}\n\nDomain: {}\nFocus: {}\nOwnership paths: {:?}\nCapability: {:?}. You are a long-lived domain specialist, not a transient task worker. Follow repository instructions; preserve unrelated work. Use peers to seek complementary expertise and independent review. Do not commit, push, publish, merge or deploy unless explicitly requested by the user.",
                            spec.charter,
                            spec.label,
                            spec.focus,
                            spec.ownership_paths,
                            spec.capability,
                        ),
                    )
                };
                roster.push(DurableAgentSpec {
                    identity: member_identity(id, name)?, profile: name.clone().into(),
                    addressability: AgentAddressability::Addressable, display_name: Some(meerkat_meta::identity_first::DisplayName::parse(&label)?),
                    labels: BTreeMap::new(), context: None,
                    additional_instructions: vec![format!(
                        "{charter}\n\nRepository: {id}\nRoot: {}\n{}\n\n{}\n\nUse native peers and send_message for collaboration. WorkGraph is the durable shared task ledger. Reading repository content does not authorize unrelated operations.",
                        repository.path.display(), repository.description.as_deref().unwrap_or(""), brief.prompt_context()
                    ), collaboration_instructions()],
                    initial_message: None, runtime_mode_override: Some(MobRuntimeMode::TurnDriven),
                    backend: None, binding: None, placement: None,
                });
            }
            let mut edges = Vec::new();
            for (index, a) in roster.iter().enumerate() {
                for b in &roster[index + 1..] {
                    edges.push(ManagedPeerEdge::new(
                        a.identity.clone(),
                        b.identity.clone(),
                    )?);
                }
            }
            let contacts = self
                .config
                .repositories
                .keys()
                .filter(|other| *other != id)
                .map(|other| {
                    // RepoId validation limits names to TOML-safe ASCII.
                    format!("\"{}\" = \"inproc\"\n", mob_id(other))
                })
                .collect::<String>();
            let contacts = meerkat_meta::contact_directory::ContactDirectory::from_toml(&format!(
                "[mobs]\n{contacts}"
            ))?;
            let mut builder = UnifiedRuntime::builder()
                .meerkat_config(connection_config.clone())
                .definition(desired_definition)
                .mob_storage(storage, provenance)
                .persistent_state(&state)
                .persistent_agent_memory_stack(
                    meerkat_meta::identity_first::agent_memory::AgentMemoryConfig {
                        realm: mob_id(id),
                        ..Default::default()
                    },
                    // Native recorder, recall, taint firewall and console
                    // projection; no unsolicited background inference.
                    meerkat_meta::memory_wiring::MemoryEnginesConfig::default(),
                )
                .continuity_from_state_dir(&state)
                .await?
                .require_incremental_session_persistence(true)
                .roster_provider(Arc::new(MutableRosterProvider::new(roster)))
                .topology_provider(Arc::new(TeamTopology(edges)))
                .identity_bootstrap_mode(IdentityBootstrapMode::EagerMaterialize)
                .session_hook(Arc::new(RepositorySessionHook {
                    root: repository.path.clone(),
                    state_root: self.config.state_root.clone(),
                    auth_binding: self.config.auth_binding.clone(),
                    grants: grants.clone(),
                    handoff_runtime: handoff_runtime.clone(),
                    studies: studies.clone(),
                    study_path: study_path.clone(),
                }))
                .contact_directory(contacts)
                .comms(true)
                .timeout(Duration::from_secs(60))
                .max_sessions(self.config.max_sessions)
                .topology_control(TopologyControlPolicy {
                    mode: TopologyControlMode::Editable,
                    allow_cross_authority: true,
                    ..TopologyControlPolicy::default()
                })?;
            if let Some(client) = &client {
                builder = builder.default_llm_client(Arc::clone(client));
            }
            let runtime = Arc::new(
                Box::pin(builder.build())
                    .await
                    .with_context(|| format!("build repository team {id}"))?,
            );
            handoff_runtime
                .set(Arc::downgrade(&runtime))
                .map_err(|_| anyhow::anyhow!("handoff runtime already initialized"))?;
            let event_drain = runtime.clone().spawn_event_drain_task();
            self.repositories.insert(
                id.clone(),
                RepositoryRuntime {
                    runtime,
                    brief,
                    team,
                    studies,
                    study_path,
                    grants,
                    event_drain: tokio::sync::Mutex::new(Some(event_drain)),
                    handoff_notifications: tokio::sync::Mutex::new(None),
                    handoff_failure: std::sync::OnceLock::new(),
                },
            );
            let entry = self
                .repositories
                .get(id)
                .context("repository runtime insertion failed")?;
            restore_member_transports(id, entry).await?;
        }
        // Persisted native rosters retain historical assignments. Retire any
        // former live worker absent from the authoritative saved team before
        // wiring or admitting study work; history remains available.
        for (repo, entry) in &self.repositories {
            for member in entry.runtime.mob_handle().list_members().await {
                if !entry.grants.contains_key(member.role.as_str()) {
                    // The new identity registry intentionally omits this old
                    // assignment. Its surviving native mob row remains owned
                    // by the mob lifecycle, which can retire it by exact ID.
                    entry
                        .runtime
                        .mob_handle()
                        .retire(member.agent_identity)
                        .await
                        .with_context(|| {
                            format!("retire former repository assignment in {repo}")
                        })?;
                }
            }
        }
        // Keep collaboration guidance current in every preserved conversation.
        // Native context append is idempotent per session and exact content;
        // updating instructions does not recreate identities or repeat studies.
        for (repo, entry) in &self.repositories {
            use sha2::{Digest, Sha256};
            let authority = entry
                .runtime
                .identity_runtime()
                .context("identity authority missing")?;
            let service = entry
                .runtime
                .mob_runtime()
                .session_service()
                .context("native session service missing")?;
            for name in entry.grants.keys() {
                let session = authority
                    .status(&member_identity(repo, name)?)
                    .await?
                    .session_id
                    .context("member session missing")?;
                let instructions = if name == REPRESENTATIVE {
                    format!(
                        "{}\n\n{}",
                        representative_instructions(&entry.team),
                        collaboration_instructions()
                    )
                } else {
                    collaboration_instructions()
                };
                let digest = format!("{:x}", Sha256::digest(instructions.as_bytes()));
                service
                    .append_system_context(
                        &session,
                        meerkat_core::service::AppendSystemContextRequest {
                            content: meerkat_core::CoreRenderable::text(instructions),
                            source: Some("fugue-ensemble-team".into()),
                            idempotency_key: Some(format!("ensemble-team-{digest}")),
                        },
                    )
                    .await?;
            }
        }
        // An absent authority cannot participate in a bilateral transaction.
        // Suspend only its local transport half; keep the pair's durable intent
        // for coordinator reconciliation if that repository returns later.
        let present: BTreeSet<_> = self.config.repositories.keys().map(mob_id).collect();
        for entry in self.repositories.values() {
            let handle = entry.runtime.mob_handle();
            let members = handle.list_all_members().await;
            let local: BTreeSet<_> = members
                .iter()
                .map(|member| &member.agent_identity)
                .collect();
            for member in &members {
                for peer in &member.wired_to {
                    if local.contains(peer) {
                        continue;
                    }
                    let address: meerkat_core::MemberCommsName = peer
                        .as_str()
                        .parse()
                        .context("parse persisted foreign member address")?;
                    if !present.contains(address.mob_id()) {
                        let name = meerkat_core::comms::PeerName::new(peer.to_string())
                            .map_err(anyhow::Error::msg)?;
                        handle
                            .unwire(
                                member.agent_identity.clone(),
                                meerkat_mob::PeerTarget::ExternalName(name),
                            )
                            .await
                            .context("suspend peer route to an absent repository")?;
                    }
                }
            }
        }
        // A bilateral coordinator owns both durable intents and installs the
        // namespace-aware routes consumed by agents' actual comms tools.
        // Reconcile every present pair, including removed configured links.
        let repos: Vec<_> = self.config.repositories.keys().cloned().collect();
        for (index, from) in repos.iter().enumerate() {
            for to in &repos[index + 1..] {
                let a = self.repository(from).context("missing source runtime")?;
                let b = self.repository(to).context("missing target runtime")?;
                a.register_peer_runtime(b).await;
                b.register_peer_runtime(a).await;
                let journal = self.config.state_root.join("peers").join(from.as_str());
                std::fs::create_dir_all(&journal)?;
                let coordinator = SameProcessTopologyCoordinator::open(
                    journal.join(format!("{}.json", to.as_str())),
                )?;
                let edge = TopologyEdge::new(
                    TopologyEndpoint {
                        authority: Some(mob_id(from)),
                        identity: member_identity(from, REPRESENTATIVE)?.to_string(),
                    },
                    TopologyEndpoint {
                        authority: Some(mob_id(to)),
                        identity: member_identity(to, REPRESENTATIVE)?.to_string(),
                    },
                )?;
                let desired = self.config.peers.iter().any(|peer| {
                    (&peer.from == from && &peer.to == to) || (&peer.from == to && &peer.to == from)
                });
                let snapshot = coordinator.query(a, b, None).await?;
                let current = snapshot.edges.iter().find(|row| row.edge == edge);
                let change = if desired {
                    match current {
                        // Rebind incarnation-sensitive ingress after restart even
                        // when durable intent and peer-directory rows still match.
                        Some(_) => Some(TopologyAction::Reconnect),
                        _ => Some(TopologyAction::Connect),
                    }
                } else if current.is_some_and(|row| row.desired || row.actual) {
                    Some(TopologyAction::Disconnect)
                } else {
                    None
                };
                if let Some(action) = change {
                    let receipt = coordinator
                        .apply(
                            a,
                            b,
                            TopologyBilateralApplyRequest {
                                expected_revisions: snapshot.authority_revisions,
                                idempotency_key: meerkat_core::types::SessionId::new().to_string(),
                                operations: vec![TopologyMutation {
                                    action,
                                    edge: edge.clone(),
                                }],
                                reason: Some(
                                    "Reconcile configured Ensemble representatives".into(),
                                ),
                                risk_tier: None,
                            },
                            None,
                            "fugue-ensemble",
                        )
                        .await
                        .with_context(|| format!("peer representatives {from} and {to}"))?;
                    if !matches!(
                        receipt.status,
                        TopologyOperationStatus::Applied | TopologyOperationStatus::Noop
                    ) {
                        bail!(
                            "representative peering {from}/{to} did not settle: {:?}",
                            receipt.status
                        );
                    }
                }
                let verified = coordinator.query(a, b, None).await?;
                let actual = verified
                    .edges
                    .iter()
                    .find(|row| row.edge == edge)
                    .is_some_and(|row| row.actual && row.desired);
                if actual != desired {
                    bail!("representative route {from}/{to} does not match its configured state");
                }
                self.peer_coordinators.push(coordinator);
            }
        }
        for (repo, entry) in &self.repositories {
            futures::future::join_all(
                entry
                    .team
                    .specialists
                    .iter()
                    .map(|(member, spec)| self.study_member(repo, entry, member, spec)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        }
        // Do not advertise a partially hydrated team as ready.
        for (repo, entry) in &self.repositories {
            if entry.runtime.workgraph_service().is_none() {
                bail!("repository {repo} has no durable WorkGraph service");
            }
            if entry.runtime.memory_panel_store().is_none() {
                bail!("repository {repo} has no durable agent memory store");
            }
        }
        for repository in self.status().await?.repositories {
            for member in repository.members {
                if member.state != IdentityLifecycleState::Active || member.session_id.is_none() {
                    bail!(
                        "repository {} member {} failed to activate: {:?}",
                        repository.repo,
                        member.identity,
                        member.state
                    );
                }
            }
        }
        for (repo, entry) in &self.repositories {
            let task = super::handoff::start_notifications(
                &entry.runtime,
                member_identity(repo, REPRESENTATIVE)?,
            )
            .await?;
            *entry.handoff_notifications.lock().await = Some(task);
        }
        Ok(())
    }

    async fn study_member(
        &self,
        repo: &RepoId,
        entry: &RepositoryRuntime,
        member: &MemberId,
        spec: &super::team::SpecialistSpec,
    ) -> Result<()> {
        let mut record = entry
            .studies
            .lock()
            .await
            .members
            .get(member)
            .cloned()
            .context("missing study journal entry")?;
        if record.completed {
            return Ok(());
        }
        eprintln!("Studying {repo}: {} ({member})", spec.label);
        let authority = entry
            .runtime
            .identity_runtime()
            .context("identity authority missing")?;
        let identity = member_identity(repo, member.as_str())?;
        let root = &self
            .config
            .repositories
            .get(repo)
            .context("study repository missing")?
            .path;
        let mut ownership_scopes = Vec::new();
        for scope in &spec.ownership_paths {
            match tokio::fs::canonicalize(root.join(scope)).await {
                Ok(path) => ownership_scopes.push(
                    path.strip_prefix(root)
                        .context("study ownership scope escaped repository")?
                        .to_path_buf(),
                ),
                // Saved ownership may outlive a deleted path. Other extant
                // scopes can still ground this study; an empty set cannot.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("resolve study ownership scope"),
            }
        }
        if study_has_terminal_failure(entry, &identity, &record.interaction_id).await? {
            record = rotate_study_attempt(entry, member).await?;
        }
        let prompt = format!(
            "Initial domain study. Inspect your ownership paths {:?} and relevant repository guidance using read tools. Establish a durable Domain Brief: architecture, responsibilities, interfaces, invariants, tests, risks, and concrete source references. This study is read-only: do not edit, run shell commands, delegate, send peer messages, or emit acknowledgements. Return the substantive Domain Brief (at least 300 characters, with concrete source references) as your final response. Domain: {}. Focus: {}.",
            spec.ownership_paths, spec.label, spec.focus,
        );
        let admitted = authority
            .dispatch_with_turn_ticket(
                &identity,
                None,
                &DispatchInput::system(prompt)
                    .with_correlation(record.interaction_id.clone())
                    .with_idempotency(format!("ensemble-study-{}", record.interaction_id)),
            )
            .await?;
        let ticket = match admitted.turn {
            TurnTracking::Tracked(ticket) => ticket,
            other => bail!("initial study {identity} has no exact completion ticket: {other:?}"),
        };
        match authority
            .wait_for_turn(&identity, ticket, Duration::from_secs(600))
            .await
        {
            TurnOutcome::Completed { output } => {
                let brief = if let Some(text) = output.text().filter(|text| !text.trim().is_empty())
                {
                    text.to_owned()
                } else if matches!(output, TurnOutput::NoOwnResult) {
                    recover_committed_study(entry, &identity, &record.interaction_id).await?
                        .with_context(|| format!("initial study {identity} was deduplicated but its committed Domain Brief is unavailable"))?
                } else {
                    rotate_study_attempt(entry, member).await?;
                    bail!("initial study {identity} completed without a substantive Domain Brief");
                };
                let mut studies = entry.studies.lock().await;
                let saved = studies
                    .members
                    .get_mut(member)
                    .context("study entry disappeared")?;
                if brief.trim().chars().count() < 300 {
                    drop(studies);
                    rotate_study_attempt(entry, member).await?;
                    bail!(
                        "initial study {identity} did not return a substantive Domain Brief (at least 300 characters)"
                    );
                }
                if !saved
                    .inspected_files
                    .keys()
                    .any(|path| ownership_scopes.iter().any(|scope| path.starts_with(scope)))
                {
                    drop(studies);
                    rotate_study_attempt(entry, member).await?;
                    bail!(
                        "initial study {identity} did not successfully read a file in its ownership scope"
                    );
                }
                saved.completed = true;
                saved.brief = Some(brief);
                save_studies(&entry.study_path, &studies)?;
                entry
                    .grants
                    .get(member.as_str())
                    .context("study grant disappeared")?
                    .studied
                    .store(true, Ordering::Release);
                Ok(())
            }
            outcome => {
                if study_has_terminal_failure(entry, &identity, &record.interaction_id).await? {
                    rotate_study_attempt(entry, member).await?;
                }
                bail!("initial study {identity} did not complete: {outcome:?}")
            }
        }
    }

    pub fn config(&self) -> &EnsembleConfig {
        &self.config
    }

    pub fn repository(&self, repo: &RepoId) -> Option<&Arc<UnifiedRuntime>> {
        self.repositories.get(repo).map(|entry| &entry.runtime)
    }

    async fn ensure_notification_health(&self) -> Result<()> {
        for (repo, entry) in &self.repositories {
            let mut watcher = entry.handoff_notifications.lock().await;
            if watcher.as_ref().is_some_and(|task| task.is_finished()) {
                if let Some(task) = watcher.take() {
                    let detail = match task.await {
                        Ok(Err(error)) => format!("{error:#}"),
                        Err(error) => error.to_string(),
                        Ok(Ok(())) => "notification subscription ended".to_owned(),
                    };
                    entry.handoff_failure.get_or_init(|| detail);
                }
            }
            if let Some(detail) = entry.handoff_failure.get() {
                bail!(
                    "repository {repo} delegation notifications failed: {detail}; restart to recover durable outcomes before admitting more work"
                );
            }
        }
        Ok(())
    }

    pub async fn status(&self) -> Result<EnsembleStatus> {
        self.ensure_notification_health().await?;
        let mut repositories = Vec::new();
        for (repo, entry) in &self.repositories {
            let authority = entry
                .runtime
                .identity_runtime()
                .context("identity authority missing")?;
            let mut members = Vec::new();
            for (name, grant) in &entry.grants {
                let identity = member_identity(repo, name)?;
                let spec = entry
                    .team
                    .specialists
                    .iter()
                    .find(|(id, _)| id.as_str() == name)
                    .map(|(_, spec)| spec);
                let status = authority.status(&identity).await?;
                members.push(MemberStatus {
                    member_id: name.clone(),
                    study_state: if name == REPRESENTATIVE {
                        StudyState::NotRequired
                    } else if grant.studied.load(Ordering::Acquire) {
                        StudyState::Completed
                    } else {
                        StudyState::Pending
                    },
                    label: spec
                        .map(|s| s.label.clone())
                        .unwrap_or_else(|| "Repository representative".into()),
                    focus: spec.map(|s| s.focus.clone()).unwrap_or_else(|| {
                        "Coordinate specialists and repository federation".into()
                    }),
                    capability: grant.capability,
                    memory_enabled: grant.studied.load(Ordering::Acquire)
                        && entry.runtime.memory_panel_store().is_some()
                        && entry
                            .runtime
                            .mob_handle()
                            .definition()
                            .profiles
                            .get(name.as_str())
                            .and_then(ProfileBinding::as_inline)
                            .is_some_and(|profile| profile.tools.memory),
                    ownership_paths: spec.map(|s| s.ownership_paths.clone()).unwrap_or_default(),
                    identity: identity.to_string(),
                    runtime_alias: status.agent_runtime_id.map(|id| id.to_string()),
                    session_id: status.session_id.map(|id| id.to_string()),
                    state: status.state,
                });
            }
            repositories.push(RepositoryStatus {
                repo: repo.clone(),
                mob_id: mob_id(repo),
                brief: entry.brief.clone(),
                members,
                discovery: entry.team.discovery.clone(),
                preparation: if entry
                    .grants
                    .values()
                    .all(|grant| grant.studied.load(Ordering::Acquire))
                {
                    PreparationState::Ready
                } else {
                    PreparationState::Preparing
                },
            });
        }
        Ok(EnsembleStatus {
            repositories,
            peers: self.config.peers.clone(),
        })
    }

    pub async fn ask(&self, repo: &RepoId, text: &str) -> Result<EnsembleAccepted> {
        self.ensure_notification_health().await?;
        let stopping = self.lifecycle.read().await;
        if *stopping {
            bail!("Ensemble is shutting down");
        }
        if text.trim().is_empty() || text.len() > 64 * 1024 {
            bail!("message must contain 1–65536 bytes");
        }
        let runtime = self.repository(repo).context("unknown repository")?;
        let authority = runtime
            .identity_runtime()
            .context("identity authority missing")?;
        let identity = member_identity(repo, REPRESENTATIVE)?;
        let interaction_id = meerkat_core::types::SessionId::new().to_string();
        authority
            .send_admission_tracked(
                &identity,
                None,
                &ContentInput::from(text),
                meerkat_core::types::HandlingMode::Queue,
                Some(&interaction_id),
            )
            .await?;
        let status = authority.status(&identity).await?;
        Ok(EnsembleAccepted {
            repo: repo.clone(),
            identity: identity.to_string(),
            interaction_id,
            session_id: status
                .session_id
                .context("admitted member has no session")?
                .to_string(),
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut stopping = self.lifecycle.write().await;
        if *stopping {
            return Ok(());
        }
        *stopping = true;
        let mut errors = Vec::new();
        for (repo, entry) in &self.repositories {
            if let Some(task) = entry.handoff_notifications.lock().await.take() {
                task.abort();
                match task.await {
                    Ok(Err(error)) => {
                        errors.push(format!("{repo} handoff notifications: {error:#}"))
                    }
                    Err(error) if !error.is_cancelled() => {
                        errors.push(format!("{repo} handoff notifications: {error}"))
                    }
                    _ => {}
                }
            }
            let report = entry.runtime.shutdown().await;
            if let Some(task) = entry.event_drain.lock().await.take() {
                task.abort();
                if let Err(error) = task.await
                    && !error.is_cancelled()
                {
                    errors.push(format!("{repo} event drain: {error}"));
                }
            }
            if !report.cleanup_completed() && !report.cleanup_retained_until_process_exit() {
                errors.push(format!("{repo}: {report:?}"));
            }
        }
        if !errors.is_empty() {
            bail!("Ensemble shutdown failed: {errors:?}");
        }
        Ok(())
    }
}

fn collaboration_instructions() -> String {
    let delivery = "Native collaboration delivery rules:\n\
For an [ENSEMBLE DELEGATED WORK] assignment, return your substantive result and supporting \
evidence as final text. Its native flow persists your result and automatically delivers the \
terminal outcome to the representative; do not send a duplicate completion reply. The \
representative must use ensemble_delegate for bounded specialist assignments and retain its \
run_id and correlation in WorkGraph. Peer messages are for FYI and cross-repository contact.\n\n\
When other work originates in a peer message or request, return the result and supporting evidence \
to that requesting peer through the matching native response tool before ending with final text. \
Your final assistant text alone is not delivered to the requesting peer. For an ordinary peer \
message in the current turn, use reply_to_peer with handling_mode=queue; if multiple deliveries \
are present, use the matching reply_to delivery ID. For a typed send_request, use send_response \
with the original request ID, requesting peer, terminal completed or failed status, and the \
result required by that request's contract. If the original ordinary request is from an earlier \
turn and no longer has a reply capability, resolve that requesting peer with peers and send \
the correlated result using send_message. Never send another peer's result to whichever peer \
happened to trigger the latest turn.\n\n\
For those ordinary peer requests, explicitly require the recipient to return its findings, executed commands, \
verification results, or blocker evidence through the matching native reply path. Retain the \
requesting peer and correlation while work is pending. Send substantive completion or failure \
evidence; do not create acknowledgement loops or reply to a terminal result merely to acknowledge \
it. A delivery receipt is not task completion.\n\n\
Exception: during an explicit initial read-only domain study, only inspect repository files and \
return the Domain Brief as final text. That study is host-initiated, requires no peer reply, and \
must not send messages or delegate.";
    format!(
        "{delivery}\n\n{}\n\n{}\n\n{}",
        super::task_contract::TASK_CONTRACT,
        super::task_contract::MEMORY_CONTRACT,
        super::task_contract::RELEASE_CONTRACT
    )
}

fn representative_instructions(team: &TeamManifest) -> String {
    use std::fmt::Write;

    let mut instructions = String::from(
        "You are the repository representative. Coordinate this repository's durable domain \
specialists through ensemble_delegate for assignments that require a result. Native flows return \
final results and failures to you automatically; retain run_id and correlation in WorkGraph, and \
use ensemble_delegation_status after a restart or failure. Use peer messages for FYI only. \
Delegate to the relevant specialist, seek independent \
review from another specialist, and return evidence-backed outcomes. Reach other repositories \
only through their representatives. Avoid acknowledgement loops.\n\n\
Current authoritative repository specialist roster:\n",
    );
    for (id, spec) in &team.specialists {
        let capability = match spec.capability {
            Capability::Code => "repository code and verification tools",
            Capability::ReadOnly => "read-only repository tools",
        };
        // Writing to a String cannot fail.
        let _ = writeln!(
            instructions,
            "{id}: {} [{capability}] — {}",
            spec.label, spec.focus
        );
    }
    instructions.push_str(
        "\nThese assignments supersede former generic planner/coder/reviewer assignments in \
conversation history. Discover active recipients with peers; never route work to retired \
assignments. Choose specialists whose capabilities permit the requested work.\n\n\
Preserve executable commands and code exactly inside fenced blocks. Keep explanatory punctuation \
outside those blocks. Correct unambiguous transcription errors introduced during delegation \
within the original authorization; send explicit replacement instructions to the same owner.\n\n\
Native message acceptance confirms delivery admission, not task completion. Record delegated work \
in WorkGraph as pending, including its owner and expected evidence. After delegation, finish this \
turn and resume when a durable delegation outcome or native peer reply arrives. An absent result within the current turn is not \
a failure and does not justify duplicate execution or reassignment. Assess each reply and report \
completion only after receiving the requested evidence; otherwise report pending progress.",
    );
    instructions
}

/// A killed process can leave durable membership without a live executor
/// attachment. Repair through the native reload lifecycle before peer wiring;
/// its healthy path is a no-op and its repair preserves the session identity.
async fn restore_member_transports(repo: &RepoId, entry: &RepositoryRuntime) -> Result<()> {
    let authority = entry
        .runtime
        .identity_runtime()
        .context("identity authority missing")?;
    for name in entry.grants.keys() {
        let identity = member_identity(repo, name)?;
        let before = authority.status(&identity).await?;
        let alias = before
            .agent_runtime_id
            .as_ref()
            .with_context(|| {
                let bootstrap = authority.identity_bootstrap_status();
                format!(
                    "member runtime alias missing for {identity}: state={:?}, session={:?}, bootstrap={:?}, continuity_unrecoverable={:?}, session_repair_required={:?}, customizer_tools_pending={:?}",
                    before.state,
                    before.session_id,
                    bootstrap.identities.get(&identity),
                    before.continuity_unrecoverable,
                    before.session_repair_required,
                    before.customizer_tools_pending,
                )
            })?
            .to_string();
        match entry.runtime.local_member_peer_info(&alias).await {
            Ok(_) => continue,
            Err(meerkat_meta::unified_runtime::cross_mob::CrossMobError::NoCommsInfo {
                ..
            }) => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect native member transport {repo}/{name}"));
            }
        }
        eprintln!("Restoring native session transport for {repo}/{name}");
        authority
            .reload_member_alias_tracked(&identity, Some(&alias))
            .await
            .with_context(|| format!("reload native member transport {repo}/{name}"))?;
        let after = authority.status(&identity).await?;
        if before.session_id != after.session_id {
            bail!("native transport recovery changed the durable session for {repo}/{name}");
        }
        let restored_alias = after
            .agent_runtime_id
            .context("reloaded member alias missing")?
            .to_string();
        entry
            .runtime
            .local_member_peer_info(&restored_alias)
            .await
            .with_context(|| {
                format!("native member transport unavailable after reload: {repo}/{name}")
            })?;
    }
    Ok(())
}

fn mob_id(repo: &RepoId) -> String {
    format!("ensemble-{repo}")
}

struct TeamTopology(Vec<ManagedPeerEdge>);

#[async_trait]
impl TopologyProvider for TeamTopology {
    async fn compute_edges(
        &self,
        _: &[AgentIdentity],
        _: &TopologyContext,
    ) -> std::result::Result<Vec<ManagedPeerEdge>, TopologyError> {
        Ok(self.0.clone())
    }
}

fn definition(repo: &RepoId, model: &str, grants: &BTreeMap<String, MemberGrant>) -> MobDefinition {
    let mut definition = MobDefinition {
        id: mob_id(repo).into(),
        runtime: Default::default(),
        orchestrator: None,
        profiles: BTreeMap::new(),
        models: BTreeMap::new(),
        image_generation_provider: None,
        wiring: Default::default(),
        skills: BTreeMap::new(),
        backend: Default::default(),
        flows: super::handoff::flows(grants.keys().map(String::as_str)),
        topology: None,
        supervisor: None,
        limits: None,
        spawn_policy: None,
        event_router: None,
        source_identity: None,
    };
    for name in grants.keys() {
        definition.profiles.insert(
            name.clone().into(),
            ProfileBinding::Inline(Box::new(Profile {
                model: model.to_owned(),
                model_fallback: None,
                provider: None,
                self_hosted_server_id: None,
                image_generation_provider: None,
                auto_compact_threshold: None,
                resume_overrides: Vec::new(),
                skills: Vec::new(),
                tools: ToolConfig {
                    builtins: false,
                    shell: false,
                    comms: true,
                    mob: false,
                    workgraph: true,
                    memory: true,
                    ..ToolConfig::default()
                },
                peer_description: format!("{repo} {name}"),
                external_addressable: true,
                backend: None,
                runtime_mode: MobRuntimeMode::TurnDriven,
                max_inline_peer_notifications: None,
                output_schema: None,
                provider_params: None,
            })),
        );
    }
    definition
}

async fn rotate_study_attempt(entry: &RepositoryRuntime, member: &MemberId) -> Result<StudyRecord> {
    let mut journal = entry.studies.lock().await;
    let record = journal
        .members
        .get_mut(member)
        .context("study attempt missing")?;
    record.interaction_id = meerkat_core::types::SessionId::new().to_string();
    record.completed = false;
    record.brief = None;
    record.inspected_files.clear();
    let next = record.clone();
    save_studies(&entry.study_path, &journal)?;
    Ok(next)
}

/// Only durable exact-terminal facts permit a new attempt. Timeouts, missing
/// receipts and broken observation channels retain the existing identity.
async fn recover_study_work(
    entry: &RepositoryRuntime,
    identity: &AgentIdentity,
    interaction: &str,
) -> Result<meerkat_mob::DurableBoundedWorkRecovery> {
    let authority = entry
        .runtime
        .identity_runtime()
        .context("identity authority missing")?;
    let alias = authority
        .status(identity)
        .await?
        .agent_runtime_id
        .context("study runtime alias missing")?;
    let recovery = entry
        .runtime
        .mob_handle()
        .recover_bounded_work_for_identity_with_delivery_identity(
            &alias.to_string().into(),
            &meerkat_mob::MobDeliveryIdentity::new(
                format!("ensemble-study-{interaction}"),
                interaction,
            )?,
            &meerkat_mob::BoundedResultSpec::new("initial-study", 64 * 1024)?,
        )
        .await?;
    Ok(recovery)
}

async fn study_has_terminal_failure(
    entry: &RepositoryRuntime,
    identity: &AgentIdentity,
    interaction: &str,
) -> Result<bool> {
    use meerkat_mob::{BoundedTurnFailure as Failure, DurableBoundedWorkState as State};
    let recovery = recover_study_work(entry, identity, interaction).await?;
    Ok(matches!(
        recovery.work(),
        State::TerminalWithoutRun { .. }
            | State::Terminal {
                result: Err(Failure::Cancelled { .. }
                    | Failure::Abandoned { .. }
                    | Failure::AbandonedWithError { .. }
                    | Failure::CompletedWithFinalizationFailure { .. }
                    | Failure::RuntimeTerminated { .. }
                    | Failure::CompletedWithoutResult { .. }
                    | Failure::ExtractionFailed { .. }),
                ..
            }
    ))
}

async fn recover_committed_study(
    entry: &RepositoryRuntime,
    identity: &AgentIdentity,
    interaction_id: &str,
) -> Result<Option<String>> {
    let recovery = recover_study_work(entry, identity, interaction_id).await?;
    if let meerkat_mob::DurableBoundedWorkState::Terminal {
        result: Ok(result), ..
    } = recovery.work()
    {
        // Preserve an empty successful result too: it is a known-invalid
        // brief whose attempt must rotate, not an unknown historical result.
        return Ok(Some(result.result().text().to_owned()));
    }
    let authority = entry
        .runtime
        .identity_runtime()
        .context("identity authority missing")?;
    let session = authority
        .status(identity)
        .await?
        .session_id
        .context("study session missing")?;
    let mut offset = 0;
    let mut found = None;
    loop {
        let page = entry
            .runtime
            .mob_runtime()
            .read_session_history(&session.to_string(), offset, Some(256))
            .await?;
        for message in &page.messages {
            if let meerkat_core::Message::BlockAssistant(assistant) = message
                && assistant
                    .identity
                    .interaction_id
                    .as_ref()
                    .is_some_and(|id| id.to_string() == interaction_id)
                && matches!(
                    assistant.stop_reason,
                    Some(
                        meerkat_core::StopReason::EndTurn | meerkat_core::StopReason::StopSequence
                    )
                )
            {
                let mut text = String::new();
                for block in assistant.text_blocks() {
                    text.push_str(block);
                }
                if !text.trim().is_empty() {
                    found = Some(text);
                }
            }
        }
        if !page.has_more {
            return Ok(found);
        }
        if page.messages.is_empty() {
            bail!("native study history pagination made no progress");
        }
        offset += page.messages.len();
    }
}

/// Records only content actually returned by the bounded reader. Persisting
/// evidence before returning the tool result also closes the study commit gap.
struct StudyRepositoryTools {
    reads: RepositoryTools,
    root: PathBuf,
    member: Option<MemberId>,
    studies: Arc<tokio::sync::Mutex<StudyJournal>>,
    study_path: PathBuf,
    studied: Option<Arc<AtomicBool>>,
}

#[derive(Deserialize)]
struct StudyReadEvidence {
    path: PathBuf,
    content: String,
}

#[async_trait]
impl AgentToolDispatcher for StudyRepositoryTools {
    fn tools(&self) -> Arc<[Arc<meerkat_core::ToolDef>]> {
        self.reads.tools()
    }

    fn tool_catalog_capabilities(&self) -> meerkat_core::ToolCatalogCapabilities {
        self.reads.tool_catalog_capabilities()
    }

    async fn dispatch(
        &self,
        call: meerkat_core::ToolCallView<'_>,
    ) -> std::result::Result<meerkat_core::ops::ToolDispatchOutcome, meerkat_core::ToolError> {
        use sha2::{Digest, Sha256};
        let observe = call.name == "repo_read_file"
            && self
                .studied
                .as_ref()
                .is_some_and(|flag| !flag.load(Ordering::Acquire));
        let outcome = self.reads.dispatch(call).await?;
        if observe && !outcome.result.is_error {
            let record = async {
                let evidence: StudyReadEvidence =
                    serde_json::from_str(&outcome.result.text_content())?;
                if evidence.content.is_empty() {
                    return Ok::<(), anyhow::Error>(());
                }
                let canonical = tokio::fs::canonicalize(self.root.join(evidence.path)).await?;
                let relative = canonical.strip_prefix(&self.root)?.to_path_buf();
                let member = self
                    .member
                    .as_ref()
                    .context("study reader has no specialist identity")?;
                let mut journal = self.studies.lock().await;
                let study = journal
                    .members
                    .get_mut(member)
                    .context("missing study evidence entry")?;
                study.inspected_files.entry(relative).or_insert_with(|| {
                    format!("{:x}", Sha256::digest(evidence.content.as_bytes()))
                });
                save_studies(&self.study_path, &journal)?;
                Ok(())
            }
            .await;
            record.map_err(|error| meerkat_core::ToolError::ExecutionFailed {
                message: format!("persist study read evidence: {error:#}"),
            })?;
        }
        Ok(outcome)
    }
}

struct StudyAdmission {
    studied: Arc<AtomicBool>,
    previous: Option<Arc<dyn meerkat_core::ToolDispatchAdmission>>,
}

#[async_trait]
impl meerkat_core::tool_execution_policy::ToolDispatchAdmission for StudyAdmission {
    async fn await_dispatch_admission(
        &self,
        call: meerkat_core::ToolCallView<'_>,
        context: Option<&meerkat_core::ToolDispatchContext>,
        effect: meerkat_core::LiveBridgeEffectKind,
    ) -> std::result::Result<(), meerkat_core::ToolError> {
        if self.studied.load(Ordering::Acquire)
            || matches!(call.name, "repo_read_file" | "repo_list_files")
        {
            if let Some(previous) = &self.previous {
                previous
                    .await_dispatch_admission(call, context, effect)
                    .await?;
            }
            Ok(())
        } else {
            Err(meerkat_core::ToolError::access_denied(call.name))
        }
    }
}

struct RepositorySessionHook {
    handoff_runtime: super::handoff::RuntimeSlot,
    studies: Arc<tokio::sync::Mutex<StudyJournal>>,
    study_path: PathBuf,
    grants: BTreeMap<String, MemberGrant>,
    root: PathBuf,
    state_root: PathBuf,
    auth_binding: Option<meerkat_core::AuthBindingRef>,
}

#[async_trait]
impl SessionHook for RepositorySessionHook {
    async fn before_create(
        &self,
        request: &mut CreateSessionRequest,
    ) -> std::result::Result<(), SessionError> {
        let build = request
            .build
            .get_or_insert_with(SessionBuildOptions::default);
        let grant = build
            .mob_member_binding
            .as_ref()
            .and_then(|binding| self.grants.get(&binding.role));
        if build.auth_binding.is_none() {
            build.auth_binding = self.auth_binding.clone();
        }
        let files: Arc<dyn AgentToolDispatcher> = Arc::new(StudyRepositoryTools {
            reads: RepositoryTools::new(self.root.clone())
                .with_excluded_state_root(self.state_root.clone()),
            root: self.root.clone(),
            member: build
                .mob_member_binding
                .as_ref()
                .and_then(|binding| MemberId::try_from(binding.role.clone()).ok()),
            studies: self.studies.clone(),
            study_path: self.study_path.clone(),
            studied: grant.map(|grant| grant.studied.clone()),
        });
        let owned: Arc<dyn AgentToolDispatcher> =
            if grant.is_some_and(|grant| grant.capability == Capability::Code) {
                let config = BuiltinToolConfig {
                    policy: ToolPolicyLayer::new()
                        .with_mode(meerkat_tools::ToolMode::DenyAll)
                        .enable_tool("shell")
                        .enable_tool("shell_jobs")
                        .enable_tool("shell_job_status")
                        .enable_tool("shell_job_cancel")
                        .enable_tool("apply_patch"),
                    ..BuiltinToolConfig::default()
                };
                Arc::new(
                    CompositeDispatcher::new(
                        Arc::new(MemoryTaskStore::new()),
                        &config,
                        Some(self.root.clone()),
                        Some(ShellConfig::with_project_root(self.root.clone())),
                        Some(files),
                        None,
                    )
                    .map_err(|error| {
                        SessionError::Unsupported(format!("build repository tools: {error}"))
                    })?,
                )
            } else {
                files
            };
        if let Some(grant) = grant {
            build.tool_dispatch_admission = Some(Arc::new(StudyAdmission {
                studied: grant.studied.clone(),
                previous: build.tool_dispatch_admission.take(),
            }));
        }
        let owned: Arc<dyn AgentToolDispatcher> = if build
            .mob_member_binding
            .as_ref()
            .is_some_and(|binding| binding.role == REPRESENTATIVE)
        {
            meerkat_meta::tool_compose::ComposedExternalTools::over(
                Arc::new(super::handoff::HandoffTools::new(
                    self.handoff_runtime.clone(),
                )),
                Some(owned),
            )
        } else {
            owned
        };
        build.override_builtins = meerkat_core::ToolCategoryOverride::Disable;
        build.override_shell = meerkat_core::ToolCategoryOverride::Disable;
        // Reapply the declared capability even when the durable session was
        // created before Ensemble enabled memory. Keep its identity/history.
        build.override_memory = meerkat_core::ToolCategoryOverride::Enable;
        build.resume_override_mask.override_memory = true;
        build.external_tools = Some(meerkat_meta::tool_compose::ComposedExternalTools::over(
            owned,
            build.external_tools.take(),
        ));
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn write_fixture_teams(config: &EnsembleConfig) {
    use super::team::{DiscoveryProvenance, InspectedFile, SpecialistSpec};
    let mut resolved = config.clone();
    resolved
        .resolve_and_validate(&std::env::current_dir().expect("cwd"))
        .expect("config");
    let _guard = resolved.claim_state().expect("claim fixture state");
    for (repo, repository) in &resolved.repositories {
        let state = resolved.state_root.join(repo.as_str());
        std::fs::create_dir_all(&state).expect("state directory");
        let mut specialists = BTreeMap::new();
        let mut studies = StudyJournal::default();
        for role in [
            EnsembleRole::Planner,
            EnsembleRole::Coder,
            EnsembleRole::Reviewer,
        ] {
            let member = MemberId::try_from(role.as_str().to_owned()).expect("fixture member");
            specialists.insert(
                member.clone(),
                SpecialistSpec {
                    label: format!("{} specialist", role.as_str()),
                    focus: format!("Repository {} evidence", role.as_str()),
                    charter: role.charter().to_owned(),
                    ownership_paths: vec![PathBuf::from("README.md")],
                    capability: if role == EnsembleRole::Coder {
                        Capability::Code
                    } else {
                        Capability::ReadOnly
                    },
                },
            );
            studies.members.insert(
                member,
                StudyRecord {
                    interaction_id: meerkat_core::types::SessionId::new().to_string(),
                    completed: true,
                    brief: Some("Previously completed fixture study.".into()),
                    inspected_files: BTreeMap::from([(PathBuf::from("README.md"), "a".repeat(64))]),
                },
            );
        }
        TeamManifest {
            version: 1,
            specialists,
            discovery: DiscoveryProvenance {
                model: resolved.model.clone(),
                discovered_at: chrono::Utc::now(),
                inspected_files: vec![InspectedFile {
                    path: PathBuf::from("README.md"),
                    sha256: "a".repeat(64),
                }],
            },
        }
        .save(&state.join("team.json"), &repository.path)
        .expect("fixture manifest");
        save_studies(&state.join("studies.json"), &studies).expect("fixture studies");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerkat_core::{Message, Provider, StopReason, TurnUsage, Usage};
    use meerkat_llm_core::{LlmClient, LlmDoneOutcome, LlmError, LlmEvent, LlmRequest, LlmStream};
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct PeerScript {
        calls: AtomicUsize,
        sent: AtomicBool,
        replied: AtomicBool,
        foreign_attempted: AtomicBool,
        foreign_denied: AtomicBool,
        peers: Mutex<BTreeMap<&'static str, String>>,
        phase: &'static str,
    }

    #[async_trait]
    impl LlmClient for PeerScript {
        fn project_replay_messages(
            &self,
            messages: &[Message],
        ) -> std::result::Result<Vec<Message>, LlmError> {
            Ok(messages.to_vec())
        }
        fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let transcript =
                serde_json::to_string(&request.messages).expect("serialize test transcript");
            assert!(
                transcript.contains(
                    "An explicit request to merge or deploy locally grants authorization"
                )
            );
            assert!(
                request
                    .tools
                    .iter()
                    .any(|tool| tool.name == "repo_read_file"),
                "repository reader must reach the real native agent"
            );
            let coder = transcript.contains("You are the repository coder.");
            assert_eq!(
                request.tools.iter().any(|tool| tool.name == "shell"),
                coder,
                "only the coder may execute shell commands"
            );
            assert_eq!(
                request.tools.iter().any(|tool| tool.name == "apply_patch"),
                coder,
                "only the coder may edit files"
            );
            let representative = transcript.contains("You are the repository representative.");
            if coder
                && transcript.contains("Probe foreign boundary.")
                && transcript.contains("peer_not_found_or_not_trusted")
            {
                self.foreign_denied.store(true, Ordering::SeqCst);
            }
            let mut events = Vec::new();
            let target = if coder
                && transcript.contains("Probe foreign boundary.")
                && !self.foreign_attempted.swap(true, Ordering::SeqCst)
            {
                Some(("beta", "Unauthorized direct coder message".to_owned()))
            } else if representative
                && transcript.contains("Repository: alpha")
                && transcript.contains(self.phase)
                && !self.sent.swap(true, Ordering::SeqCst)
            {
                Some(("beta", format!("Federated question {}", self.phase)))
            } else if representative
                && transcript.contains("Repository: beta")
                && transcript.contains(&format!("Federated question {}", self.phase))
                && !self.replied.swap(true, Ordering::SeqCst)
            {
                Some(("alpha", format!("Federated answer {}", self.phase)))
            } else {
                None
            };
            let stop_reason = if let Some((target, body)) = target {
                assert!(
                    request.tools.iter().any(|tool| tool.name == "send_message"),
                    "native send_message must be visible"
                );
                let peer = self
                    .peers
                    .lock()
                    .expect("peers")
                    .get(target)
                    .cloned()
                    .expect("resolved peer");
                events.push(LlmEvent::ToolCallComplete {
                    id: format!("message-{}", self.calls.load(Ordering::SeqCst)),
                    name: "send_message".into(),
                    args: json!({"peer_id": peer, "body": body, "handling_mode": "queue"}),
                    meta: None,
                });
                StopReason::ToolUse
            } else {
                let text = if transcript.contains(&format!("Federated answer {}", self.phase)) {
                    format!("Round trip complete {}", self.phase)
                } else {
                    "Ready.".into()
                };
                events.push(LlmEvent::TextDelta {
                    delta: text,
                    meta: None,
                });
                StopReason::EndTurn
            };
            events.push(LlmEvent::UsageUpdate {
                usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
            });
            events.push(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { stop_reason },
            });
            Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
        }
        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
        async fn health_check(&self) -> std::result::Result<(), LlmError> {
            Ok(())
        }
    }

    fn isolated(name: &str) -> bool {
        if std::env::var("FUGUE_ENSEMBLE_NATIVE_TEST").ok().as_deref() == Some(name) {
            return true;
        }
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .env("FUGUE_ENSEMBLE_NATIVE_TEST", name)
            .env("RUST_MIN_STACK", "16777216")
            .arg(format!("ensemble::runtime::tests::{name}"))
            .args(["--exact", "--nocapture"])
            .output()
            .expect("spawn test process");
        assert!(
            output.status.success(),
            "native test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        false
    }

    fn fixture_config(root: &std::path::Path) -> EnsembleConfig {
        let mut repos = BTreeMap::new();
        for name in ["alpha", "beta"] {
            let path = root.join(name);
            std::fs::create_dir_all(&path).expect("repo dir");
            assert!(
                std::process::Command::new("git")
                    .args(["init", "-q"])
                    .arg(&path)
                    .status()
                    .expect("git init")
                    .success()
            );
            std::fs::write(
                path.join("README.md"),
                format!("# {name}\nTest repository.\n"),
            )
            .expect("readme");
            repos.insert(
                RepoId::try_from(name.to_string()).expect("repo id"),
                super::super::config::RepositoryConfig {
                    path,
                    description: None,
                },
            );
        }
        let config = EnsembleConfig {
            version: 1,
            name: "Test ensemble".into(),
            state_root: root.join("state"),
            model: "gpt-5.5".into(),
            auth_binding: None,
            max_sessions: 16,
            repositories: repos,
            peers: vec![PeerConfig {
                from: RepoId::try_from("alpha".to_string()).expect("alpha"),
                to: RepoId::try_from("beta".to_string()).expect("beta"),
            }],
        };
        write_fixture_teams(&config);
        config
    }

    #[derive(Default)]
    struct MemoryScript {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LlmClient for MemoryScript {
        fn project_replay_messages(
            &self,
            messages: &[Message],
        ) -> std::result::Result<Vec<Message>, LlmError> {
            Ok(messages.to_vec())
        }

        fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            for tool in ["memory", "memory_search"] {
                assert!(
                    request
                        .tools
                        .iter()
                        .any(|definition| definition.name == tool),
                    "native {tool} must reach fresh and restored agents"
                );
            }
            let prompt = request
                .messages
                .iter()
                .rev()
                .find_map(|message| {
                    let Message::User(user) = message else {
                        return None;
                    };
                    user.content.iter().find_map(|block| match block {
                        meerkat_core::ContentBlock::Text { text }
                            if text.starts_with("MEMORY_PROBE_") =>
                        {
                            Some(text.as_str())
                        }
                        _ => None,
                    })
                })
                .expect("explicit memory probe");
            let tool_id = format!("tool-{prompt}");
            let result = request.messages.iter().rev().find_map(|message| {
                let Message::ToolResults { results, .. } = message else {
                    return None;
                };
                results.iter().find(|result| result.tool_use_id == tool_id)
            });
            let mut events = Vec::new();
            let stop_reason = if let Some(result) = result {
                assert!(
                    !result.is_error,
                    "native memory failed: {}",
                    result.text_content()
                );
                let text = result.text_content();
                if prompt.contains("REMEMBER") {
                    assert!(text.contains("Stored memory"), "write must commit: {text}");
                    assert!(
                        !text.contains("QUARANTINED"),
                        "trusted fixture write: {text}"
                    );
                } else if prompt.contains("ABSENT") {
                    assert_eq!(
                        text, "No matching memory records.",
                        "memory must remain identity-scoped"
                    );
                } else {
                    assert!(
                        text.contains("ORCHID-417"),
                        "recall must read the persisted body: {text}"
                    );
                }
                events.push(LlmEvent::TextDelta {
                    delta: "Memory probe verified.".into(),
                    meta: None,
                });
                StopReason::EndTurn
            } else {
                let args = if prompt.contains("REMEMBER") {
                    json!({
                        "action": "remember", "title": "Release evidence token",
                        "body": "Use verification token ORCHID-417 for release evidence.",
                        "description": "Release evidence token for the operator",
                        "kind": "preference", "epistemic": "operator_said"
                    })
                } else {
                    json!({"action": "recall"})
                };
                events.push(LlmEvent::ToolCallComplete {
                    id: tool_id,
                    name: "memory".into(),
                    args,
                    meta: None,
                });
                StopReason::ToolUse
            };
            events.push(LlmEvent::UsageUpdate {
                usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
            });
            events.push(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { stop_reason },
            });
            Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
        }

        fn provider(&self) -> Provider {
            Provider::OpenAI
        }

        async fn health_check(&self) -> std::result::Result<(), LlmError> {
            Ok(())
        }
    }

    async fn memory_probe(ensemble: &EnsembleRuntime, repo: &str, member: &str, prompt: &str) {
        let repo = RepoId::try_from(repo.to_owned()).expect("repo");
        let native = ensemble.repository(&repo).expect("native runtime");
        native
            .identity_runtime()
            .expect("authority")
            .send_awaiting_commit(
                &member_identity(&repo, member).expect("member"),
                &ContentInput::from(prompt),
            )
            .await
            .expect("native memory turn");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_memory_is_durable_identity_scoped_and_upgrades_existing_profiles() {
        if !isolated("native_memory_is_durable_identity_scoped_and_upgrades_existing_profiles") {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        // Exercise the dynamic-profile upgrade, not the older fixed-roster
        // migration. Persist the same real fixture under domain member IDs.
        for (repo, repository) in &config.repositories {
            let state = config.state_root.join(repo.as_str());
            let mut team = TeamManifest::load(&state.join("team.json"), &repository.path)
                .expect("team")
                .expect("saved team");
            team.specialists = team
                .specialists
                .into_iter()
                .map(|(id, spec)| {
                    (
                        MemberId::try_from(format!("domain-{id}")).expect("domain ID"),
                        spec,
                    )
                })
                .collect();
            std::fs::write(
                state.join("team.json"),
                serde_json::to_vec(&team).expect("team JSON"),
            )
            .expect("fixture team");
            let mut journal: StudyJournal = serde_json::from_slice(
                &std::fs::read(state.join("studies.json")).expect("journal"),
            )
            .expect("journal shape");
            journal.members = journal
                .members
                .into_iter()
                .map(|(id, record)| {
                    (
                        MemberId::try_from(format!("domain-{id}")).expect("domain ID"),
                        record,
                    )
                })
                .collect();
            save_studies(&state.join("studies.json"), &journal).expect("fixture journal");
        }
        let client = Arc::new(MemoryScript::default());
        let ensemble = EnsembleRuntime::build_with_client(config.clone(), client.clone())
            .await
            .expect("memory-enabled team");
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            0,
            "memory startup stays idle"
        );
        let before = ensemble.status().await.expect("status");
        assert!(
            before
                .repositories
                .iter()
                .flat_map(|repo| &repo.members)
                .all(|member| member.memory_enabled),
            "status reflects native memory configuration and completed admission"
        );
        memory_probe(&ensemble, "alpha", REPRESENTATIVE, "MEMORY_PROBE_REMEMBER").await;
        memory_probe(&ensemble, "alpha", REPRESENTATIVE, "MEMORY_PROBE_PRESENT").await;
        memory_probe(
            &ensemble,
            "alpha",
            "domain-coder",
            "MEMORY_PROBE_ABSENT_MEMBER",
        )
        .await;
        memory_probe(
            &ensemble,
            "beta",
            REPRESENTATIVE,
            "MEMORY_PROBE_ABSENT_REPO",
        )
        .await;
        ensemble.shutdown().await.expect("shutdown");
        drop(ensemble);

        // Native definition CAS recreates the previous release's persisted
        // memory-disabled profile policy, without editing SQLite or identities.
        for repo in config.repositories.keys() {
            let (storage, _) = meerkat_meta::mob_composition_manifest::persistent_mob_storage(
                config.state_root.join(repo.as_str()).join("mob.sqlite3"),
            )
            .expect("native storage");
            let previous = storage
                .created_definition_snapshot()
                .await
                .expect("snapshot")
                .expect("definition");
            let mut disabled = previous.definition().clone();
            for profile in disabled.profiles.values_mut() {
                if let ProfileBinding::Inline(profile) = profile {
                    profile.tools.memory = false;
                }
            }
            storage
                .update_definition(previous.epoch(), disabled)
                .await
                .expect("previous policy");
        }

        let client = Arc::new(MemoryScript::default());
        let restored = EnsembleRuntime::build_with_client(config, client.clone())
            .await
            .expect("upgrade saved profiles and restore memory");
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            0,
            "upgrade must not repeat study"
        );
        let after = restored.status().await.expect("restored status");
        for (before, after) in before.repositories.iter().zip(&after.repositories) {
            for (before, after) in before.members.iter().zip(&after.members) {
                assert_eq!(before.identity, after.identity);
                assert_eq!(
                    before.session_id, after.session_id,
                    "keep existing live identities"
                );
            }
        }
        memory_probe(
            &restored,
            "alpha",
            REPRESENTATIVE,
            "MEMORY_PROBE_PRESENT_RESTORED",
        )
        .await;
        memory_probe(
            &restored,
            "alpha",
            "domain-coder",
            "MEMORY_PROBE_ABSENT_MEMBER_RESTORED",
        )
        .await;
        memory_probe(
            &restored,
            "beta",
            REPRESENTATIVE,
            "MEMORY_PROBE_ABSENT_REPO_RESTORED",
        )
        .await;
        restored.shutdown().await.expect("shutdown");
    }

    async fn resolve_script_peers(ensemble: &EnsembleRuntime, client: &PeerScript) {
        for name in ["alpha", "beta"] {
            let repo = RepoId::try_from(name.to_string()).expect("id");
            let runtime = ensemble.repository(&repo).expect("runtime");
            let alias = runtime
                .identity_runtime()
                .expect("authority")
                .runtime_id_for(
                    &EnsembleRole::Representative
                        .identity(&repo)
                        .expect("identity"),
                )
                .await
                .expect("alias")
                .to_string();
            let (peer_id, _, _) = runtime
                .local_member_peer_info(&alias)
                .await
                .expect("peer info");
            client.peers.lock().expect("peers").insert(name, peer_id);
        }
    }

    async fn await_roundtrip(ensemble: &EnsembleRuntime, phase: &str) {
        let repo = RepoId::try_from("alpha".to_string()).expect("id");
        let runtime = ensemble.repository(&repo).expect("runtime");
        let identity = EnsembleRole::Representative
            .identity(&repo)
            .expect("identity");
        let authority = runtime.identity_runtime().expect("authority");
        let completed = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let inspection = authority.inspect(&identity).await.expect("inspect");
                if inspection
                    .output_preview
                    .as_deref()
                    .is_some_and(|text| text.contains(&format!("Round trip complete {phase}")))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        if let Err(error) = completed {
            for name in ["alpha", "beta"] {
                let repo = RepoId::try_from(name.to_owned()).expect("id");
                let runtime = ensemble.repository(&repo).expect("runtime");
                let authority = runtime.identity_runtime().expect("authority");
                let identity = EnsembleRole::Representative
                    .identity(&repo)
                    .expect("identity");
                eprintln!(
                    "federation timeout {name}: {:?}",
                    authority.inspect(&identity).await
                );
            }
            panic!("real peer response reaches origin: {error}");
        }
    }

    #[derive(Default)]
    struct StudyScript {
        steps: Mutex<BTreeMap<String, usize>>,
        denied: AtomicBool,
    }

    #[async_trait]
    impl LlmClient for StudyScript {
        fn project_replay_messages(
            &self,
            messages: &[Message],
        ) -> std::result::Result<Vec<Message>, LlmError> {
            Ok(messages.to_vec())
        }
        fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
            let transcript = serde_json::to_string(&request.messages).expect("transcript");
            let domain = ["storage-engine", "interface-contracts", "quality-strategy"]
                .into_iter()
                .find(|name| transcript.contains(&format!("DOMAIN_KEY={name}")))
                .expect("dynamic specialist charter");
            let mut steps = self.steps.lock().expect("steps");
            let step = steps.entry(domain.to_owned()).or_default();
            let current = *step;
            *step += 1;
            drop(steps);
            let result = |id: &str| {
                request.messages.iter().rev().find_map(|message| {
                    if let Message::ToolResults { results, .. } = message {
                        results.iter().find(|result| result.tool_use_id == id)
                    } else {
                        None
                    }
                })
            };
            let mut events = Vec::new();
            let stop_reason = match current {
                0 => {
                    events.push(LlmEvent::ToolCallComplete {
                        id: "study-read".into(),
                        name: "repo_read_file".into(),
                        args: json!({"path":"README.md"}),
                        meta: None,
                    });
                    events.push(LlmEvent::ToolCallComplete {
                        id: "study-denied-peers".into(),
                        name: "peers".into(),
                        args: json!({}),
                        meta: None,
                    });
                    if domain == "storage-engine" {
                        events.push(LlmEvent::ToolCallComplete {
                            id: "study-denied-edit".into(), name: "apply_patch".into(),
                            args: json!({"patch":"*** Begin Patch\n*** Add File: forbidden-study.txt\n+forbidden\n*** End Patch"}), meta: None,
                        });
                    }
                    StopReason::ToolUse
                }
                1 => {
                    assert!(
                        !result("study-read")
                            .expect("actual repository read")
                            .is_error
                    );
                    assert!(
                        result("study-denied-peers")
                            .expect("native comms gate")
                            .is_error
                    );
                    if domain == "storage-engine" {
                        assert!(
                            result("study-denied-edit")
                                .expect("denied edit feedback")
                                .is_error
                        );
                        self.denied.store(true, Ordering::SeqCst);
                    }
                    events.push(LlmEvent::TextDelta { delta: format!("Domain Brief for {domain}: Architecture: README.md documents this small repository and its principal boundary. Interfaces: source ownership stays inside the checkout. Invariants: preserve unrelated work and use the documented behavior as the contract. Tests: validate each change with the declared repository checks. Risks: the fixture contains limited implementation evidence, so inspect additional source before implementing. Sources: README.md was read directly during this study."), meta: None });
                    StopReason::EndTurn
                }
                2 if domain == "storage-engine" => {
                    events.push(LlmEvent::ToolCallComplete {
                        id: "post-study-edit".into(), name: "apply_patch".into(),
                        args: json!({"patch":"*** Begin Patch\n*** Add File: allowed-after-study.txt\n+verified\n*** End Patch"}), meta: None,
                    });
                    StopReason::ToolUse
                }
                3 if domain == "storage-engine" => {
                    assert!(
                        !result("post-study-edit")
                            .expect("post-study edit feedback")
                            .is_error
                    );
                    events.push(LlmEvent::TextDelta {
                        delta: "Post-study capability verified.".into(),
                        meta: None,
                    });
                    StopReason::EndTurn
                }
                _ => panic!("unexpected repeated study or turn for {domain}: {current}"),
            };
            events.push(LlmEvent::UsageUpdate {
                usage: TurnUsage::host_declared(Provider::OpenAI, &request.model, Usage::default()),
            });
            events.push(LlmEvent::Done {
                outcome: LlmDoneOutcome::Success { stop_reason },
            });
            Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
        }
        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
        async fn health_check(&self) -> std::result::Result<(), LlmError> {
            Ok(())
        }
    }

    struct FailedStudy(Option<&'static str>);

    #[async_trait]
    impl LlmClient for FailedStudy {
        fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
            let events = if let Some(text) = self.0 {
                vec![
                    Ok(LlmEvent::TextDelta {
                        delta: text.to_owned(),
                        meta: None,
                    }),
                    Ok(LlmEvent::UsageUpdate {
                        usage: TurnUsage::host_declared(
                            Provider::OpenAI,
                            &request.model,
                            Usage::default(),
                        ),
                    }),
                    Ok(LlmEvent::Done {
                        outcome: LlmDoneOutcome::Success {
                            stop_reason: StopReason::EndTurn,
                        },
                    }),
                ]
            } else {
                vec![Err(LlmError::InvalidRequest {
                    message: "scripted terminal study failure".into(),
                })]
            };
            Box::pin(futures::stream::iter(events))
        }
        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
        async fn health_check(&self) -> std::result::Result<(), LlmError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dynamic_specialists_study_read_only_then_keep_their_sessions_and_code_grants() {
        if !isolated("dynamic_specialists_study_read_only_then_keep_their_sessions_and_code_grants")
        {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let mut config = fixture_config(temp.path());
        config.repositories.retain(|id, _| id.as_str() == "alpha");
        config.peers.clear();
        // Start the old fixed fixture first: replacement must retire obsolete
        // identities, not leave hidden generic workers attached to the mob.
        let old =
            EnsembleRuntime::build_with_client(config.clone(), Arc::new(PeerScript::default()))
                .await
                .expect("old fixture");
        old.shutdown().await.expect("old shutdown");
        drop(old);
        let repo = RepoId::try_from("alpha".to_owned()).expect("repo");
        let root = &config.repositories[&repo].path;
        let team_path = config.state_root.join("alpha/team.json");
        let mut team = TeamManifest::load(&team_path, root)
            .expect("load fixture")
            .expect("team");
        let template = team.specialists.values().next().expect("template").clone();
        team.specialists.clear();
        for name in ["storage-engine", "interface-contracts", "quality-strategy"] {
            let mut spec = template.clone();
            spec.label = name.replace('-', " ");
            spec.focus = format!("Understand and maintain {name} boundaries");
            spec.charter = format!(
                "DOMAIN_KEY={name}. Own this repository domain with concrete file evidence and collaborate with other domain specialists."
            );
            if name == "quality-strategy" {
                spec.ownership_paths = vec![PathBuf::from(".")];
            }
            spec.capability = if name == "storage-engine" {
                Capability::Code
            } else {
                Capability::ReadOnly
            };
            team.specialists
                .insert(MemberId::try_from(name.to_owned()).expect("member"), spec);
        }
        std::fs::remove_file(&team_path).expect("replace fixture declaration");
        team.save(&team_path, root).expect("new manifest");
        std::fs::remove_file(config.state_root.join("alpha/studies.json")).expect("fresh studies");
        for response in [
            None,
            Some("OK"),
            Some(
                "A claimed Domain Brief without inspected evidence. Architecture: the implementation is organized into clear modules. Interfaces: modules expose well-defined boundaries. Invariants: correctness and isolation must hold across state transitions. Tests: a comprehensive suite validates the system. Risks: hidden assumptions should be investigated. Sources: this response claims expertise but deliberately reads no repository file, so the host must reject it.",
            ),
        ] {
            let failed =
                EnsembleRuntime::build_with_client(config.clone(), Arc::new(FailedStudy(response)))
                    .await;
            assert!(
                failed.is_err(),
                "failed or ungrounded studies cannot unlock the team"
            );
            let journal: StudyJournal = serde_json::from_slice(
                &std::fs::read(config.state_root.join("alpha/studies.json"))
                    .expect("failed journal"),
            )
            .expect("journal");
            assert!(journal.members.values().all(|record| !record.completed));
        }
        let client = Arc::new(StudyScript::default());
        let runtime = EnsembleRuntime::build_with_client(config.clone(), client.clone())
            .await
            .expect("dynamic runtime");
        assert!(client.denied.load(Ordering::SeqCst));
        assert!(!root.join("forbidden-study.txt").exists());
        let native = runtime.repository(&repo).expect("runtime");
        let active = native.mob_handle().list_members().await;
        assert_eq!(active.len(), 4, "obsolete fixed workers must retire");
        assert!(
            active
                .iter()
                .all(|member| !["planner", "coder", "reviewer"].contains(&member.role.as_str()))
        );
        let before = runtime.status().await.expect("status");
        assert!(
            before.repositories[0]
                .members
                .iter()
                .filter(|member| member.member_id != REPRESENTATIVE)
                .all(|member| matches!(member.study_state, StudyState::Completed))
        );
        let authority = native.identity_runtime().expect("authority");
        authority
            .send_awaiting_commit(
                &member_identity(&repo, "storage-engine").expect("identity"),
                &ContentInput::from("Execute post-study patch now."),
            )
            .await
            .expect("code grant unlocked");
        assert_eq!(
            std::fs::read_to_string(root.join("allowed-after-study.txt")).expect("proof"),
            "verified\n"
        );
        drop(authority);
        runtime.shutdown().await.expect("shutdown");
        drop(runtime);
        // Simulate a crash after native commit but before the host's study
        // journal commit. Native idempotency plus exact transcript correlation
        // must recover the brief without spending another model turn.
        let study_path = config.state_root.join("alpha/studies.json");
        let mut journal: StudyJournal =
            serde_json::from_slice(&std::fs::read(&study_path).expect("journal"))
                .expect("journal shape");
        let pending = journal.members.values_mut().next().expect("study");
        pending.completed = false;
        pending.brief = None;
        save_studies(&study_path, &journal).expect("simulate interrupted journal commit");
        let quiet = Arc::new(StudyScript::default());
        let restored = EnsembleRuntime::build_with_client(config, quiet.clone())
            .await
            .expect("restore dynamic sessions");
        assert!(
            quiet.steps.lock().expect("steps").is_empty(),
            "completed studies must never repeat"
        );
        let after = restored.status().await.expect("restored status");
        for (a, b) in before.repositories[0]
            .members
            .iter()
            .zip(&after.repositories[0].members)
        {
            assert_eq!(a.identity, b.identity);
            assert_eq!(a.session_id, b.session_id);
        }
        restored.shutdown().await.expect("final shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abrupt_process_exit_restores_member_transports_and_peer_delivery() {
        let name = "abrupt_process_exit_restores_member_transports_and_peer_delivery";
        if let Ok(path) = std::env::var("FUGUE_ABRUPT_CONFIG") {
            let config: EnsembleConfig =
                serde_json::from_slice(&std::fs::read(&path).expect("crash config"))
                    .expect("config shape");
            let runtime =
                EnsembleRuntime::build_with_client(config, Arc::new(PeerScript::default()))
                    .await
                    .expect("child build");
            let status = runtime.status().await.expect("child status");
            let sessions: BTreeMap<_, _> = status
                .repositories
                .iter()
                .flat_map(|repo| {
                    repo.members.iter().map(move |member| {
                        (
                            format!("{}/{}", repo.repo, member.member_id),
                            member.session_id.clone(),
                        )
                    })
                })
                .collect();
            std::fs::write(
                format!("{path}.sessions"),
                serde_json::to_vec(&sessions).expect("session json"),
            )
            .expect("save sessions");
            // Deliberately bypass every destructor and asynchronous shutdown.
            // This is an actual process loss, not graceful in-process restart.
            std::process::exit(0);
        }
        if !isolated(name) {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        let path = temp.path().join("abrupt-config.json");
        std::fs::write(&path, serde_json::to_vec(&config).expect("config json"))
            .expect("save config");
        let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                &format!("ensemble::runtime::tests::{name}"),
                "--nocapture",
            ])
            .env("FUGUE_ABRUPT_CONFIG", &path)
            .env("RUST_MIN_STACK", "16777216")
            .output()
            .expect("run crash child");
        assert!(
            child.status.success(),
            "crash child setup failed: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let before: BTreeMap<String, Option<String>> = serde_json::from_slice(
            &std::fs::read(format!("{}.sessions", path.display())).expect("sessions"),
        )
        .expect("sessions shape");
        let client = Arc::new(PeerScript {
            phase: "after-crash",
            ..PeerScript::default()
        });
        let runtime = EnsembleRuntime::build_with_client(config, client.clone())
            .await
            .expect("recover abrupt process loss");
        let after = runtime.status().await.expect("recovered status");
        for repo in &after.repositories {
            for member in &repo.members {
                assert_eq!(
                    before[&format!("{}/{}", repo.repo, member.member_id)],
                    member.session_id
                );
            }
        }
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            0,
            "recovery must not manufacture model turns"
        );
        resolve_script_peers(&runtime, &client).await;
        runtime
            .ask(
                &RepoId::try_from("alpha".to_owned()).expect("alpha"),
                "Start native round trip after-crash.",
            )
            .await
            .expect("admit after crash");
        await_roundtrip(&runtime, "after-crash").await;
        runtime
            .shutdown()
            .await
            .expect("shutdown recovered runtime");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_federation_is_idle_delivers_peer_messages_and_survives_restart() {
        if !isolated("native_federation_is_idle_delivers_peer_messages_and_survives_restart") {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        let client = Arc::new(PeerScript {
            phase: "first-exchange",
            ..PeerScript::default()
        });
        let ensemble = EnsembleRuntime::build_with_client(config.clone(), client.clone())
            .await
            .expect("build");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            0,
            "formation must not consume an LLM turn"
        );
        let before = ensemble.status().await.expect("status");
        assert_eq!(before.repositories.len(), 2);
        assert!(before.repositories.iter().all(|repo| {
            repo.members.len() == 4
                && repo
                    .members
                    .iter()
                    .all(|member| member.state == IdentityLifecycleState::Active)
        }));
        resolve_script_peers(&ensemble, &client).await;
        let alpha = RepoId::try_from("alpha".to_string()).expect("alpha");
        let native = ensemble.repository(&alpha).expect("native runtime");
        let roster = native.mob_handle().list_all_members().await;
        let local: BTreeSet<_> = roster
            .iter()
            .map(|member| member.agent_identity.clone())
            .collect();
        for member in &roster {
            let foreign = member
                .wired_to
                .iter()
                .filter(|peer| !local.contains(*peer))
                .count();
            assert_eq!(
                foreign,
                usize::from(member.role.as_str() == "representative"),
                "only the representative has foreign peers"
            );
        }
        let authority = native.identity_runtime().expect("authority");
        for role in EnsembleRole::ALL {
            authority
                .send_admission_tracked(
                    &role.identity(&alpha).expect("identity"),
                    None,
                    &ContentInput::from("Probe role tools and report readiness."),
                    meerkat_core::types::HandlingMode::Queue,
                    None,
                )
                .await
                .expect("role probe");
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            while client.calls.load(Ordering::SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("all role tools probed");
        authority
            .send_admission_tracked(
                &EnsembleRole::Coder
                    .identity(&alpha)
                    .expect("coder identity"),
                None,
                &ContentInput::from("Probe foreign boundary."),
                meerkat_core::types::HandlingMode::Queue,
                None,
            )
            .await
            .expect("boundary probe");
        tokio::time::timeout(Duration::from_secs(20), async {
            while !client.foreign_denied.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("native comms rejects direct coder cross-repository access");
        drop(authority);
        ensemble
            .ask(&alpha, "Coordinate first-exchange with beta.")
            .await
            .expect("ask");
        await_roundtrip(&ensemble, "first-exchange").await;
        assert!(client.sent.load(Ordering::SeqCst) && client.replied.load(Ordering::SeqCst));
        ensemble.shutdown().await.expect("shutdown");
        assert!(ensemble.ask(&alpha, "late request").await.is_err());
        drop(ensemble);
        let next = Arc::new(PeerScript {
            phase: "after-restart",
            ..PeerScript::default()
        });
        let restarted = EnsembleRuntime::build_with_client(config, next.clone())
            .await
            .expect("restart");
        let after = restarted.status().await.expect("restored status");
        for (before_repo, after_repo) in before.repositories.iter().zip(&after.repositories) {
            for (before_member, after_member) in before_repo.members.iter().zip(&after_repo.members)
            {
                assert_eq!(before_member.identity, after_member.identity);
                assert_eq!(
                    before_member.session_id, after_member.session_id,
                    "native session history must survive"
                );
            }
        }
        resolve_script_peers(&restarted, &next).await;
        restarted
            .ask(&alpha, "Coordinate after-restart with beta.")
            .await
            .expect("ask after restart");
        await_roundtrip(&restarted, "after-restart").await;
        restarted.shutdown().await.expect("final shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_config_removal_revokes_peers_and_readdition_preserves_memory() {
        if !isolated("native_config_removal_revokes_peers_and_readdition_preserves_memory") {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        let alpha = RepoId::try_from("alpha".to_string()).expect("alpha");
        let beta = RepoId::try_from("beta".to_string()).expect("beta");
        let first =
            EnsembleRuntime::build_with_client(config.clone(), Arc::new(PeerScript::default()))
                .await
                .expect("initial configuration");
        let original = first.status().await.expect("original status");
        first.shutdown().await.expect("initial shutdown");
        drop(first);

        let mut without_beta = config.clone();
        without_beta.peers.clear();
        without_beta.repositories.remove(&beta);
        let idle = Arc::new(PeerScript::default());
        let remaining = EnsembleRuntime::build_with_client(without_beta, idle.clone())
            .await
            .expect("temporarily remove repository");
        assert_eq!(
            remaining
                .status()
                .await
                .expect("remaining status")
                .repositories
                .len(),
            1
        );
        assert_eq!(idle.calls.load(Ordering::SeqCst), 0);
        assert_only_local_peers(&remaining, &alpha).await;
        remaining.shutdown().await.expect("remaining shutdown");
        drop(remaining);

        let mut disconnected = config.clone();
        disconnected.peers.clear();
        let restored = EnsembleRuntime::build_with_client(disconnected, idle.clone())
            .await
            .expect("restore repository without its old peer link");
        assert_only_local_peers(&restored, &alpha).await;
        assert_only_local_peers(&restored, &beta).await;
        assert_eq!(idle.calls.load(Ordering::SeqCst), 0);
        restored.shutdown().await.expect("disconnected shutdown");
        drop(restored);

        let client = Arc::new(PeerScript {
            phase: "readded-link",
            ..PeerScript::default()
        });
        let reconnected = EnsembleRuntime::build_with_client(config, client.clone())
            .await
            .expect("re-add configured link");
        let current = reconnected.status().await.expect("reconnected status");
        for (old_repo, new_repo) in original.repositories.iter().zip(&current.repositories) {
            for (old, new) in old_repo.members.iter().zip(&new_repo.members) {
                assert_eq!(old.identity, new.identity);
                assert_eq!(old.session_id, new.session_id);
            }
        }
        assert_eq!(client.calls.load(Ordering::SeqCst), 0);
        resolve_script_peers(&reconnected, &client).await;
        reconnected
            .ask(&alpha, "Coordinate readded-link with beta.")
            .await
            .expect("readded ask");
        await_roundtrip(&reconnected, "readded-link").await;
        reconnected.shutdown().await.expect("reconnected shutdown");
    }

    async fn assert_only_local_peers(ensemble: &EnsembleRuntime, repo: &RepoId) {
        let runtime = ensemble.repository(repo).expect("runtime");
        let members = runtime.mob_handle().list_all_members().await;
        let local: BTreeSet<_> = members
            .iter()
            .map(|member| &member.agent_identity)
            .collect();
        for member in &members {
            assert!(
                member.wired_to.iter().all(|peer| local.contains(peer)),
                "removed repository/link remains wired for {}: {:?}",
                member.role,
                member.wired_to
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_second_repository_releases_started_runtime_and_state_lock() {
        if !isolated("failed_second_repository_releases_started_runtime_and_state_lock") {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let mut config = fixture_config(temp.path());
        config.resolve_and_validate(temp.path()).expect("config");
        drop(config.claim_state().expect("claim empty state"));
        let invalid_store = config.state_root.join("beta/mob.sqlite3");
        std::fs::create_dir_all(&invalid_store).expect("invalid database directory");
        let client = Arc::new(PeerScript::default());
        let failed = EnsembleRuntime::build_with_client(config.clone(), client.clone()).await;
        assert!(failed.is_err(), "opening a directory as SQLite must fail");
        assert_eq!(client.calls.load(Ordering::SeqCst), 0);
        std::fs::remove_dir(&invalid_store).expect("remove invalid database");
        let recovered = EnsembleRuntime::build_with_client(config, client)
            .await
            .expect("partial-build cleanup must release both native ownership and state lock");
        assert_eq!(
            recovered.status().await.expect("status").repositories.len(),
            2
        );
        recovered.shutdown().await.expect("shutdown");
    }
    struct HandoffScript {
        fail_worker: bool,
        pause_worker: bool,
        workers: AtomicUsize,
        handoffs: AtomicUsize,
        returned: Mutex<Option<String>>,
    }

    #[async_trait]
    impl LlmClient for HandoffScript {
        async fn health_check(&self) -> std::result::Result<(), LlmError> {
            Ok(())
        }
        fn project_replay_messages(
            &self,
            messages: &[Message],
        ) -> std::result::Result<Vec<Message>, LlmError> {
            Ok(messages.to_vec())
        }
        fn stream<'a>(&'a self, request: &'a LlmRequest) -> LlmStream<'a> {
            let latest_user = request
                .messages
                .iter()
                .rev()
                .find_map(|message| match message {
                    Message::User(user) => {
                        Some(serde_json::to_string(&user.content).expect("user"))
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let worker = latest_user.contains("[ENSEMBLE DELEGATED WORK]");
            let handoff = latest_user.contains("[ENSEMBLE DURABLE HANDOFF]")
                || latest_user.contains("[ENSEMBLE INTERRUPTED HANDOFF]");
            if handoff && std::env::var_os("FUGUE_HANDOFF_CRASH_CONFIG").is_some() {
                // The worker's result is already in the native flow store, but
                // this return turn has not produced any output or side effect.
                std::process::exit(0);
            }
            if worker {
                self.workers.fetch_add(1, Ordering::SeqCst);
            }
            if worker && self.pause_worker {
                return Box::pin(futures::stream::pending());
            }
            if worker && self.fail_worker {
                return Box::pin(futures::stream::iter(vec![Err(LlmError::InvalidRequest {
                    message: "intentional worker failure".into(),
                })]));
            }
            let text = if worker {
                "WORKER FINAL EVIDENCE"
            } else if handoff {
                self.handoffs.fetch_add(1, Ordering::SeqCst);
                *self.returned.lock().expect("returned") = Some(latest_user);
                "HANDOFF RECEIVED"
            } else {
                "Ready."
            };
            Box::pin(futures::stream::iter(vec![
                Ok(LlmEvent::TextDelta {
                    delta: text.into(),
                    meta: None,
                }),
                Ok(LlmEvent::UsageUpdate {
                    usage: TurnUsage::host_declared(
                        Provider::OpenAI,
                        &request.model,
                        Usage::default(),
                    ),
                }),
                Ok(LlmEvent::Done {
                    outcome: LlmDoneOutcome::Success {
                        stop_reason: StopReason::EndTurn,
                    },
                }),
            ]))
        }
        fn provider(&self) -> Provider {
            Provider::OpenAI
        }
    }

    async fn assert_native_handoff(fail_worker: bool) {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = fixture_config(temp.path());
        let client = Arc::new(HandoffScript {
            fail_worker,
            pause_worker: false,
            workers: AtomicUsize::new(0),
            handoffs: AtomicUsize::new(0),
            returned: Mutex::new(None),
        });
        let ensemble = EnsembleRuntime::build_with_client(config.clone(), client.clone())
            .await
            .expect("runtime");
        let repo = RepoId::try_from("alpha".to_owned()).expect("repo");
        let runtime = ensemble.repository(&repo).expect("repository");
        let handle = runtime.mob_handle();
        let slot = Arc::new(std::sync::OnceLock::new());
        slot.set(Arc::downgrade(runtime)).expect("bind runtime");
        let tools = super::super::handoff::HandoffTools::new(slot);
        let input = serde_json::value::to_raw_value(&json!({
            "member":"coder", "correlation":"work-42/review-1", "task":"Inspect the implementation"
        }))
        .expect("args");
        let call = meerkat_core::ToolCallView {
            id: "delegate-1",
            name: "ensemble_delegate",
            args: &input,
        };
        let accepted = tools.dispatch(call).await.expect("flow admitted");
        #[derive(Deserialize)]
        struct Receipt {
            run_id: meerkat_mob::RunId,
            reused: bool,
        }
        let receipt: Receipt =
            serde_json::from_str(&accepted.result.text_content()).expect("receipt");
        assert!(!receipt.reused);
        let run_id = receipt.run_id;
        let replay = tools.dispatch(call).await.expect("retry admission");
        let replay: Receipt =
            serde_json::from_str(&replay.result.text_content()).expect("replay receipt");
        assert_eq!(replay.run_id, run_id);
        assert!(replay.reused);
        let changed = serde_json::value::to_raw_value(&json!({
            "member":"coder", "correlation":"work-42/review-1", "task":"Different task"
        }))
        .expect("args");
        assert!(
            tools
                .dispatch(meerkat_core::ToolCallView {
                    args: &changed,
                    ..call
                })
                .await
                .is_err(),
            "same key with changed task must fail"
        );
        let run = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let run = handle
                    .flow_status(run_id.clone())
                    .await
                    .expect("status")
                    .expect("run");
                if matches!(
                    run.status,
                    meerkat_mob::MobRunStatus::Completed
                        | meerkat_mob::MobRunStatus::Failed
                        | meerkat_mob::MobRunStatus::Canceled
                ) {
                    break run;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("bounded native handoff");
        assert_eq!(
            run.status,
            if fail_worker {
                meerkat_mob::MobRunStatus::Failed
            } else {
                meerkat_mob::MobRunStatus::Completed
            },
            "native status is preserved: {run:?}"
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while client.handoffs.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("automatic terminal outcome arrives");
        assert_eq!(
            client.handoffs.load(Ordering::SeqCst),
            1,
            "exactly one native return without any peer send"
        );
        let text = client
            .returned
            .lock()
            .expect("returned")
            .clone()
            .expect("return text");
        assert!(text.contains("work-42/review-1"), "exact correlation");
        assert_eq!(text.contains("WORKER FINAL EVIDENCE"), !fail_worker);
        assert_eq!(
            run.failure_ledger.is_empty(),
            !fail_worker,
            "worker failure remains durable, never laundered as task success"
        );
        ensemble.shutdown().await.expect("shutdown");
        drop(handle);
        drop(ensemble);
        let restored = EnsembleRuntime::build_with_client(config, client.clone())
            .await
            .expect("restart");
        let slot = Arc::new(std::sync::OnceLock::new());
        slot.set(Arc::downgrade(
            restored.repository(&repo).expect("restored repository"),
        ))
        .expect("rebind");
        let tools = super::super::handoff::HandoffTools::new(slot);
        let replay = tools.dispatch(call).await.expect("retry after restart");
        let replay: Receipt =
            serde_json::from_str(&replay.result.text_content()).expect("persisted receipt");
        assert_eq!(replay.run_id, run_id);
        assert!(replay.reused);
        assert_eq!(
            client.handoffs.load(Ordering::SeqCst),
            1,
            "restart never repeats completed handoff"
        );
        restored.shutdown().await.expect("final shutdown");
    }

    #[tokio::test]
    async fn native_flow_delivers_worker_final_without_peer_reply() {
        if !isolated("native_flow_delivers_worker_final_without_peer_reply") {
            return;
        }
        assert_native_handoff(false).await;
    }

    #[tokio::test]
    async fn native_flow_delivers_worker_failure_without_peer_reply() {
        if !isolated("native_flow_delivers_worker_failure_without_peer_reply") {
            return;
        }
        assert_native_handoff(true).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_handoff_recovers_worker_result_after_process_loss() {
        let name = "native_handoff_recovers_worker_result_after_process_loss";
        if let Ok(path) = std::env::var("FUGUE_HANDOFF_CRASH_CONFIG") {
            let config: EnsembleConfig =
                serde_json::from_slice(&std::fs::read(path).expect("config")).expect("shape");
            let script = Arc::new(HandoffScript {
                fail_worker: false,
                pause_worker: false,
                workers: AtomicUsize::new(0),
                handoffs: AtomicUsize::new(0),
                returned: Mutex::new(None),
            });
            let ensemble = EnsembleRuntime::build_with_client(config, script)
                .await
                .expect("child runtime");
            let repo = RepoId::try_from("alpha".to_owned()).expect("repo");
            let handle = ensemble.repository(&repo).expect("repository").mob_handle();
            let flow_id = meerkat_mob::FlowId::from("ensemble-delegate-coder");
            let run_id = meerkat_mob::RunId::for_work_execution(
                handle.mob_id(),
                &flow_id,
                "crash-correlation",
            );
            let delivery = meerkat_mob::store::MobExternalDeliveryIdentity::new(
                "crash-correlation",
                run_id.to_string(),
            )
            .expect("delivery");
            handle
                .run_flow_with_external_identity(
                    flow_id,
                    json!({"correlation":"crash-correlation", "task":"Inspect the implementation"}),
                    &delivery,
                )
                .await
                .expect("launch");
            tokio::time::sleep(Duration::from_secs(40)).await;
            panic!("child never reached return turn");
        }
        if !isolated(name) {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        let path = temp.path().join("crash.json");
        std::fs::write(&path, serde_json::to_vec(&config).expect("config JSON"))
            .expect("config file");
        let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                &format!("ensemble::runtime::tests::{name}"),
                "--nocapture",
            ])
            .env("FUGUE_HANDOFF_CRASH_CONFIG", &path)
            .env("RUST_MIN_STACK", "16777216")
            .output()
            .expect("child");
        assert!(
            child.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let script = Arc::new(HandoffScript {
            fail_worker: false,
            pause_worker: false,
            workers: AtomicUsize::new(0),
            handoffs: AtomicUsize::new(0),
            returned: Mutex::new(None),
        });
        let ensemble = EnsembleRuntime::build_with_client(config.clone(), script.clone())
            .await
            .expect("restore");
        let repo = RepoId::try_from("alpha".to_owned()).expect("repo");
        let handle = ensemble.repository(&repo).expect("repository").mob_handle();
        let run_id = meerkat_mob::RunId::for_work_execution(
            handle.mob_id(),
            &"ensemble-delegate-coder".into(),
            "crash-correlation",
        );
        let run = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let run = handle
                    .flow_status(run_id.clone())
                    .await
                    .expect("status")
                    .expect("run");
                if !matches!(
                    run.status,
                    meerkat_mob::MobRunStatus::Pending | meerkat_mob::MobRunStatus::Running
                ) {
                    break run;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("recovered handoff terminates");
        assert_eq!(
            run.status,
            meerkat_mob::MobRunStatus::Completed,
            "native worker completion survives process loss: {run:?}"
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while script.handoffs.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("recovery notification arrives");
        assert_eq!(
            script.handoffs.load(Ordering::SeqCst),
            1,
            "pending return wakes once without operator input"
        );
        assert!(
            script
                .returned
                .lock()
                .expect("return")
                .as_ref()
                .is_some_and(|text| text.contains("WORKER FINAL EVIDENCE"))
        );
        assert_eq!(
            script.workers.load(Ordering::SeqCst),
            0,
            "recovery must never repeat worker side effects"
        );
        let authority = ensemble
            .repository(&repo)
            .expect("runtime")
            .identity_runtime()
            .expect("authority");
        let identity = member_identity(&repo, REPRESENTATIVE).expect("identity");
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if authority
                    .inspect(&identity)
                    .await
                    .expect("inspect")
                    .output_preview
                    .as_deref()
                    == Some("HANDOFF RECEIVED")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("notification commits");
        drop(authority);
        ensemble.shutdown().await.expect("shutdown");
        drop(handle);
        drop(ensemble);
        let restarted = EnsembleRuntime::build_with_client(config, script.clone())
            .await
            .expect("second restore");
        let runtime = restarted.repository(&repo).expect("restarted runtime");
        let tickets = super::super::handoff::notify_terminal_handoffs(runtime, &identity)
            .await
            .expect("deduplicated notification");
        assert_eq!(tickets.len(), 1);
        for ticket in tickets {
            let outcome = runtime
                .identity_runtime()
                .expect("authority")
                .wait_for_turn(&identity, ticket, Duration::from_secs(20))
                .await;
            assert!(
                matches!(outcome, TurnOutcome::Completed { .. }),
                "deduplicated exact admission settles: {outcome:?}"
            );
        }
        assert_eq!(
            script.handoffs.load(Ordering::SeqCst),
            1,
            "native inbox deduplicates recovery across restarts"
        );
        restarted.shutdown().await.expect("final shutdown");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_handoff_notifies_live_cancellation() {
        if !isolated("native_handoff_notifies_live_cancellation") {
            return;
        }
        let temp = tempfile::tempdir().expect("temp");
        let config = fixture_config(temp.path());
        let script = Arc::new(HandoffScript {
            fail_worker: false,
            pause_worker: true,
            workers: AtomicUsize::new(0),
            handoffs: AtomicUsize::new(0),
            returned: Mutex::new(None),
        });
        let ensemble = EnsembleRuntime::build_with_client(config, script.clone())
            .await
            .expect("build");
        let repo = RepoId::try_from("alpha".to_owned()).expect("repo");
        let handle = ensemble
            .repository(&repo)
            .expect("repo runtime")
            .mob_handle();
        let run_id = handle
            .run_flow(
                "ensemble-delegate-coder".into(),
                json!({
                    "correlation":"cancel-correlation", "task":"Wait for cancellation"
                }),
            )
            .await
            .expect("flow");
        tokio::time::timeout(Duration::from_secs(20), async {
            while script.workers.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("worker starts");
        let slot = Arc::new(std::sync::OnceLock::new());
        slot.set(Arc::downgrade(ensemble.repository(&repo).expect("runtime")))
            .expect("slot");
        let tools = super::super::handoff::HandoffTools::new(slot);
        for (member, correlation) in [
            ("coder", "overlap"),
            ("representative", "self"),
            ("unknown", "missing"),
        ] {
            let args = serde_json::value::to_raw_value(
                &json!({"member":member, "correlation":correlation, "task":"Must not start"}),
            )
            .expect("args");
            let outcome = tools
                .dispatch(meerkat_core::ToolCallView {
                    id: "invalid",
                    name: "ensemble_delegate",
                    args: &args,
                })
                .await;
            assert!(
                outcome.is_err(),
                "busy, self and unknown targets fail before admission"
            );
        }
        handle.cancel_flow(run_id.clone()).await.expect("cancel");
        tokio::time::timeout(Duration::from_secs(20), async {
            while script.handoffs.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("live cancellation reaches representative");
        assert_eq!(script.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(script.workers.load(Ordering::SeqCst), 1);
        let text = script
            .returned
            .lock()
            .expect("returned")
            .clone()
            .expect("notice");
        assert!(text.contains("cancel-correlation"));
        assert!(text.contains(&run_id.to_string()));
        assert!(text.contains("canceled"));
        let entry = ensemble.repositories.get(&repo).expect("entry");
        entry
            .handoff_notifications
            .lock()
            .await
            .as_ref()
            .expect("watcher")
            .abort();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if entry
                    .handoff_notifications
                    .lock()
                    .await
                    .as_ref()
                    .expect("watcher")
                    .is_finished()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watcher stops");
        let health = ensemble.status().await;
        assert!(
            health.is_err(),
            "lost outcome delivery fails readiness closed"
        );
        assert!(
            ensemble
                .ask(&repo, "Must not admit more work")
                .await
                .is_err()
        );
        ensemble.shutdown().await.expect("shutdown");
    }
}
