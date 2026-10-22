//! Validated, portable configuration for persistent repository teams.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RepoId(String);

impl RepoId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RepoId {
    type Error = String;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        if value.len() > 64
            || !value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err(format!(
                "invalid repository ID {value:?}: use 1–64 lowercase letters, digits, '-' or '_', starting with a letter"
            ));
        }
        Ok(Self(value))
    }
}

impl From<RepoId> for String {
    fn from(value: RepoId) -> Self {
        value.0
    }
}

impl AsRef<str> for RepoId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsembleConfig {
    #[serde(default)]
    pub notifications: super::notifications::NotificationConfig,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default = "default_state_root")]
    pub state_root: PathBuf,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_binding: Option<meerkat_core::AuthBindingRef>,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    #[serde(deserialize_with = "deserialize_repositories")]
    pub repositories: BTreeMap<RepoId, RepositoryConfig>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryConfig {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// An undirected connection between the two repository representatives.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub from: RepoId,
    pub to: RepoId,
}

/// Owns the single-daemon lease for an Ensemble state directory.
///
/// The lock file is deliberately retained after shutdown. Deleting it would
/// let another process lock a new inode while an existing owner remains alive.
#[derive(Debug)]
pub struct EnsembleStateGuard {
    _lock: fs::File,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateManifest {
    version: u32,
    repositories: BTreeMap<RepoId, PathBuf>,
}

fn deserialize_repositories<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<RepoId, RepositoryConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct RepositoriesVisitor;
    impl<'de> serde::de::Visitor<'de> for RepositoriesVisitor {
        type Value = BTreeMap<RepoId, RepositoryConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map of unique repository IDs to checkout configurations")
        }

        fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut repositories = BTreeMap::new();
            while let Some((id, repository)) = access.next_entry::<RepoId, RepositoryConfig>()? {
                match repositories.entry(id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(repository);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate repository ID {}",
                            entry.key()
                        )));
                    }
                }
            }
            Ok(repositories)
        }
    }
    deserializer.deserialize_map(RepositoriesVisitor)
}

impl EnsembleConfig {
    /// Check the actual discovered roster, including its representative.
    pub fn validate_team_capacity(
        &self,
        repository: &RepoId,
        team: &super::team::TeamManifest,
    ) -> Result<()> {
        let required = team.specialists.len() + 1;
        if self.max_sessions < required {
            bail!(
                "repository {repository} requires {required} sessions for its discovered specialists and representative; max_sessions is {}",
                self.max_sessions
            );
        }
        Ok(())
    }

    /// Claim this state directory and bind repository namespaces to checkouts.
    ///
    /// Keep the returned guard alive until every native runtime has shut down.
    pub fn claim_state(&self) -> Result<EnsembleStateGuard> {
        let mut config = self.clone();
        config.resolve_and_validate(&std::env::current_dir()?)?;
        fs::create_dir_all(&config.state_root).context("create Ensemble state directory")?;
        let root = config
            .state_root
            .canonicalize()
            .context("resolve Ensemble state directory")?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("ensemble.lock"))
            .context("open Ensemble ownership lock")?;
        lock.try_lock().with_context(|| {
            format!(
                "cannot claim Ensemble state {}; another daemon may already own it",
                root.display()
            )
        })?;
        let guard = EnsembleStateGuard { _lock: lock };
        let manifest_path = root.join("ensemble-state.json");
        let mut manifest = match fs::read(&manifest_path) {
            Ok(bytes) => serde_json::from_slice::<StateManifest>(&bytes)
                .context("decode Ensemble state ownership manifest")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for entry in fs::read_dir(&root).context("inspect Ensemble state directory")? {
                    let entry = entry.context("read Ensemble state directory entry")?;
                    if entry.file_name() != "ensemble.lock" {
                        bail!(
                            "Ensemble state {} is nonempty but lacks its ownership manifest; restore ensemble-state.json or choose a new state_root",
                            root.display()
                        );
                    }
                }
                StateManifest {
                    version: 1,
                    repositories: BTreeMap::new(),
                }
            }
            Err(error) => return Err(error).context("read Ensemble state ownership manifest"),
        };
        if manifest.version != 1 {
            bail!(
                "unsupported Ensemble state manifest version {}",
                manifest.version
            );
        }
        let mut changed = false;
        for (id, repository) in &config.repositories {
            match manifest.repositories.get(id) {
                Some(path) if *path != repository.path => {
                    bail!(
                        "repository {id} already owns conversations for {}; refusing to attach them to {}. Use a new repository ID or state_root",
                        path.display(),
                        repository.path.display()
                    );
                }
                Some(_) => {}
                None => {
                    manifest
                        .repositories
                        .insert(id.clone(), repository.path.clone());
                    changed = true;
                }
            }
        }
        if changed {
            let bytes = serde_json::to_vec_pretty(&manifest)
                .context("encode Ensemble state ownership manifest")?;
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .context("system clock precedes the Unix epoch")?
                .as_nanos();
            let temporary = root.join(format!(
                ".ensemble-state-{}-{nonce}.tmp",
                std::process::id()
            ));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .context("create temporary Ensemble state manifest")?;
            file.write_all(&bytes)
                .context("write Ensemble state manifest")?;
            file.sync_all().context("flush Ensemble state manifest")?;
            drop(file);
            fs::rename(&temporary, &manifest_path).context("publish Ensemble state manifest")?;
            #[cfg(unix)]
            fs::File::open(&root)?
                .sync_all()
                .context("flush Ensemble state directory")?;
        }
        Ok(guard)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let path = path
            .canonicalize()
            .with_context(|| format!("resolve Ensemble configuration {}", path.display()))?;
        let text = fs::read_to_string(&path)
            .with_context(|| format!("read Ensemble configuration {}", path.display()))?;
        let mut config: Self =
            serde_yaml::from_str(&text).context("parse Ensemble configuration")?;
        let parent = path
            .parent()
            .context("configuration has no parent directory")?;
        config.resolve_and_validate(parent)?;
        Ok(config)
    }

    /// Resolve configured paths relative to the configuration file's directory.
    pub fn resolve_and_validate(&mut self, base: &Path) -> Result<()> {
        if self.notifications.enabled && self.notifications.poll_interval_secs == 0 {
            bail!("notification poll_interval_secs must be positive");
        }
        if self.version != 1 {
            bail!(
                "unsupported Ensemble configuration version {}",
                self.version
            );
        }
        if self.name.trim().is_empty() || self.model.trim().is_empty() {
            bail!("Ensemble name and model must be nonempty");
        }
        if self.max_sessions < super::team::MIN_SPECIALISTS + 1 {
            bail!(
                "max_sessions must be at least 2; it must also fit each repository's discovered specialists and representative"
            );
        }
        if self.state_root.as_os_str().is_empty() {
            bail!("state_root must be nonempty");
        }
        if self.state_root.is_relative() {
            self.state_root = base.join(&self.state_root);
        }
        let mut paths = BTreeSet::new();
        for (id, repository) in &mut self.repositories {
            let path = if repository.path.is_relative() {
                base.join(&repository.path)
            } else {
                repository.path.clone()
            };
            repository.path =
                repository_root(&path).with_context(|| format!("validate repository {id}"))?;
            if !paths.insert(repository.path.clone()) {
                bail!(
                    "repository {id} duplicates checkout {}",
                    repository.path.display()
                );
            }
        }
        let mut edges = BTreeSet::new();
        for edge in &self.peers {
            if edge.from == edge.to {
                bail!("repository {} cannot peer with itself", edge.from);
            }
            for id in [&edge.from, &edge.to] {
                if !self.repositories.contains_key(id) {
                    bail!("peer connection refers to unknown repository {id}");
                }
            }
            let pair = if edge.from < edge.to {
                (&edge.from, &edge.to)
            } else {
                (&edge.to, &edge.from)
            };
            if !edges.insert(pair) {
                bail!("duplicate peer connection {} ↔ {}", edge.from, edge.to);
            }
        }
        Ok(())
    }

    /// Generate a team for each checkout and connect their representatives.
    pub fn for_repositories(paths: &[PathBuf], name: String) -> Result<Self> {
        let mut repositories = BTreeMap::new();
        for path in paths {
            let path = repository_root(path)?;
            let basename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("repo");
            let normalized: String = basename
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() {
                        ch.to_ascii_lowercase()
                    } else {
                        '-'
                    }
                })
                .take(52)
                .collect();
            let base = if normalized
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
            {
                normalized
            } else {
                format!("repo-{normalized}")
            };
            let mut id = RepoId::try_from(base.clone()).map_err(anyhow::Error::msg)?;
            let mut suffix = 2;
            while repositories.contains_key(&id) {
                id = RepoId::try_from(format!("{base}-{suffix}")).map_err(anyhow::Error::msg)?;
                suffix += 1;
            }
            repositories.insert(
                id,
                RepositoryConfig {
                    path,
                    description: None,
                },
            );
        }
        let ids: Vec<_> = repositories.keys().cloned().collect();
        let mut peers = Vec::new();
        for (index, from) in ids.iter().enumerate() {
            peers.extend(ids[index + 1..].iter().map(|to| PeerConfig {
                from: from.clone(),
                to: to.clone(),
            }));
        }
        let mut config = Self {
            notifications: Default::default(),
            version: default_version(),
            name,
            state_root: default_state_root(),
            model: default_model(),
            auth_binding: None,
            max_sessions: default_max_sessions(),
            repositories,
            peers,
        };
        // Validation leaves the generated state directory relative for portability.
        config.resolve_and_validate(&std::env::current_dir()?)?;
        config.state_root = default_state_root();
        Ok(config)
    }
}

fn repository_root(path: &Path) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve repository {}", path.display()))?;
    if !canonical.is_dir() {
        bail!(
            "repository path is not a directory: {}",
            canonical.display()
        );
    }
    let result = Command::new("git")
        .args(["-C"])
        .arg(&canonical)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .context("run git to validate repository")?;
    if !result.status.success() {
        bail!("not a Git working tree: {}", canonical.display());
    }
    let root = PathBuf::from(
        String::from_utf8(result.stdout)
            .context("Git path is not UTF-8")?
            .trim(),
    )
    .canonicalize()
    .context("resolve Git working tree root")?;
    if root != canonical {
        bail!(
            "use the repository root {} instead of {}",
            root.display(),
            canonical.display()
        );
    }
    Ok(root)
}

fn default_version() -> u32 {
    1
}
fn default_name() -> String {
    "My Ensemble".to_string()
}
fn default_state_root() -> PathBuf {
    PathBuf::from(".fugue/ensemble")
}
fn default_model() -> String {
    crate::config::default_agent_model().to_string()
}
fn default_max_sessions() -> usize {
    64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git_repo(path: &Path) {
        fs::create_dir_all(path).unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .arg(path)
                .status()
                .unwrap()
                .success()
        );
    }

    #[test]
    fn generation_and_load_preserve_roots_and_bidirectional_edges() {
        let temp = tempfile::tempdir().unwrap();
        let a = temp.path().join("api");
        let b = temp.path().join("web");
        git_repo(&a);
        git_repo(&b);
        let config = EnsembleConfig::for_repositories(&[a.clone(), b], "Product".into()).unwrap();
        assert_eq!(config.repositories.len(), 2);
        assert_eq!(config.peers.len(), 1);
        let file = temp.path().join("ensemble.yaml");
        fs::write(&file, serde_yaml::to_string(&config).unwrap()).unwrap();
        let loaded = EnsembleConfig::load(&file).unwrap();
        assert_eq!(
            loaded.repositories[&RepoId::try_from("api".to_owned()).unwrap()].path,
            a.canonicalize().unwrap()
        );
        assert_eq!(
            loaded.state_root,
            temp.path().canonicalize().unwrap().join(".fugue/ensemble")
        );
    }

    #[test]
    fn rejects_duplicate_checkouts_and_invalid_ids() {
        let temp = tempfile::tempdir().unwrap();
        git_repo(temp.path());
        assert!(
            EnsembleConfig::for_repositories(&[temp.path().into(), temp.path().into()], "x".into())
                .is_err()
        );
        for value in ["", "../repo", "repo/escape", "UPPER", "1repo", "repo space"] {
            assert!(RepoId::try_from(value.to_string()).is_err(), "{value}");
        }
    }

    #[test]
    fn rejects_unknown_fields_and_invalid_edges() {
        assert!(serde_yaml::from_str::<EnsembleConfig>("repositories: {}\nmodle: typo").is_err());
        assert!(
            serde_yaml::from_str::<EnsembleConfig>(
                "repositories:\n  api:\n    path: a\n  api:\n    path: b\n"
            )
            .is_err()
        );
        let temp = tempfile::tempdir().unwrap();
        git_repo(temp.path());
        let mut config =
            EnsembleConfig::for_repositories(&[temp.path().into()], "x".into()).unwrap();
        let id = config.repositories.keys().next().unwrap().clone();
        config.peers.push(PeerConfig {
            from: id.clone(),
            to: id,
        });
        assert!(config.resolve_and_validate(temp.path()).is_err());
        config.peers[0].to = RepoId::try_from("missing".to_owned()).unwrap();
        assert!(config.resolve_and_validate(temp.path()).is_err());
        config.peers.clear();
        config.max_sessions = 1;
        assert!(config.resolve_and_validate(temp.path()).is_err());
        config.max_sessions = 2;
        assert!(config.resolve_and_validate(temp.path()).is_ok());
    }

    #[test]
    fn rejects_reverse_duplicate_edges_and_nested_directories() {
        let temp = tempfile::tempdir().unwrap();
        let a = temp.path().join("api");
        let b = temp.path().join("web");
        git_repo(&a);
        git_repo(&b);
        let mut config =
            EnsembleConfig::for_repositories(&[a.clone(), b], "Product".into()).unwrap();
        let edge = &config.peers[0];
        config.peers.push(PeerConfig {
            from: edge.to.clone(),
            to: edge.from.clone(),
        });
        assert!(config.resolve_and_validate(temp.path()).is_err());
        let nested = a.join("src");
        fs::create_dir(&nested).unwrap();
        assert!(EnsembleConfig::for_repositories(&[nested], "Product".into()).is_err());
    }

    #[test]
    fn state_claim_is_exclusive_and_released_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        git_repo(&repo);
        let mut config = EnsembleConfig::for_repositories(&[repo], "Test".into()).unwrap();
        config.state_root = temp.path().join("state");
        let guard = config.claim_state().unwrap();
        assert!(config.claim_state().is_err());
        drop(guard);
        assert!(config.claim_state().is_ok());
    }

    #[test]
    fn removed_repository_namespace_cannot_be_reassigned() {
        let temp = tempfile::tempdir().unwrap();
        let a = temp.path().join("api");
        let b = temp.path().join("web");
        git_repo(&a);
        git_repo(&b);
        let mut config = EnsembleConfig::for_repositories(&[a, b.clone()], "Test".into()).unwrap();
        config.state_root = temp.path().join("state");
        drop(config.claim_state().unwrap());
        let api = RepoId::try_from("api".to_owned()).unwrap();
        config.repositories.remove(&api);
        config.peers.clear();
        drop(config.claim_state().unwrap());
        config.repositories.clear();
        config.repositories.insert(
            api,
            RepositoryConfig {
                path: b,
                description: None,
            },
        );
        assert!(
            config
                .claim_state()
                .unwrap_err()
                .to_string()
                .contains("already owns conversations")
        );
    }

    #[test]
    fn unowned_or_corrupt_state_is_not_adopted() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        git_repo(&repo);
        let mut config = EnsembleConfig::for_repositories(&[repo], "Test".into()).unwrap();
        config.state_root = temp.path().join("state");
        fs::create_dir(&config.state_root).unwrap();
        fs::write(config.state_root.join("mob.sqlite3"), "existing state").unwrap();
        assert!(config.claim_state().is_err());
        fs::write(config.state_root.join("ensemble-state.json"), "corrupt").unwrap();
        assert!(config.claim_state().is_err());
    }
}
