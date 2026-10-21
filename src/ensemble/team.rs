//! Durable, repository-grounded specialist assignments.
//!
//! A saved team is a historical discovery result. Native sessions retain the
//! identities declared here even as the repository's files evolve.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const REPRESENTATIVE: &str = "representative";
// Structural resource limits, not a requested or preferred team size.
pub const MIN_SPECIALISTS: usize = 1;
pub const MAX_SPECIALISTS: usize = 32;
const MAX_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_INSPECTED_FILES: usize = 256;

/// Stable native identity component; distinct from a specialist's editable label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MemberId(String);

impl MemberId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MemberId {
    type Error = String;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        let slug = (2..=48).contains(&value.len())
            && value.split('-').all(|part| {
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            });
        if !slug {
            return Err(format!(
                "invalid specialist ID {value:?}: use a 2–48 character lowercase kebab-case slug"
            ));
        }
        if matches!(
            value.as_str(),
            REPRESENTATIVE
                | "ingress"
                | "thread"
                | "thread-agent"
                | "user"
                | "assistant"
                | "system"
        ) {
            return Err(format!("specialist ID {value:?} is reserved"));
        }
        Ok(Self(value))
    }
}

impl From<MemberId> for String {
    fn from(value: MemberId) -> Self {
        value.0
    }
}

impl AsRef<str> for MemberId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    ReadOnly,
    Code,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecialistSpec {
    pub label: String,
    pub focus: String,
    pub charter: String,
    /// Exact inspected repository-relative files or directories, not glob patterns.
    /// These describe responsibility; the tool capability controls mutation.
    pub ownership_paths: Vec<PathBuf>,
    pub capability: Capability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectedFile {
    pub path: PathBuf,
    /// SHA-256 of the exact UTF-8 sample returned by the first successful read.
    /// It is historical evidence, not a hash of the entire current file.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryProvenance {
    pub model: String,
    pub discovered_at: DateTime<Utc>,
    pub inspected_files: Vec<InspectedFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamManifest {
    pub version: u32,
    #[serde(deserialize_with = "deserialize_specialists")]
    pub specialists: BTreeMap<MemberId, SpecialistSpec>,
    pub discovery: DiscoveryProvenance,
}

pub(super) fn deserialize_specialists<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<MemberId, SpecialistSpec>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct SpecialistVisitor;
    impl<'de> serde::de::Visitor<'de> for SpecialistVisitor {
        type Value = BTreeMap<MemberId, SpecialistSpec>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map of unique specialist IDs")
        }

        fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut specialists = BTreeMap::new();
            while let Some((id, spec)) = access.next_entry::<MemberId, SpecialistSpec>()? {
                match specialists.entry(id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(spec);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate specialist ID {}",
                            entry.key()
                        )));
                    }
                }
            }
            Ok(specialists)
        }
    }
    deserializer.deserialize_map(SpecialistVisitor)
}

impl TeamManifest {
    /// Validate a newly discovered team against the actual checkout and read evidence.
    pub fn validate(&self, repository: &Path) -> Result<()> {
        self.validate_inner(repository, true)
    }

    /// Load an existing team without re-running discovery.
    ///
    /// Deleted ownership/evidence paths remain valid historical assignments.
    /// Existing paths must still resolve inside this checkout.
    pub fn load(path: &Path, repository: &Path) -> Result<Option<Self>> {
        let file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("open saved specialist team"),
        };
        let mut bytes = Vec::new();
        file.take(MAX_MANIFEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("read saved specialist team")?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            bail!("specialist team exceeds the {MAX_MANIFEST_BYTES}-byte limit");
        }
        let team: Self = serde_json::from_slice(&bytes).context("decode saved specialist team")?;
        team.validate_inner(repository, false)
            .context("validate saved specialist team")?;
        Ok(Some(team))
    }

    /// Atomically create a specialist assignment; never replace an existing team.
    ///
    /// Hold the Ensemble state lease while creating teams. Explicit future team
    /// revisions must preserve native identities and use a separate update path.
    pub fn save(&self, path: &Path, repository: &Path) -> Result<()> {
        self.validate(repository)?;
        let bytes = serde_json::to_vec_pretty(self).context("encode specialist team")?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            bail!("specialist team exceeds the {MAX_MANIFEST_BYTES}-byte limit");
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).context("create specialist team directory")?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock precedes the Unix epoch")?
            .as_nanos();
        let temporary = parent.join(format!(".team-{}-{nonce}.tmp", std::process::id()));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .context("create temporary specialist team")?;
        let write_result = (|| -> Result<()> {
            file.write_all(&bytes).context("write specialist team")?;
            file.sync_all().context("flush specialist team")?;
            drop(file);
            // A same-directory hard link publishes the complete file atomically
            // while refusing to replace a target created by another writer.
            fs::hard_link(&temporary, path).with_context(|| {
                format!(
                    "publish {}; existing teams are never overwritten",
                    path.display()
                )
            })?;
            Ok(())
        })();
        let cleanup = fs::remove_file(&temporary);
        if let Err(error) = write_result {
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("temporary team cleanup failed: {cleanup}")))
                }
            };
        }
        cleanup.context("remove temporary specialist team")?;
        #[cfg(unix)]
        fs::File::open(parent)?
            .sync_all()
            .context("flush specialist team directory")?;
        Ok(())
    }

    fn validate_inner(&self, repository: &Path, require_exists: bool) -> Result<()> {
        if self.version != 1 {
            bail!("unsupported specialist team version {}", self.version);
        }
        if !(MIN_SPECIALISTS..=MAX_SPECIALISTS).contains(&self.specialists.len()) {
            bail!(
                "a team must contain at least one specialist and cannot exceed the safety limit of {MAX_SPECIALISTS}"
            );
        }
        bounded_text("discovery model", &self.discovery.model, 1, 200)?;
        if self.discovery.inspected_files.is_empty()
            || self.discovery.inspected_files.len() > MAX_INSPECTED_FILES
        {
            bail!("discovery requires 1–{MAX_INSPECTED_FILES} inspected files");
        }
        let root = repository
            .canonicalize()
            .context("resolve specialist checkout")?;
        let mut evidence = BTreeSet::new();
        for file in &self.discovery.inspected_files {
            let path = checked_path(&root, &file.path, require_exists)?;
            if require_exists && !path.is_file() {
                bail!("inspected evidence is not a file: {}", file.path.display());
            }
            if !evidence.insert(path) {
                bail!("duplicate inspected evidence path {}", file.path.display());
            }
            if file.sha256.len() != 64 || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                bail!("invalid SHA-256 sample digest for {}", file.path.display());
            }
        }
        let mut labels = BTreeSet::new();
        let mut has_code = false;
        for (id, specialist) in &self.specialists {
            bounded_text("specialist label", &specialist.label, 2, 48)?;
            bounded_text("specialist focus", &specialist.focus, 3, 180)?;
            bounded_text("specialist charter", &specialist.charter, 20, 800)?;
            if !labels.insert(specialist.label.trim().to_lowercase()) {
                bail!("duplicate specialist label {:?}", specialist.label);
            }
            if !(1..=6).contains(&specialist.ownership_paths.len()) {
                bail!("specialist {id} requires 1–6 inspected ownership paths");
            }
            has_code |= specialist.capability == Capability::Code;
            let mut scopes = BTreeSet::new();
            for relative in &specialist.ownership_paths {
                let path = checked_path(&root, relative, require_exists)?;
                if !scopes.insert(path.clone()) {
                    bail!(
                        "specialist {id} repeats ownership path {}",
                        relative.display()
                    );
                }
                if !evidence
                    .iter()
                    .any(|inspected| inspected.starts_with(&path))
                {
                    bail!(
                        "specialist {id} ownership path {} has no inspected file evidence",
                        relative.display()
                    );
                }
            }
        }
        if !has_code {
            bail!("a development team requires at least one code-capable specialist");
        }
        Ok(())
    }
}

fn bounded_text(field: &str, value: &str, min: usize, max: usize) -> Result<()> {
    if value.trim() != value || !(min..=max).contains(&value.chars().count()) {
        bail!("{field} must contain {min}–{max} characters with no surrounding whitespace");
    }
    if value
        .chars()
        .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    {
        bail!("{field} contains unsupported control characters");
    }
    Ok(())
}

fn checked_path(root: &Path, relative: &Path, require_exists: bool) -> Result<PathBuf> {
    let Some(text) = relative.to_str() else {
        bail!("specialist paths must be UTF-8");
    };
    if text.is_empty()
        || text.chars().count() > 120
        || text.contains(['*', '?', '[', ']', '\\', '\0'])
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("use an exact repository-relative specialist path: {text:?}");
    }
    let candidate = root.join(relative);
    match candidate.canonicalize() {
        Ok(path) => {
            if !path.starts_with(root) {
                bail!("specialist path escapes the repository: {text:?}");
            }
            if !path.is_file() && !path.is_dir() {
                bail!("specialist path is not an ordinary file or directory: {text:?}");
            }
            Ok(path)
        }
        Err(error) if !require_exists && error.kind() == std::io::ErrorKind::NotFound => {
            // Preserve historical paths, but reject a newly redirected existing
            // ancestor (for example a directory replaced by an external symlink).
            for ancestor in candidate.ancestors().skip(1) {
                match ancestor.canonicalize() {
                    Ok(path) => {
                        if !path.starts_with(root) {
                            bail!("specialist path ancestor escapes the repository: {text:?}");
                        }
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error).context("resolve specialist path ancestor"),
                }
            }
            Ok(candidate)
        }
        Err(error) => Err(error).with_context(|| format!("resolve specialist path {text:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(root: &Path) -> TeamManifest {
        let mut specialists = BTreeMap::new();
        let mut inspected_files = Vec::new();
        for (id, label, file, capability) in [
            (
                "account-domain",
                "Account domain",
                "account.rs",
                Capability::Code,
            ),
            (
                "browser-contracts",
                "Browser contracts",
                "browser.ts",
                Capability::Code,
            ),
            (
                "storage-integrity",
                "Storage integrity",
                "schema.sql",
                Capability::ReadOnly,
            ),
        ] {
            fs::write(root.join(file), format!("Source for {label}")).unwrap();
            specialists.insert(
                MemberId::try_from(id.to_string()).unwrap(),
                SpecialistSpec {
                    label: label.into(),
                    focus: format!("Understand and maintain {label}"),
                    charter: format!("Own repository expertise for {label}. Reinspect files and validate changes before reporting."),
                    ownership_paths: vec![file.into()],
                    capability,
                },
            );
            inspected_files.push(InspectedFile {
                path: file.into(),
                sha256: "a".repeat(64),
            });
        }
        TeamManifest {
            version: 1,
            specialists,
            discovery: DiscoveryProvenance {
                model: "test-model".into(),
                discovered_at: Utc::now(),
                inspected_files,
            },
        }
    }

    #[test]
    fn dynamic_team_roundtrips_and_preserves_history_when_files_are_deleted() {
        let repo = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let team = fixture(repo.path());
        let file = state.path().join("team.json");
        assert!(TeamManifest::load(&file, repo.path()).unwrap().is_none());
        team.save(&file, repo.path()).unwrap();
        assert_eq!(
            TeamManifest::load(&file, repo.path()).unwrap(),
            Some(team.clone())
        );
        fs::remove_file(repo.path().join("schema.sql")).unwrap();
        assert!(team.validate(repo.path()).is_err());
        assert_eq!(TeamManifest::load(&file, repo.path()).unwrap(), Some(team));
    }

    #[test]
    fn saved_teams_are_not_silently_replaced() {
        let repo = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let team = fixture(repo.path());
        let file = state.path().join("team.json");
        team.save(&file, repo.path()).unwrap();
        let mut replacement = team.clone();
        replacement.discovery.model = "another-model".into();
        assert!(replacement.save(&file, repo.path()).is_err());
        assert_eq!(TeamManifest::load(&file, repo.path()).unwrap(), Some(team));
        assert_eq!(fs::read_dir(state.path()).unwrap().count(), 1);
    }

    #[test]
    fn rejects_uninspected_scope_bad_capabilities_and_generic_reserved_identity() {
        let repo = tempfile::tempdir().unwrap();
        let mut team = fixture(repo.path());
        fs::write(repo.path().join("unread.rs"), "not inspected").unwrap();
        team.specialists
            .values_mut()
            .next()
            .unwrap()
            .ownership_paths = vec!["unread.rs".into()];
        assert!(team.validate(repo.path()).is_err());
        team = fixture(repo.path());
        for specialist in team.specialists.values_mut() {
            specialist.capability = Capability::ReadOnly;
        }
        assert!(team.validate(repo.path()).is_err());
        for id in [
            "representative",
            "system",
            "bad--slug",
            "UPPER",
            "a",
            "../other",
            "space name",
        ] {
            assert!(MemberId::try_from(id.to_string()).is_err(), "{id}");
        }
    }

    #[test]
    fn rejects_duplicate_ids_labels_unknown_fields_and_missing_domains() {
        let repo = tempfile::tempdir().unwrap();
        let mut team = fixture(repo.path());
        team.specialists.clear();
        assert!(team.validate(repo.path()).is_err());
        team = fixture(repo.path());
        for specialist in team.specialists.values_mut() {
            specialist.label = "Duplicate".into();
        }
        assert!(team.validate(repo.path()).is_err());
        let spec = serde_json::to_string(team.specialists.values().next().unwrap()).unwrap();
        let duplicate = format!(
            r#"{{"version":1,"specialists":{{"account-domain":{spec},"account-domain":{spec}}},"discovery":{}}}"#,
            serde_json::to_string(&team.discovery).unwrap()
        );
        assert!(serde_json::from_str::<TeamManifest>(&duplicate).is_err());
        assert!(serde_json::from_str::<SpecialistSpec>(r#"{"extra":true}"#).is_err());
    }

    #[test]
    fn session_capacity_includes_every_specialist_and_the_representative() {
        let repo = tempfile::tempdir().unwrap();
        let team = fixture(repo.path());
        let mut config: super::super::config::EnsembleConfig =
            serde_yaml::from_str("repositories: {}\nmax_sessions: 3").unwrap();
        let id = super::super::config::RepoId::try_from("product".to_owned()).unwrap();
        assert!(config.validate_team_capacity(&id, &team).is_err());
        config.max_sessions = 4;
        assert!(config.validate_team_capacity(&id, &team).is_ok());
    }

    #[test]
    fn team_size_follows_repository_needs_with_a_structural_safety_limit() {
        let repo = tempfile::tempdir().unwrap();
        let mut team = fixture(repo.path());
        team.specialists
            .retain(|id, _| id.as_str() == "account-domain");
        assert!(team.validate(repo.path()).is_ok());
        let prototype = team.specialists.values().next().unwrap().clone();
        team.specialists.clear();
        for index in 0..MAX_SPECIALISTS {
            let mut specialist = prototype.clone();
            specialist.label = format!("Repository domain {index}");
            team.specialists.insert(
                MemberId::try_from(format!("domain-{index}")).unwrap(),
                specialist,
            );
        }
        assert!(team.validate(repo.path()).is_ok());
        let mut overflow = prototype;
        overflow.label = "Overflow domain".to_owned();
        team.specialists.insert(
            MemberId::try_from("overflow-domain".to_owned()).unwrap(),
            overflow,
        );
        assert!(
            team.validate(repo.path())
                .unwrap_err()
                .to_string()
                .contains("safety limit")
        );
    }

    #[test]
    fn corrupt_or_oversized_saved_manifests_fail_without_rediscovery() {
        let repo = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("team.json");
        fs::write(&file, b"not json").unwrap();
        assert!(TeamManifest::load(&file, repo.path()).is_err());
        fs::write(&file, vec![b' '; MAX_MANIFEST_BYTES as usize + 1]).unwrap();
        assert!(TeamManifest::load(&file, repo.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_absolute_parent_glob_and_redirected_historical_paths() {
        let repo = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = repo.path().canonicalize().unwrap();
        for path in ["/etc/passwd", "../escape", "src/*.rs", "src\\file"] {
            assert!(checked_path(&root, Path::new(path), false).is_err());
        }
        std::os::unix::fs::symlink(outside.path(), repo.path().join("redirect")).unwrap();
        assert!(checked_path(&root, Path::new("redirect/missing.rs"), false).is_err());
    }
}
