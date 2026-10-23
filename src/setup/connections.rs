use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const SERVICE_CREDENTIAL_ENV: &[&str] = &[
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
    "GH_HOST",
    "JIRA_API_TOKEN",
    "JIRA_TOKEN",
    "JIRA_EMAIL",
    "JIRA_BASE_URL",
    "LINEAR_API_KEY",
];

/// Serializable only for the private credential file and incoming requests.
/// Public API responses use the separate, secret-free connection projections.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Secret {
    pub(super) fn expose(&self) -> &str {
        &self.0
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.0.trim().is_empty()
            || self.0.len() > 16 * 1024
            || self.0.contains(['\r', '\n', '\0'])
        {
            bail!("Enter a non-empty credential without line breaks");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProvider {
    Openai,
    Anthropic,
    Gemini,
}

impl ModelProvider {
    pub fn key_environment(self) -> &'static str {
        match self {
            Self::Openai => "RKAT_OPENAI_API_KEY",
            Self::Anthropic => "RKAT_ANTHROPIC_API_KEY",
            Self::Gemini => "RKAT_GEMINI_API_KEY",
        }
    }

    pub fn environment_names(self) -> &'static [&'static str] {
        match self {
            Self::Openai => &["RKAT_OPENAI_API_KEY", "OPENAI_API_KEY"],
            Self::Anthropic => &["RKAT_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"],
            Self::Gemini => &[
                "RKAT_GEMINI_API_KEY",
                "GEMINI_API_KEY",
                "RKAT_GOOGLE_API_KEY",
                "GOOGLE_API_KEY",
            ],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelCredential {
    Environment,
    ApiKey {
        #[serde(default)]
        key: Secret,
    },
    /// Meerkat owns the binding, persisted OAuth credential, and refresh.
    Binding {
        auth_binding: meerkat_core::AuthBindingRef,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConnection {
    pub provider: ModelProvider,
    pub model: String,
    pub credential: ModelCredential,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum GitHubCredential {
    GhCli,
    Token {
        #[serde(default)]
        token: Secret,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubConnection {
    pub host: String,
    pub credential: GitHubCredential,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrackerConnection {
    Local,
    Linear {
        #[serde(default)]
        api_key: Secret,
    },
    Jira {
        site: String,
        email: String,
        #[serde(default)]
        api_token: Secret,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connections {
    pub model: Option<ModelConnection>,
    pub github: Option<GitHubConnection>,
    pub tracker: Option<TrackerConnection>,
}

#[derive(Serialize)]
pub struct ConnectionStatus {
    pub model: Option<ModelStatus>,
    pub github: Option<GitHubStatus>,
    pub tracker: Option<TrackerStatus>,
    pub environment_providers: Vec<ModelProvider>,
}

#[derive(Serialize)]
pub struct ModelStatus {
    pub provider: ModelProvider,
    pub model: String,
    pub method: &'static str,
    pub auth_binding: Option<meerkat_core::AuthBindingRef>,
}

#[derive(Serialize)]
pub struct GitHubStatus {
    pub host: String,
    pub method: &'static str,
}

#[derive(Serialize)]
pub struct TrackerStatus {
    pub kind: &'static str,
    pub site: Option<String>,
    pub email: Option<String>,
}

impl Connections {
    /// Explicit subprocess credentials. Never change the host process environment.
    pub(crate) fn github_environment(&self) -> BTreeMap<String, String> {
        let mut environment = SERVICE_CREDENTIAL_ENV
            .iter()
            .map(|name| (name.to_string(), String::new()))
            .collect::<BTreeMap<_, _>>();
        if let Some(github) = &self.github {
            environment.insert("GH_HOST".into(), github.host.clone());
            // A PAT environment variable authenticates gh, not Git itself.
            // Scope the helper to this subprocess and host so later fetches and
            // specialist shell commands work without changing a user's checkout
            // or consulting unrelated global credential helpers.
            environment.extend([
                ("GIT_CONFIG_COUNT".into(), "2".into()),
                ("GIT_CONFIG_KEY_0".into(), "credential.helper".into()),
                ("GIT_CONFIG_VALUE_0".into(), String::new()),
                (
                    "GIT_CONFIG_KEY_1".into(),
                    format!("credential.https://{}.helper", github.host),
                ),
                (
                    "GIT_CONFIG_VALUE_1".into(),
                    "!gh auth git-credential".into(),
                ),
            ]);
            if let GitHubCredential::Token { token } = &github.credential {
                environment.insert(
                    github_token_environment(&github.host).into(),
                    token.expose().to_owned(),
                );
            }
        }
        environment
    }

    pub(crate) fn model_binding(
        &self,
        scope: &Path,
    ) -> Result<Option<meerkat_core::AuthBindingRef>> {
        use sha2::{Digest, Sha256};
        let Some(ModelConnection {
            provider,
            credential: ModelCredential::ApiKey { .. },
            ..
        }) = &self.model
        else {
            return Ok(None);
        };
        let provider = match provider {
            ModelProvider::Openai => "openai",
            ModelProvider::Anthropic => "anthropic",
            ModelProvider::Gemini => "gemini",
        };
        Ok(Some(meerkat_core::AuthBindingRef {
            realm: meerkat_core::RealmId::parse(format!(
                "fugue-{:x}",
                Sha256::digest(scope.as_os_str().as_encoded_bytes())
            ))?,
            binding: meerkat_core::BindingId::parse(format!("default_{provider}"))?,
            profile: None,
            origin: meerkat_core::BindingOrigin::Configured,
        }))
    }

    pub(crate) fn apply_model_config(
        &self,
        config: &mut meerkat::Config,
        scope: &Path,
    ) -> Result<()> {
        if let Some(binding) = self.model_binding(scope)?
            && let Some(ModelConnection {
                provider,
                credential: ModelCredential::ApiKey { key },
                ..
            }) = &self.model
        {
            let provider = match provider {
                ModelProvider::Openai => "openai",
                ModelProvider::Anthropic => "anthropic",
                ModelProvider::Gemini => "gemini",
            };
            config.realm.insert(
                binding.realm.to_string(),
                meerkat_core::RealmConfigSection::from_inline_api_keys(&[(provider, key.expose())]),
            );
        }
        Ok(())
    }
    pub(super) fn jira_client(&self, project: &str) -> Result<crate::jira::JiraClient> {
        let Some(TrackerConnection::Jira {
            site,
            email,
            api_token,
        }) = &self.tracker
        else {
            bail!("Save Jira access first");
        };
        if project.trim().is_empty() {
            bail!("Enter a Jira project key first");
        }
        crate::jira::JiraClient::from_config(&crate::config::TrackerConfig {
            kind: "jira".into(),
            endpoint: site.clone(),
            api_key: Some(api_token.expose().into()),
            jira: Some(crate::jira::JiraConfig {
                email: Some(email.clone()),
                ..Default::default()
            }),
            project_slug: Some(project.trim().into()),
            intake_states: vec![],
            in_flight_states: vec![],
            active_states: vec![],
            terminal_states: vec![],
            attachment_root: None,
            max_attachment_files: 0,
            max_attachment_bytes: 0,
            max_attachment_total_bytes: 0,
        })
        .map_err(Into::into)
    }
    /// An omitted credential means keep the saved value for exactly the same
    /// provider/account. It must never copy credentials to a different host.
    pub fn retain_missing_credentials(&mut self, previous: &Self) {
        if let (Some(next), Some(old)) = (&mut self.model, &previous.model)
            && next.provider == old.provider
            && let (ModelCredential::ApiKey { key }, ModelCredential::ApiKey { key: saved }) =
                (&mut next.credential, &old.credential)
            && key.0.is_empty()
        {
            *key = saved.clone();
        }
        if let (Some(next), Some(old)) = (&mut self.github, &previous.github)
            && next.host.eq_ignore_ascii_case(&old.host)
            && let (GitHubCredential::Token { token }, GitHubCredential::Token { token: saved }) =
                (&mut next.credential, &old.credential)
            && token.0.is_empty()
        {
            *token = saved.clone();
        }
        match (&mut self.tracker, &previous.tracker) {
            (
                Some(TrackerConnection::Linear { api_key }),
                Some(TrackerConnection::Linear { api_key: saved }),
            ) if api_key.0.is_empty() => *api_key = saved.clone(),
            (
                Some(TrackerConnection::Jira {
                    site,
                    email,
                    api_token,
                }),
                Some(TrackerConnection::Jira {
                    site: old_site,
                    email: old_email,
                    api_token: saved,
                }),
            ) if site == old_site && email == old_email && api_token.0.is_empty() => {
                *api_token = saved.clone()
            }
            _ => {}
        }
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(model) = &self.model {
            if model.model.trim().is_empty()
                || model.model.len() > 256
                || model.model.contains(['\n', '\r', '\0'])
            {
                bail!("Enter a model name");
            }
            if let ModelCredential::ApiKey { key } = &model.credential {
                key.validate()?;
            }
        }
        if let Some(github) = &self.github {
            validate_github_host(&github.host)?;
            if let GitHubCredential::Token { token } = &github.credential {
                token.validate()?;
            }
        }
        match &self.tracker {
            Some(TrackerConnection::Linear { api_key }) => api_key.validate()?,
            Some(TrackerConnection::Jira {
                site,
                email,
                api_token,
            }) => {
                validate_jira_site(site)?;
                if !email.contains('@') || email.contains(['\r', '\n', '\0']) {
                    bail!("Enter the Jira account email");
                }
                api_token.validate()?;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn status(&self) -> ConnectionStatus {
        ConnectionStatus {
            model: self.model.as_ref().map(|model| ModelStatus {
                provider: model.provider,
                model: model.model.clone(),
                method: match model.credential {
                    ModelCredential::Environment => "environment",
                    ModelCredential::ApiKey { .. } => "api_key",
                    ModelCredential::Binding { .. } => "binding",
                },
                auth_binding: match &model.credential {
                    ModelCredential::Binding { auth_binding } => Some(auth_binding.clone()),
                    _ => None,
                },
            }),
            github: self.github.as_ref().map(|github| GitHubStatus {
                host: github.host.clone(),
                method: match github.credential {
                    GitHubCredential::GhCli => "gh_cli",
                    GitHubCredential::Token { .. } => "token",
                },
            }),
            tracker: self.tracker.as_ref().map(|tracker| match tracker {
                TrackerConnection::Local => TrackerStatus {
                    kind: "local",
                    site: None,
                    email: None,
                },
                TrackerConnection::Linear { .. } => TrackerStatus {
                    kind: "linear",
                    site: None,
                    email: None,
                },
                TrackerConnection::Jira { site, email, .. } => TrackerStatus {
                    kind: "jira",
                    site: Some(site.clone()),
                    email: Some(email.clone()),
                },
            }),
            environment_providers: [
                ModelProvider::Openai,
                ModelProvider::Anthropic,
                ModelProvider::Gemini,
            ]
            .into_iter()
            .filter(|provider| {
                provider
                    .environment_names()
                    .iter()
                    .any(|name| std::env::var(name).is_ok_and(|value| !value.trim().is_empty()))
            })
            .collect(),
        }
    }

    /// Apply to a child process, never mutate this multi-threaded process's env.
    pub fn environment(&self) -> BTreeMap<&'static str, String> {
        let mut env = BTreeMap::new();
        if let Some(ModelConnection {
            provider,
            credential: ModelCredential::ApiKey { key },
            ..
        }) = &self.model
        {
            env.insert(provider.key_environment(), key.expose().to_owned());
        }
        if let Some(GitHubConnection {
            host,
            credential: GitHubCredential::Token { token },
        }) = &self.github
        {
            env.insert(github_token_environment(host), token.expose().to_owned());
        }
        match &self.tracker {
            Some(TrackerConnection::Linear { api_key }) => {
                env.insert("LINEAR_API_KEY", api_key.expose().to_owned());
            }
            Some(TrackerConnection::Jira {
                email, api_token, ..
            }) => {
                env.insert("JIRA_EMAIL", email.clone());
                env.insert("JIRA_API_TOKEN", api_token.expose().to_owned());
            }
            _ => {}
        }
        env
    }
}

fn github_token_environment(host: &str) -> &'static str {
    if host == "github.com" || host.ends_with(".ghe.com") {
        "GH_TOKEN"
    } else {
        "GH_ENTERPRISE_TOKEN"
    }
}

pub fn validate_github_host(host: &str) -> Result<()> {
    if host.starts_with('-') || host.len() > 259 {
        bail!("Enter a GitHub hostname without command arguments");
    }
    crate::tools::GitHubHost::try_from(host.to_string())
        .map(|_| ())
        .map_err(anyhow::Error::msg)
}

fn validate_jira_site(site: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(site).context("Enter the HTTPS URL of your Jira site")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("Jira must use an HTTPS URL without embedded credentials, query, or fragment");
    }
    Ok(url)
}

/// An explicit private file, separate from portable workflow and repository files.
pub struct ConnectionStore {
    directory: PathBuf,
    _lock: std::fs::File,
}

impl ConnectionStore {
    pub fn open(root: &Path) -> Result<Self> {
        reject_repository_storage(root)?;
        let directory = root.join("private");
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        reject_repository_storage(&directory)?;
        check_private_permissions(&directory)?;
        let lock = private_options().open(directory.join("connections.lock"))?;
        lock.try_lock()
            .context("another setup process owns these connections")?;
        Ok(Self {
            directory,
            _lock: lock,
        })
    }

    pub fn load(&self) -> Result<Connections> {
        let path = self.directory.join("connections.json");
        if !path.exists() {
            return Ok(Connections::default());
        }
        check_private_permissions(&path)?;
        let bytes = std::fs::read(path).context("read private connections")?;
        let connections: Connections = serde_json::from_slice(&bytes).map_err(|_| {
            anyhow::anyhow!(
                "Private connections file is invalid; restore it or configure connections again"
            )
        })?;
        connections.validate()?;
        Ok(connections)
    }

    pub fn save(&self, connections: &Connections) -> Result<()> {
        connections.validate()?;
        super::save_json(&self.directory.join("connections.json"), connections)
    }
}

fn reject_repository_storage(path: &Path) -> Result<()> {
    let absolute = std::path::absolute(path)?;
    let existing = absolute
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .context("Connection storage has no existing parent")?
        .canonicalize()?;
    if existing
        .ancestors()
        .any(|ancestor| ancestor.join(".git").exists())
    {
        bail!(
            "Choose an application state directory outside Git repositories; credentials must not become repository content"
        );
    }
    Ok(())
}

fn private_options() -> std::fs::OpenOptions {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn check_private_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            bail!(
                "Private connection storage is accessible to other users; restrict its permissions before continuing"
            );
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct GitHubAccount {
    pub login: String,
}

impl GitHubConnection {
    /// Read-only check through the same gh credential resolution used by tools.
    pub async fn check(&self) -> Result<GitHubAccount> {
        validate_github_host(&self.host)?;
        let mut command = tokio::process::Command::new("gh");
        for name in SERVICE_CREDENTIAL_ENV {
            command.env_remove(name);
        }
        command
            .args(["api", "--hostname", &self.host, "user"])
            .env("GH_PROMPT_DISABLED", "1")
            .env("GH_HOST", &self.host)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        if let GitHubCredential::Token { token } = &self.credential {
            token.validate()?;
            command.env(github_token_environment(&self.host), token.expose());
        }
        let output = tokio::time::timeout(Duration::from_secs(20), command.output())
            .await
            .context("GitHub connection check timed out")?
            .context("GitHub CLI is unavailable; install gh and try again")?;
        if !output.status.success() {
            // gh stderr may contain host response data. Never reflect it into UI.
            bail!(
                "GitHub sign-in failed for this host. Check the token or sign in with gh auth login --hostname {}",
                self.host
            );
        }
        serde_json::from_slice(&output.stdout)
            .context("GitHub returned an invalid account response")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connections() -> Connections {
        Connections {
            model: Some(ModelConnection {
                provider: ModelProvider::Openai,
                model: "example-model".into(),
                credential: ModelCredential::ApiKey {
                    key: Secret("model-secret-sentinel".into()),
                },
            }),
            github: Some(GitHubConnection {
                host: "git.example.com".into(),
                credential: GitHubCredential::Token {
                    token: Secret("github-secret-sentinel".into()),
                },
            }),
            tracker: Some(TrackerConnection::Jira {
                site: "https://jira.example.com".into(),
                email: "test@example.com".into(),
                api_token: Secret("jira-secret-sentinel".into()),
            }),
        }
    }

    #[test]
    fn repository_credentials_are_scoped_without_process_environment_changes() {
        let first = connections();
        let mut second = first.clone();
        second.github = Some(GitHubConnection {
            host: "github.com".into(),
            credential: GitHubCredential::GhCli,
        });
        let first_env = first.github_environment();
        let second_env = second.github_environment();
        assert_eq!(first_env["GH_ENTERPRISE_TOKEN"], "github-secret-sentinel");
        assert_eq!(second_env["GH_ENTERPRISE_TOKEN"], "");
        assert_eq!(second_env["GH_HOST"], "github.com");
        for env in [&first_env, &second_env] {
            assert_eq!(env["JIRA_API_TOKEN"], "");
            assert_eq!(env["JIRA_EMAIL"], "");
            assert!(!env.values().any(|value| value == "model-secret-sentinel"));
        }
        let first_binding = first
            .model_binding(Path::new("/private/repository-one"))
            .unwrap()
            .unwrap();
        let second_binding = second
            .model_binding(Path::new("/private/repository-two"))
            .unwrap()
            .unwrap();
        assert_ne!(first_binding.realm, second_binding.realm);
        let mut config = meerkat::Config::default();
        first
            .apply_model_config(&mut config, Path::new("/private/repository-one"))
            .unwrap();
        assert!(config.realm.contains_key(first_binding.realm.as_str()));
        assert!(!config.realm.contains_key(second_binding.realm.as_str()));
    }

    #[cfg(unix)]
    #[test]
    fn repository_git_uses_saved_credentials_without_checkout_or_global_helpers() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        use std::process::{Command, Stdio};

        let root = tempfile::tempdir().unwrap();
        let bin = root.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let helper = bin.join("gh");
        std::fs::write(
            &helper,
            "#!/bin/sh\n[ \"$1 $2 $3\" = 'auth git-credential get' ] || exit 1\nprintf 'username=repository-test\\npassword=%s\\n' \"${GH_ENTERPRISE_TOKEN:-cli-login-sentinel}\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let global = root.path().join("gitconfig");
        std::fs::write(
            &global,
            "[credential]\nhelper = !printf 'username=unrelated\\npassword=unrelated-secret\\n'\n",
        )
        .unwrap();
        let mut paths = vec![bin];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let path = std::env::join_paths(paths).unwrap();
        let lookup = |connections: &Connections, host: &str| {
            let mut child = Command::new("git")
                .args(["credential", "fill"])
                .current_dir(root.path())
                .env("HOME", root.path())
                .env("GIT_CONFIG_GLOBAL", &global)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("PATH", &path)
                .envs(connections.github_environment())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            writeln!(child.stdin.take().unwrap(), "protocol=https\nhost={host}\n").unwrap();
            child.wait_with_output().unwrap()
        };

        let connection = connections();
        let saved = lookup(&connection, "git.example.com");
        assert!(saved.status.success());
        let saved = String::from_utf8(saved.stdout).unwrap();
        assert!(saved.contains("password=github-secret-sentinel"), "{saved}");
        assert!(!saved.contains("unrelated-secret"));
        assert!(
            !lookup(&connection, "unrelated.example.com")
                .status
                .success()
        );

        let mut cli = connection.clone();
        cli.github.as_mut().unwrap().credential = GitHubCredential::GhCli;
        let cli = lookup(&cli, "git.example.com");
        assert!(cli.status.success());
        assert!(
            String::from_utf8(cli.stdout)
                .unwrap()
                .contains("password=cli-login-sentinel")
        );
    }

    #[test]
    fn public_status_and_debug_never_expose_secrets() {
        let connections = connections();
        let status = serde_json::to_string(&connections.status()).unwrap();
        let debug = format!("{connections:?}");
        for secret in [
            "model-secret-sentinel",
            "github-secret-sentinel",
            "jira-secret-sentinel",
        ] {
            assert!(!status.contains(secret));
            assert!(!debug.contains(secret));
        }
        assert!(status.contains("api_key"));
        assert!(status.contains("git.example.com"));
    }

    #[test]
    fn omitted_credentials_are_kept_only_for_the_same_destination() {
        let previous = connections();
        let mut changed = previous.clone();
        changed.model.as_mut().unwrap().credential = ModelCredential::ApiKey {
            key: Secret::default(),
        };
        changed.github.as_mut().unwrap().credential = GitHubCredential::Token {
            token: Secret::default(),
        };
        changed.retain_missing_credentials(&previous);
        changed.validate().unwrap();
        assert_eq!(changed.environment(), previous.environment());

        changed.github.as_mut().unwrap().host = "other.example.com".into();
        changed.github.as_mut().unwrap().credential = GitHubCredential::Token {
            token: Secret::default(),
        };
        changed.retain_missing_credentials(&previous);
        assert!(changed.validate().is_err());
        changed = previous.clone();
        changed.model.as_mut().unwrap().provider = ModelProvider::Anthropic;
        changed.model.as_mut().unwrap().credential = ModelCredential::ApiKey {
            key: Secret::default(),
        };
        changed.retain_missing_credentials(&previous);
        assert!(changed.validate().is_err());
    }

    #[test]
    fn private_store_roundtrips_and_has_exclusive_ownership() {
        let root = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(root.path()).unwrap();
        assert!(ConnectionStore::open(root.path()).is_err());
        assert!(store.load().unwrap().model.is_none());
        store.save(&connections()).unwrap();
        assert_eq!(
            store.load().unwrap().environment()["GH_ENTERPRISE_TOKEN"],
            "github-secret-sentinel"
        );
        assert!(!root.path().join("fugue.yaml").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(root.path().join("private/connections.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn interrupted_credential_save_keeps_committed_values_and_allows_retry() {
        let root = tempfile::tempdir().unwrap();
        let previous = connections();
        {
            let store = ConnectionStore::open(root.path()).unwrap();
            store.save(&previous).unwrap();
            // A process died while writing the former fixed transaction file.
            std::fs::write(
                root.path().join("private/connections.pending"),
                b"{\"model\":",
            )
            .unwrap();
        }
        let reopened = ConnectionStore::open(root.path()).unwrap();
        assert_eq!(
            reopened.load().unwrap().environment(),
            previous.environment()
        );
        let updated = Connections::default();
        reopened.save(&updated).unwrap();
        assert!(reopened.load().unwrap().environment().is_empty());
    }

    #[test]
    fn endpoints_cannot_embed_credentials_or_command_arguments() {
        for bad in [
            "https://github.com",
            "--hostname",
            "github.com/repo",
            "user@github.com",
            "github.com\nsecret",
        ] {
            assert!(validate_github_host(bad).is_err());
        }
        for good in [
            "github.com",
            "git.example.com",
            "example.ghe.com",
            "git.example.com:443",
        ] {
            validate_github_host(good).unwrap();
        }
        for bad in [
            "http://jira.example.com",
            "https://token@jira.example.com",
            "https://jira.example.com/?token=secret",
        ] {
            assert!(validate_jira_site(bad).is_err());
        }
        validate_jira_site("https://jira.example.com/jira").unwrap();
    }

    #[test]
    fn refuses_credential_storage_inside_a_repository() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join(".git")).unwrap();
        assert!(ConnectionStore::open(&root.path().join("state")).is_err());
        assert!(!root.path().join("state/private/connections.json").exists());
    }

    #[test]
    fn credentials_are_scoped_to_the_intended_process_variables() {
        let connections = connections();
        let env = connections.environment();
        assert!(env.contains_key("RKAT_OPENAI_API_KEY"));
        assert!(env.contains_key("GH_ENTERPRISE_TOKEN"));
        assert!(!env.contains_key("GH_TOKEN"));
        assert_eq!(github_token_environment("github.com"), "GH_TOKEN");
        assert_eq!(github_token_environment("example.ghe.com"), "GH_TOKEN");
    }

    #[cfg(unix)]
    #[test]
    fn refuses_publicly_readable_credentials() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(root.path()).unwrap();
        store.save(&connections()).unwrap();
        std::fs::set_permissions(
            root.path().join("private/connections.json"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(store.load().is_err());
    }
}
