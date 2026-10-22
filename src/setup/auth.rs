//! Native rkat account discovery and login transport. Fugue never persists AI secrets.
use super::connections::{ModelProvider, Secret};
use anyhow::{Context, Result, bail};
use meerkat_core::{AuthBindingRef, BindingId, BindingOrigin, Config, Provider, RealmId};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, process::Stdio, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, sync::Mutex};

#[derive(Default, Deserialize, Serialize)]
struct HostConnections {
    #[serde(default)]
    realm: BTreeMap<String, meerkat_core::connection::RealmConfigSection>,
}

#[derive(Debug, Serialize)]
pub struct Account {
    pub auth_binding: AuthBindingRef,
    pub provider: ModelProvider,
    pub name: String,
    pub method: String,
}

pub async fn accounts() -> Result<Vec<Account>> {
    let path = Config::global_config_path().context("Cannot find rkat's user configuration")?;
    accounts_at(&path).await
}

async fn accounts_at(path: &Path) -> Result<Vec<Account>> {
    let host: HostConnections = match tokio::fs::read_to_string(path).await {
        Ok(contents) => toml::from_str(&contents)
            .map_err(|_| anyhow::anyhow!("Could not read rkat account configuration"))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => HostConnections::default(),
        Err(_) => bail!("Could not read rkat account configuration"),
    };
    let config = Config {
        realm: host.realm,
        ..Config::default()
    };
    let Some(global) = config.realm.get("global") else {
        return Ok(Vec::new());
    };
    let mut accounts = Vec::new();
    for (id, binding) in &global.binding {
        let backend = global
            .backend
            .get(&binding.backend_profile)
            .context("rkat binding has no backend")?;
        let provider = match backend.provider.as_str() {
            "openai" => ModelProvider::Openai,
            "anthropic" => ModelProvider::Anthropic,
            "gemini" => ModelProvider::Gemini,
            _ => continue,
        };
        let reference = AuthBindingRef {
            realm: RealmId::global(),
            binding: BindingId::parse(id)?,
            profile: None,
            origin: BindingOrigin::Configured,
        };
        // Use native resolution, including shared credential accounts and constraints.
        let target = meerkat_core::resolve_auth_binding_or_default_for_provider(
            &config,
            provider.native(),
            Some(&reference),
            Some(&RealmId::global()),
            false,
        )
        .map_err(|_| {
            anyhow::anyhow!(
                "An rkat account needs configuration repair. Run rkat auth bindings to inspect it."
            )
        })?;
        accounts.push(Account {
            auth_binding: target.auth_binding,
            provider,
            name: target.auth_profile.id,
            method: target.auth_profile.auth_method,
        });
    }
    Ok(accounts)
}

impl ModelProvider {
    pub fn name(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
            Self::Gemini => "gemini",
        }
    }
    fn native(self) -> Provider {
        match self {
            Self::Openai => Provider::OpenAI,
            Self::Anthropic => Provider::Anthropic,
            Self::Gemini => Provider::Gemini,
        }
    }

    fn api_key_route(self) -> (&'static str, &'static str) {
        use meerkat_core::provider_matrix::{
            AnthropicAuthMethod, AnthropicBackendKind, GoogleAuthMethod, GoogleBackendKind,
            OpenAiAuthMethod, OpenAiBackendKind,
        };
        match self {
            Self::Openai => (
                OpenAiBackendKind::OpenAiApi.as_str(),
                OpenAiAuthMethod::ApiKey.as_str(),
            ),
            Self::Anthropic => (
                AnthropicBackendKind::AnthropicApi.as_str(),
                AnthropicAuthMethod::ApiKey.as_str(),
            ),
            Self::Gemini => (
                GoogleBackendKind::GoogleGenAi.as_str(),
                GoogleAuthMethod::ApiKey.as_str(),
            ),
        }
    }
}

/// Non-interactive rkat login only stores the secret. Register the matching
/// non-secret route in its owning global document using Meerkat's config store.
/// Never replace an existing OAuth, custom backend, or shared credential route.
async fn ensure_api_key_binding(path: &Path, provider: ModelProvider) -> Result<()> {
    use meerkat_core::{
        AuthProfileConfig, BackendProfileConfig, ConfigDelta, ConfigStore, CredentialSourceSpec,
        FileConfigStore, ProviderBindingConfig, RealmConfigSection,
    };
    let id = format!("default_{}", provider.name());
    let (backend_kind, auth_method) = provider.api_key_route();
    let backend = BackendProfileConfig {
        provider: provider.name().into(),
        backend_kind: backend_kind.into(),
        base_url: None,
        options: serde_json::Value::Null,
        server: None,
    };
    let auth = AuthProfileConfig {
        provider: provider.name().into(),
        auth_method: auth_method.into(),
        source: CredentialSourceSpec::ManagedStore,
        constraints: Default::default(),
        metadata_defaults: Default::default(),
    };
    let binding = ProviderBindingConfig {
        backend_profile: id.clone(),
        auth_profile: id.clone(),
        credential_account: None,
        default_model: None,
        policy: Default::default(),
        provider_default: false,
    };
    let store = FileConfigStore::new(path.to_path_buf(), meerkat_models::canonical());
    let config = store
        .get()
        .await
        .map_err(|_| anyhow::anyhow!("Could not read rkat account configuration"))?;
    if let Some(global) = config.realm.get("global") {
        if global
            .backend
            .get(&id)
            .is_some_and(|value| value != &backend)
            || global.auth.get(&id).is_some_and(|value| value != &auth)
            || global
                .binding
                .get(&id)
                .is_some_and(|value| value != &binding)
        {
            bail!(
                "The default rkat account already uses another configuration. Select that account or configure a separate API-key account with rkat."
            );
        }
        if global.backend.contains_key(&id)
            && global.auth.contains_key(&id)
            && global.binding.contains_key(&id)
        {
            return Ok(());
        }
    }
    let section = RealmConfigSection {
        backend: BTreeMap::from([(id.clone(), backend)]),
        auth: BTreeMap::from([(id.clone(), auth)]),
        binding: BTreeMap::from([(id, binding)]),
        ..Default::default()
    };
    let patch = HostConnections {
        realm: BTreeMap::from([("global".into(), section)]),
    };
    store
        .patch(ConfigDelta(serde_json::to_value(patch)?))
        .await
        .map_err(|_| {
            anyhow::anyhow!("Could not register the API-key account in rkat configuration")
        })?;
    Ok(())
}

/// Secret travels through stdin only. The native CLI chooses storage and writes
/// its token store; nothing is copied to Fugue's connection file.
pub async fn provision_key(
    provider: ModelProvider,
    key: &Secret,
    directory: &Path,
) -> Result<AuthBindingRef> {
    key.validate()?;
    let path = Config::global_config_path().context("Cannot find rkat's user configuration")?;
    ensure_api_key_binding(&path, provider).await?;
    let mut child = tokio::process::Command::new("rkat")
        .args([
            "auth",
            "login",
            provider.name(),
            "--non-interactive",
            "--realm",
            "global",
        ])
        .current_dir(directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("Install rkat to connect AI accounts")?;
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let mut input = child
            .stdin
            .take()
            .context("Could not open rkat credential input")?;
        input.write_all(key.expose().as_bytes()).await?;
        input.shutdown().await?;
        drop(input);
        child.wait().await.map_err(anyhow::Error::from)
    })
    .await
    .context(
        "rkat did not finish saving the account; inspect rkat auth bindings before retrying",
    )??;
    if !result.success() {
        bail!(
            "rkat could not save this account. Check the installed rkat version and credential-store access."
        );
    }
    let binding = format!("default_{}", provider.name());
    accounts().await?.into_iter().find(|account| account.provider == provider && account.auth_binding.binding.as_str() == binding)
        .map(|account| account.auth_binding).context("rkat saved the credential but its account could not be loaded. Refresh the account list.")
}

#[derive(Clone, Default, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum LoginStatus {
    #[default]
    Idle,
    Waiting,
    Complete,
    Failed {
        error: &'static str,
    },
    Cancelled,
}

#[derive(Default)]
pub struct Login {
    status: Arc<Mutex<LoginStatus>>,
    operation: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Login {
    pub async fn status(&self) -> LoginStatus {
        self.status.lock().await.clone()
    }
    pub async fn start(&self, provider: ModelProvider, directory: &Path) -> Result<()> {
        let mut operation = self.operation.lock().await;
        if operation.as_ref().is_some_and(|task| !task.is_finished()) {
            bail!("An rkat sign-in is already waiting. Complete or cancel it first.");
        }
        let mut child = tokio::process::Command::new("rkat")
            .args(["auth", "login", provider.name(), "--realm", "global"])
            .current_dir(directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("Install rkat to sign in to AI accounts")?;
        *self.status.lock().await = LoginStatus::Waiting;
        let status = Arc::clone(&self.status);
        *operation = Some(tokio::spawn(async move {
            let result = tokio::time::timeout(Duration::from_secs(180), child.wait()).await;
            *status.lock().await = match result {
                Ok(Ok(result)) if result.success() => LoginStatus::Complete,
                _ => LoginStatus::Failed {
                    error: "rkat sign-in did not complete. Retry or run rkat auth login in a terminal on this machine.",
                },
            };
        }));
        Ok(())
    }
    pub async fn cancel(&self) -> Result<()> {
        if let Some(operation) = self.operation.lock().await.take() {
            operation.abort();
            if let Err(error) = operation.await
                && !error.is_cancelled()
            {
                return Err(error.into());
            }
        }
        *self.status.lock().await = LoginStatus::Cancelled;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn api_key_setup_creates_discoverable_native_bindings_and_preserves_other_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[agent]\nmodel = 'gpt-6-astra'\n").unwrap();
        for provider in [
            ModelProvider::Openai,
            ModelProvider::Anthropic,
            ModelProvider::Gemini,
        ] {
            ensure_api_key_binding(&path, provider).await.unwrap();
            ensure_api_key_binding(&path, provider).await.unwrap();
        }
        let accounts = accounts_at(&path).await.unwrap();
        assert_eq!(accounts.len(), 3);
        assert!(accounts.iter().all(|a| a.method == "api_key"));
        let text = std::fs::read_to_string(&path).unwrap();
        let (config, _) = Config::from_persisted_toml(&text).unwrap();
        assert_eq!(config.agent.model, "gpt-6-astra");
        assert!(
            config.realm["global"]
                .auth
                .values()
                .all(|a| a.source == meerkat_core::CredentialSourceSpec::ManagedStore)
        );
        assert!(config.realm["global"].default_binding.is_none());
    }

    #[tokio::test]
    async fn api_key_setup_refuses_to_replace_existing_oauth_account() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        ensure_api_key_binding(&path, ModelProvider::Openai)
            .await
            .unwrap();
        let original = std::fs::read_to_string(&path)
            .unwrap()
            .replace("api_key", "managed_chatgpt_oauth");
        std::fs::write(&path, &original).unwrap();
        assert!(
            ensure_api_key_binding(&path, ModelProvider::Openai)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    }

    #[tokio::test]
    async fn account_discovery_is_empty_without_config_and_redacts_parse_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        assert!(accounts_at(&path).await.unwrap().is_empty());
        std::fs::write(&path, "realm = 'secret-sentinel'").unwrap();
        assert!(
            !accounts_at(&path)
                .await
                .unwrap_err()
                .to_string()
                .contains("sentinel")
        );
    }
}
