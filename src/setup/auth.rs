//! Native rkat account discovery and login transport. Fugue never persists AI secrets.
use super::connections::{ModelProvider, Secret};
use anyhow::{Context, Result, bail};
use meerkat_core::{AuthBindingRef, BindingId, BindingOrigin, Config, Provider, RealmId};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, process::Stdio, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, sync::Mutex};

#[derive(Default, Deserialize)]
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
}

/// Secret travels through stdin only. The native CLI chooses storage and writes
/// its owning global config; nothing is copied to Fugue's connection file.
pub async fn provision_key(
    provider: ModelProvider,
    key: &Secret,
    directory: &Path,
) -> Result<AuthBindingRef> {
    key.validate()?;
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
