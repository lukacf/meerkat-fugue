//! Browser device authorization through Meerkat's native host-auth service.
//! Only the user-facing code is projected to the authenticated setup UI.
use super::{HostConnections, LoginStatus};
use anyhow::{Context, Result, bail};
use meerkat_core::{
    AuthConstraints, AuthProfileConfig, BackendProfileConfig, BindingId, Config, ConfigDelta,
    CredentialAccountId, CredentialSourceSpec, FileConfigStore, OAuthProviderIdentity,
    ProviderBindingConfig, RealmConfigSection, RealmId,
};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;

static SIGN_IN: Mutex<()> = Mutex::const_new(());

async fn configure(path: &Path) -> Result<Config> {
    let store = Arc::new(FileConfigStore::new(
        path.to_path_buf(),
        meerkat_models::canonical(),
    ));
    let runtime = meerkat_core::ConfigRuntime::new(
        store,
        path.parent()
            .context("Config document has no parent")?
            .join("config_state.json"),
    );
    let guard = meerkat_core::AdmissionGuard::try_acquire(runtime.admission_fences())?;
    let snapshot = runtime.get().await?;
    let config = snapshot.config;
    let existing = config.realm.get("global");
    if existing.is_some_and(|section| section.copilot.is_some()) {
        bail!("An operator-configured Copilot account already exists");
    }
    let mut section = RealmConfigSection::default();
    let account = CredentialAccountId::parse("github_copilot")?;
    for route in meerkat_copilot::CopilotProviderRoute::ALL {
        let id = route.binding_id();
        let backend = BackendProfileConfig {
            provider: route.provider().as_str().into(),
            backend_kind: route.backend_kind().into(),
            base_url: None,
            options: serde_json::Value::Null,
            server: None,
        };
        let auth = AuthProfileConfig {
            provider: route.provider().as_str().into(),
            auth_method: route.auth_method().into(),
            source: CredentialSourceSpec::ManagedStore,
            constraints: AuthConstraints {
                allow_interactive_login: true,
                ..Default::default()
            },
            metadata_defaults: Default::default(),
        };
        let binding = ProviderBindingConfig {
            backend_profile: id.into(),
            auth_profile: id.into(),
            credential_account: Some(account.clone()),
            default_model: None,
            policy: Default::default(),
            provider_default: false,
        };
        if let Some(old) = existing {
            // Never replace operator settings or another authentication method.
            if old.backend.get(id).is_some_and(|v| v != &backend)
                || old.auth.get(id).is_some_and(|v| v != &auth)
                || old.binding.get(id).is_some_and(|v| {
                    v.backend_profile != binding.backend_profile
                        || v.auth_profile != binding.auth_profile
                        || v.credential_account != binding.credential_account
                })
            {
                bail!("Existing Copilot configuration requires operator review");
            }
        }
        if !existing.is_some_and(|old| old.backend.contains_key(id)) {
            section.backend.insert(id.into(), backend);
        }
        if !existing.is_some_and(|old| old.auth.contains_key(id)) {
            section.auth.insert(id.into(), auth);
        }
        if !existing.is_some_and(|old| old.binding.contains_key(id)) {
            section.binding.insert(id.into(), binding);
        }
    }
    let patch = HostConnections {
        realm: BTreeMap::from([("global".into(), section)]),
    };
    Ok(runtime
        .patch_under(
            &guard,
            ConfigDelta(serde_json::to_value(patch)?),
            Some(snapshot.generation),
        )
        .await?
        .config)
}

pub(super) async fn login(
    directory: &Path,
    status: &Arc<Mutex<LoginStatus>>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    use meerkat_providers::auth_store::TokenStoreBackend;
    let _login_guard = SIGN_IN
        .try_lock()
        .context("Another Copilot sign-in is in progress")?;
    let path =
        Config::global_config_path().context("Cannot locate native account configuration")?;
    let config = configure(&path).await?;
    let persistence = TokenStoreBackend::default_auto()?.open_with_refresh_authority()?;
    let factory = meerkat::AgentFactory::new(directory.join("auth-sessions"))
        .context_root(directory)
        .with_provider_auth_persistence(persistence.clone())
        .builtins(false)
        .shell(false)
        .comms(false)
        .mob(false)
        .memory(false)
        .schedule(false)
        .workgraph(false);
    let runtime = meerkat::build_ephemeral_service(factory, config.clone(), 1);
    let machine = runtime
        .canonical_runtime_adapter()
        .context("Native auth runtime unavailable")?;
    let auth =
        meerkat::HostAuthService::new(persistence, machine.provider_auth_runtime_authority());
    let target = meerkat::HostAuthTarget {
        provider: OAuthProviderIdentity::GitHubCopilot,
        realm_id: RealmId::global(),
        binding_id: BindingId::parse("copilot_openai")?,
        profile_id: None,
    };
    let device = tokio::time::timeout(Duration::from_secs(30), auth.device_start(&config, &target))
        .await??;
    // The provider owns this URI. Refuse anything except its pinned HTTPS page.
    if device.verification_uri != "https://github.com/login/device" {
        bail!("Unexpected device authorization origin");
    }
    *status.lock().await = LoginStatus::Device {
        verification_uri: device.verification_uri.clone(),
        user_code: device.user_code.clone(),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(device.expires_in.min(1800));
    let mut interval = device.interval.max(1);
    loop {
        // Cancellation and expiry can stop waiting between polls. A poll
        // already in progress must settle, including its native token commit.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                *status.lock().await = LoginStatus::Cancelled;
                return Ok(());
            }
            _ = tokio::time::sleep_until(deadline) => {
                *status.lock().await = LoginStatus::Failed { error: "GitHub sign-in expired. Start sign-in again." };
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_secs(interval)) => {}
        }
        let result = auth
            .device_poll(&config, &target, &device.device_code)
            .await;
        let next = match result {
            Err(_) => LoginStatus::Failed {
                error: "GitHub sign-in could not be confirmed. Check connected accounts before retrying.",
            },
            Ok(meerkat::HostAuthDevicePoll::Pending) => continue,
            Ok(meerkat::HostAuthDevicePoll::SlowDown) => {
                interval = interval.saturating_add(5);
                continue;
            }
            Ok(meerkat::HostAuthDevicePoll::AccessDenied) => LoginStatus::Failed {
                error: "GitHub sign-in was declined. You can retry when ready.",
            },
            Ok(meerkat::HostAuthDevicePoll::Expired) => LoginStatus::Failed {
                error: "GitHub sign-in expired. Start sign-in again.",
            },
            Ok(meerkat::HostAuthDevicePoll::Ready(_)) => LoginStatus::CopilotComplete,
        };
        *status.lock().await = next;
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configuration_refuses_other_writer_fence_without_modifying_document() {
        use meerkat_core::ConfigStore;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        configure(&path).await.unwrap();
        let before = std::fs::read(&path).unwrap();
        let store = FileConfigStore::new(path.clone(), meerkat_models::canonical());
        let fence = store.admission_fence().unwrap();
        let guard = meerkat_core::AdmissionGuard::try_acquire(vec![fence]).unwrap();
        assert!(configure(&path).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(guard);
        configure(&path).await.unwrap();
    }

    #[tokio::test]
    async fn copilot_setup_preserves_other_accounts_and_uses_shared_native_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        super::super::ensure_api_key_binding(&path, super::super::ModelProvider::Openai)
            .await
            .unwrap();
        let config = configure(&path).await.unwrap();
        assert!(
            config.realm["global"]
                .binding
                .contains_key("default_openai")
        );
        for route in meerkat_copilot::CopilotProviderRoute::ALL {
            let binding = &config.realm["global"].binding[route.binding_id()];
            assert_eq!(
                binding.credential_account.as_ref().unwrap().as_str(),
                "github_copilot"
            );
            assert!(!binding.provider_default);
        }
        configure(&path).await.unwrap();
        assert_eq!(super::super::accounts_at(&path).await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn copilot_setup_refuses_conflicting_route_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        configure(&path).await.unwrap();
        let original = std::fs::read_to_string(&path)
            .unwrap()
            .replace("github_copilot_oauth", "github_copilot_token");
        std::fs::write(&path, &original).unwrap();
        assert!(configure(&path).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    }
}
