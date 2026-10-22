//! Bounded formation work uses native runtime admission, including auth leases.
use std::sync::Arc;

use anyhow::{Context, Result};
use meerkat_core::service::{
    CreateSessionRequest, DeferredPromptPolicy, InitialTurnPolicy, SessionBuildOptions,
};

type Service = meerkat::EphemeralSessionService<meerkat::FactoryAgentBuilder>;

pub(super) struct FormationSession {
    service: Arc<Service>,
    session: Option<meerkat::SessionId>,
}

impl FormationSession {
    pub async fn new(
        factory: meerkat::AgentFactory,
        config: meerkat::Config,
        model: &str,
        instructions: &str,
        build: SessionBuildOptions,
    ) -> Result<Self> {
        let service = Arc::new(meerkat::build_ephemeral_service(factory, config, 1));
        let machine = service.canonical_runtime_adapter()?;
        let created = meerkat::surface::materialize_ephemeral_runtime_session(
            &service,
            &machine,
            CreateSessionRequest {
                model: model.to_owned(),
                prompt: String::new().into(),
                injected_context: Vec::new(),
                system_prompt: meerkat::SystemPromptOverride::Set(instructions.to_owned()),
                max_tokens: None,
                event_tx: None,
                initial_turn: InitialTurnPolicy::Defer,
                deferred_prompt_policy: DeferredPromptPolicy::Discard,
                build: Some(build),
                labels: None,
            },
            false,
        )
        .await?;
        Ok(Self {
            service,
            session: Some(created.session_id),
        })
    }

    pub async fn run(&self, prompt: String) -> Result<meerkat::RunResult> {
        let session = self
            .session
            .as_ref()
            .context("formation session is closed")?;
        let machine = self.service.canonical_runtime_adapter()?;
        let outcome =
            meerkat::surface::run_ephemeral_runtime_turn(&machine, session, prompt.into(), None)
                .await?;
        Ok(meerkat::surface::ephemeral_runtime_completion_result(
            outcome,
        )?)
    }

    pub async fn snapshot(&self) -> Result<meerkat::Session> {
        let session = self
            .session
            .as_ref()
            .context("formation session is closed")?;
        Ok(self.service.export_session(session).await?)
    }

    pub async fn close(&mut self) -> Result<()> {
        if let Some(session) = &self.session {
            retire(&self.service, session).await?;
            self.session = None;
        }
        Ok(())
    }
}

async fn retire(service: &Arc<Service>, session: &meerkat::SessionId) -> Result<()> {
    service
        .canonical_runtime_adapter()?
        .unregister_session(session)
        .await?;
    service.try_shutdown().await?;
    Ok(())
}

impl Drop for FormationSession {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let service = self.service.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(error) = retire(&service, &session).await {
                        tracing::error!(%session, %error, "formation session cleanup failed");
                    }
                });
            }
            Err(error) => tracing::error!(%session, %error, "formation cleanup has no runtime"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerkat_core::auth::{PersistedTokens, TokenKey};
    use meerkat_core::{AuthBindingRef, BindingId, BindingOrigin, RealmId};
    use meerkat_providers::auth_store::TokenStoreBackend;

    #[tokio::test]
    async fn stored_api_key_formation_uses_native_auth_and_retires_its_actor() {
        let root = tempfile::tempdir().unwrap();
        let persistence = TokenStoreBackend::File {
            root: root.path().join("credentials"),
        }
        .open_with_refresh_authority()
        .unwrap();
        let binding = AuthBindingRef {
            realm: RealmId::parse("global").unwrap(),
            binding: BindingId::parse("default_openai").unwrap(),
            profile: None,
            origin: BindingOrigin::Configured,
        };
        let (config, _) = meerkat::Config::from_persisted_toml(
            r#"
[realm.global.backend.default_openai]
provider = "openai"
backend_kind = "openai_api"
[realm.global.auth.default_openai]
provider = "openai"
auth_method = "api_key"
source = { kind = "managed_store" }
[realm.global.binding.default_openai]
backend_profile = "default_openai"
auth_profile = "default_openai"
"#,
        )
        .unwrap();
        let factory = || {
            meerkat::AgentFactory::new(root.path().join("sessions"))
                .context_root(root.path())
                .user_config_root(root.path())
                .with_provider_auth_persistence(persistence.clone())
                .builtins(false)
                .shell(false)
                .comms(false)
                .mob(false)
                .memory(false)
                .schedule(false)
                .workgraph(false)
        };
        // Match rkat login's native lifecycle publication and durable marker.
        let login = meerkat::build_ephemeral_service(factory(), config.clone(), 1);
        let login_machine = login.canonical_runtime_adapter().unwrap();
        let tokens = PersistedTokens::api_key("formation-test-key-never-sent");
        let transition = meerkat_core::publish_token_lifecycle_acquired(
            &login_machine.generated_auth_lease_handle(),
            &binding,
            &tokens,
        )
        .unwrap();
        let key = TokenKey::from_auth_binding(&binding);
        let marked = meerkat_core::mark_tokens_lifecycle_published_for_transition(
            &key,
            &tokens,
            &transition,
        )
        .unwrap();
        persistence.token_store().save(&key, &marked).await.unwrap();
        let mut standalone = meerkat::AgentBuildConfig::new("gpt-6-astra");
        standalone.auth_binding = Some(binding.clone());
        let error = factory()
            .build_agent(standalone, &config)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("auth lease absent"), "{error}");

        let mut session = FormationSession::new(
            factory(),
            config.clone(),
            "gpt-6-astra",
            "Study the repository.",
            SessionBuildOptions {
                auth_binding: Some(binding.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let id = session.snapshot().await.unwrap().id().clone();
        let machine = session.service.canonical_runtime_adapter().unwrap();
        assert!(machine.contains_session(&id).await);
        tokio::time::timeout(std::time::Duration::from_secs(10), session.close())
            .await
            .unwrap()
            .unwrap();
        assert!(!machine.contains_session(&id).await);
        assert!(!session.service.live_session_actor_registered(&id).await);
        session.close().await.unwrap();

        let cancelled = FormationSession::new(
            factory(),
            config,
            "gpt-6-astra",
            "Study the repository.",
            SessionBuildOptions {
                auth_binding: Some(binding),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let id = cancelled.snapshot().await.unwrap().id().clone();
        let service = cancelled.service.clone();
        let machine = service.canonical_runtime_adapter().unwrap();
        drop(cancelled);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while machine.contains_session(&id).await {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!service.live_session_actor_registered(&id).await);
    }
}
