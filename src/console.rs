//! One native Meta console projection for every repository runtime in this host.
use anyhow::Result;
use axum::Router;
use meerkat_meta::{
    RuntimeDecisionState, UnifiedRuntime,
    console_aggregator::{
        ConsoleRuntimeRegistration, MetaConsoleAggregator, SqliteConsoleLogStore,
    },
};
use std::{path::Path, sync::Arc};

pub struct ConsoleHub {
    pub(crate) aggregator: MetaConsoleAggregator,
}

impl ConsoleHub {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            aggregator: MetaConsoleAggregator::new(Arc::new(
                SqliteConsoleLogStore::open(path).map_err(anyhow::Error::from_boxed)?,
            )),
        }))
    }

    pub fn register(&self, key: String, runtime: Arc<UnifiedRuntime>) {
        self.aggregator
            .register_runtime(ConsoleRuntimeRegistration {
                identity_namespace: key.clone(),
                runtime_key: key,
                runtime,
                visibility_policy: Arc::new(crate::meta_runtime::FugueConsoleVisibilityPolicy),
            });
    }

    pub fn unregister(&self, key: &str) {
        self.aggregator.unregister_runtime(key);
    }

    pub fn router(&self, decisions: RuntimeDecisionState) -> Router {
        meerkat_meta::http_console::console_frontend_router().merge(
            meerkat_meta::http_console::console_json_router_with_aggregator(
                decisions,
                self.aggregator.clone(),
            ),
        )
    }
}
