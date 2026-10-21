//! Read-only identity of the compiled executable, used to verify deployments.
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BuildIdentity {
    pub version: &'static str,
    pub commit: &'static str,
    pub tree: &'static str,
    pub dirty: bool,
}

pub fn current() -> BuildIdentity {
    BuildIdentity {
        version: env!("CARGO_PKG_VERSION"),
        commit: env!("FUGUE_BUILD_COMMIT"),
        tree: env!("FUGUE_BUILD_TREE"),
        dirty: env!("FUGUE_BUILD_DIRTY") != "false",
    }
}

pub async fn endpoint() -> axum::Json<BuildIdentity> {
    axum::Json(current())
}
