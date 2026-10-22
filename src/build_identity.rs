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

/// All cooperating executables expose the same immutable release identity.
pub fn print_if_requested() -> anyhow::Result<bool> {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_some_and(|argument| argument == "build-info") && args.next().is_none() {
        println!("{}", serde_json::to_string(&current())?);
        return Ok(true);
    }
    Ok(false)
}
