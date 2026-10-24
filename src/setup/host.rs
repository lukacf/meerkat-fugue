//! Stable multi-repository entry point for packaged installations.
use anyhow::Result;
use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
};

pub async fn serve(root: PathBuf, port: u16) -> Result<()> {
    // Apply the existing private-storage rules before creating host files.
    drop(super::connections::ConnectionStore::open(&root)?);
    let config_path = root.join("host.yaml");
    if !config_path.try_exists()? {
        let mut config = crate::ensemble::config::EnsembleConfig::for_repositories(
            &[],
            "My repositories".into(),
        )?;
        config.state_root = root.join("host-team");
        let mut pending = tempfile::NamedTempFile::new_in(&root)?;
        use std::io::Write;
        pending.write_all(serde_yaml::to_string(&config)?.as_bytes())?;
        pending.as_file().sync_all()?;
        pending.persist_noclobber(&config_path)?;
    }
    crate::ensemble::serve_with_directory(
        config_path,
        SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        Some(root.join("repositories")),
    )
    .await
}
