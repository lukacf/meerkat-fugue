//! First-run configuration and connection checks for the local Fugue app.
//!
//! Credentials are host configuration. They never become repository content,
//! agent instructions, or browser-readable configuration.
pub mod auth;
pub mod connections;
pub mod directory;
pub mod host;
pub mod managed;
mod proxy;
pub mod repository;
pub mod settings;
pub mod web;
pub mod workflow;

pub fn default_state_dir() -> anyhow::Result<std::path::PathBuf> {
    use anyhow::Context;
    use std::path::PathBuf;
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path).join("meerkat-fugue"));
    }
    let home = std::env::var_os("HOME")
        .context("Set HOME or pass --state-dir for private application state")?;
    Ok(PathBuf::from(home).join(".local/share/meerkat-fugue"))
}

pub(crate) mod runtime_host;
