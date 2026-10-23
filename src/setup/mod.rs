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

/// Publish complete private JSON without reusing a crashed writer's temporary
/// file. The owning setup/directory locks serialize changes to each document.
fn save_json<T: serde::Serialize + ?Sized>(
    path: &std::path::Path,
    value: &T,
) -> anyhow::Result<()> {
    use anyhow::Context;
    use std::io::Write;
    let parent = path
        .parent()
        .context("Setup document has no parent directory")?;
    let mut pending = tempfile::Builder::new()
        .prefix(".fugue-save-")
        .tempfile_in(parent)?;
    {
        let mut writer = std::io::BufWriter::new(pending.as_file_mut());
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.flush()?;
    }
    pending.as_file().sync_all()?;
    pending.persist(path)?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::save_json;
    use serde::ser::{SerializeMap, Serializer};

    struct InterruptedWrite;
    impl serde::Serialize for InterruptedWrite {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("saved", "partial replacement")?;
            Err(serde::ser::Error::custom("simulated write interruption"))
        }
    }

    #[test]
    fn failed_json_save_preserves_committed_document_and_cleans_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("workflow-draft.json");
        save_json(&path, &"committed settings").unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(save_json(&path, &InterruptedWrite).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        save_json(&path, &"retried settings").unwrap();
        assert_eq!(
            serde_json::from_slice::<String>(&std::fs::read(&path).unwrap()).unwrap(),
            "retried settings"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
