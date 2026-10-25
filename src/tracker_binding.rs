//! Immutable tracker identity for a repository's retained native state.
//!
//! Issue identifiers are only meaningful within their original tracker. Bind
//! that scope before creating native state, and never infer it from replacement
//! settings when unbound state already exists.

use std::{fs, io, io::Write, path::Path};

use serde::{Deserialize, Serialize};

const FILE_NAME: &str = "fugue-tracker-context.json";
const STATE_FILES: [&str; 4] = [
    "mob.sqlite3",
    "fugue-runs.sqlite3",
    "review-requests.sqlite3",
    "workgraph.sqlite3",
];

#[derive(Debug, thiserror::Error)]
pub(crate) enum BindingError {
    #[error(
        "This repository's saved work belongs to a different tracker. Restore its original tracker settings, or use a new workspace for the new tracker."
    )]
    DifferentTracker,
    #[error(
        "This repository has saved native state without a tracker binding. Preserve it and use a fresh workspace; do not attach replacement tracker settings to existing work."
    )]
    UnboundState,
    #[error("Unsupported repository tracker binding version")]
    UnsupportedVersion,
    #[error("Cannot read or write repository tracker binding: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid repository tracker binding: {0}")]
    Invalid(#[from] serde_json::Error),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    tracker_context: String,
}

fn read(root: &Path) -> Result<Option<Binding>, BindingError> {
    match fs::read(root.join(FILE_NAME)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn check(binding: &Binding, context: &str) -> Result<(), BindingError> {
    if binding.version != 1 {
        return Err(BindingError::UnsupportedVersion);
    }
    if binding.tracker_context != context {
        return Err(BindingError::DifferentTracker);
    }
    Ok(())
}

/// Read-only preflight for setup, before contacting providers or changing files.
pub(crate) fn validate(root: &Path, context: &str) -> Result<(), BindingError> {
    if let Some(binding) = read(root)? {
        return check(&binding, context);
    }
    for name in STATE_FILES {
        if root.join(name).try_exists()? {
            // Another initializer may have published its binding after our
            // first read and then created its native store.
            return match read(root)? {
                Some(binding) => check(&binding, context),
                None => Err(BindingError::UnboundState),
            };
        }
    }
    Ok(())
}

/// Publish a complete binding once. Concurrent callers must agree on scope.
pub(crate) fn bind(root: &Path, context: &str) -> Result<(), BindingError> {
    validate(root, context)?;
    if let Some(binding) = read(root)? {
        return check(&binding, context);
    }
    fs::create_dir_all(root)?;
    let binding = Binding {
        version: 1,
        tracker_context: context.to_owned(),
    };
    let mut pending = tempfile::NamedTempFile::new_in(root)?;
    pending.write_all(&serde_json::to_vec(&binding)?)?;
    pending.as_file().sync_all()?;
    match pending.persist_noclobber(root.join(FILE_NAME)) {
        Ok(_) => {
            #[cfg(unix)]
            fs::File::open(root)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => match read(root)? {
            Some(binding) => check(&binding, context),
            None => Err(error.error.into()),
        },
        Err(error) => Err(error.error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_binding_preflight_does_not_create_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("new");
        validate(&root, "first").unwrap();
        assert!(!root.exists());
        bind(&root, "first").unwrap();
        assert!(matches!(
            validate(&root, "second"),
            Err(BindingError::DifferentTracker)
        ));
    }

    #[test]
    fn tracker_binding_never_adopts_or_changes_unbound_state() {
        for name in STATE_FILES {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(name);
            fs::write(&path, b"preserved native state").unwrap();
            assert!(matches!(
                bind(temp.path(), "replacement"),
                Err(BindingError::UnboundState)
            ));
            assert_eq!(fs::read(&path).unwrap(), b"preserved native state");
            assert!(!temp.path().join(FILE_NAME).exists());
        }
    }

    #[test]
    fn tracker_binding_concurrent_claims_have_one_scope() {
        let temp = tempfile::tempdir().unwrap();
        let barrier = std::sync::Barrier::new(4);
        let outcomes = std::thread::scope(|scope| {
            let threads: Vec<_> = ["first", "first", "second", "second"]
                .into_iter()
                .map(|context| {
                    let barrier = &barrier;
                    let root = temp.path();
                    scope.spawn(move || {
                        barrier.wait();
                        (context, bind(root, context))
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        let winner = read(temp.path()).unwrap().unwrap().tracker_context;
        for (context, result) in outcomes {
            if context == winner {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(BindingError::DifferentTracker)));
            }
        }
    }

    #[test]
    fn tracker_binding_invalid_record_is_preserved() {
        for bytes in [
            b"{truncated".as_slice(),
            br#"{"version":2,"tracker_context":"first"}"#,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(FILE_NAME);
            fs::write(&path, bytes).unwrap();
            assert!(bind(temp.path(), "first").is_err());
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }
}
