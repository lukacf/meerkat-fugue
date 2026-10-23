//! Process-owned file leases with explicit release before descriptor teardown.

use std::fs::{File, TryLockError};

#[derive(Debug)]
pub(crate) struct FileLease(File);

impl FileLease {
    pub(crate) fn acquire(file: File) -> Result<Self, TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
    }

    #[cfg(test)]
    pub(crate) fn duplicate_descriptor_for_test(&self) -> std::io::Result<File> {
        self.0.try_clone()
    }
}

impl Drop for FileLease {
    fn drop(&mut self) {
        // A concurrent fork can briefly retain a descriptor before exec closes
        // it. Relinquish the lease explicitly instead of waiting for every copy.
        if let Err(error) = self.0.unlock() {
            tracing::error!(%error, "failed to release Fugue state ownership");
        }
    }
}
