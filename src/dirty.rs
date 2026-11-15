//! One dirty mark per instance.
//!
//! A mark is written before `cwld` changes an instance's disk state (a start or
//! a clean stop) and removed once the instance is cleanly stopped. A mark that
//! survives a crash is the evidence that the instance's disk may not match its
//! last clean state; `cwld` reports it, and `ensure_clean` resets an ephemeral
//! instance before its next start. A persistent instance is never reset
//! automatically.

use std::path::{Path, PathBuf};

use cdrv::{InstanceId, Timestamp};
use serde::{Deserialize, Serialize};

use crate::store::{self, StoreError};

/// Why an instance is marked dirty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirtyReason {
    /// A start was requested and has not yet ended in a clean stop.
    Starting,
    /// A clean stop was requested and has not yet completed.
    Stopping,
}

/// One instance's dirty mark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirtyMark {
    /// The instance the mark belongs to.
    pub instance: InstanceId,
    /// Why it was written.
    pub reason: DirtyReason,
    /// When it was written.
    pub at: Timestamp,
    /// Optional detail for a human.
    pub detail: Option<String>,
}

/// The dirty marks of one state directory, one JSON file per instance.
#[derive(Debug, Clone)]
pub struct DirtyMarks {
    dir: PathBuf,
}

impl DirtyMarks {
    /// Open (creating if needed) the marks directory.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let dir = dir.into();
        store::ensure_dir(&dir)?;
        Ok(Self { dir })
    }

    /// The directory the marks live in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write or replace one instance's mark.
    pub fn mark(
        &self,
        instance: &InstanceId,
        reason: DirtyReason,
        at: Timestamp,
        detail: Option<String>,
    ) -> Result<DirtyMark, StoreError> {
        let mark = DirtyMark {
            instance: instance.clone(),
            reason,
            at,
            detail,
        };
        store::write_json(&self.path(instance), &mark)?;
        Ok(mark)
    }

    /// Remove one instance's mark; `Ok(false)` means it was not dirty.
    pub fn clear(&self, instance: &InstanceId) -> Result<bool, StoreError> {
        store::remove_file(&self.path(instance))
    }

    /// One instance's mark, when it is dirty.
    pub fn get(&self, instance: &InstanceId) -> Result<Option<DirtyMark>, StoreError> {
        store::read_json(&self.path(instance))
    }

    /// Whether one instance is dirty.
    pub fn is_dirty(&self, instance: &InstanceId) -> Result<bool, StoreError> {
        Ok(self.get(instance)?.is_some())
    }

    /// Every mark, in filename order.
    pub fn all(&self) -> Result<Vec<DirtyMark>, StoreError> {
        let mut marks = Vec::new();
        for path in store::json_files(&self.dir)? {
            if let Some(mark) = store::read_json(&path)? {
                marks.push(mark);
            }
        }
        Ok(marks)
    }

    fn path(&self, instance: &InstanceId) -> PathBuf {
        self.dir
            .join(format!("{}.json", store::file_key(instance.as_str())))
    }
}
