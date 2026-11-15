//! Small JSON-on-disk helpers shared by the dirty marks and the plan journal.
//!
//! Every file is written through a temporary file and renamed into place, so a
//! crash leaves either the old content or the new one, never a half-written
//! document. A missing file is `None`, but a file that exists and does not
//! parse is an error: `cwld` never guesses what a broken state file meant.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

/// A persisted-state failure.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A state file or directory could not be created, read, written or
    /// removed.
    #[error("state file {path}: {source}")]
    Io {
        /// The file that failed.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A state file exists but is not valid JSON for its type.
    #[error("state file {path} is invalid JSON: {source}")]
    Json {
        /// The file that failed.
        path: String,
        /// The underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
}

pub(crate) fn io_error(path: &Path, source: std::io::Error) -> StoreError {
    StoreError::Io {
        path: path.display().to_string(),
        source,
    }
}

fn json_error(path: &Path, source: serde_json::Error) -> StoreError {
    StoreError::Json {
        path: path.display().to_string(),
        source,
    }
}

/// Create a directory and every missing parent.
pub(crate) fn ensure_dir(path: &Path) -> Result<(), StoreError> {
    fs::create_dir_all(path).map_err(|source| io_error(path, source))
}

/// Write one JSON document through a temporary file and rename it into place.
pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let text = serde_json::to_vec_pretty(value).map_err(|source| json_error(path, source))?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, text).map_err(|source| io_error(&temporary, source))?;
    fs::rename(&temporary, path).map_err(|source| io_error(path, source))
}

/// Read one JSON document; a missing file is `None`, a broken one an error.
pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, StoreError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error(path, source)),
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|source| json_error(path, source))
}

/// Remove one file; `Ok(false)` means it was already gone.
pub(crate) fn remove_file(path: &Path) -> Result<bool, StoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error(path, source)),
    }
}

/// Every `.json` file in a directory, in name order.
///
/// A missing directory is empty: nothing has been written yet. Temporary files
/// from an interrupted write are ignored.
pub(crate) fn json_files(dir: &Path) -> Result<Vec<PathBuf>, StoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(io_error(dir, source)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| io_error(dir, source))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// A filename-safe, injective key for an identifier.
///
/// Instance names are opaque text and may contain anything; hex encoding keeps
/// the key safe for every filesystem and cannot collide.
pub(crate) fn file_key(text: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = String::with_capacity(text.len() * 2);
    for byte in text.as_bytes() {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    key
}

#[cfg(test)]
mod tests {
    use super::file_key;

    #[test]
    fn file_keys_are_injective_and_safe() {
        assert_eq!(file_key("a"), "61");
        assert_eq!(file_key("../x y"), "2e2e2f782079");
        assert_ne!(file_key("ab"), file_key("a b"));
    }
}
