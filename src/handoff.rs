//! The hand-off back to `cdct`: record what a successful switch displaced.
//!
//! `cdct` documents that the runtime records a plan's `displaced` tenants with
//! the holder that took the card, so a later switch back to the host restores
//! exactly that set. `cwld` writes it through `cdct`'s own holder store, which
//! keeps the one-writer-per-host file lock; a plan that did not run to the end
//! records nothing.

use std::path::{Path, PathBuf};

use cdct::{HolderStore, SwitchPlan};
use cdrv::InstanceId;
use thiserror::Error;

/// Where a successful guest switch records the tenants it displaced.
pub trait DisplacedHandoff {
    /// Record the plan's displaced tenants. Called only after every step ran.
    fn record(&mut self, plan: &SwitchPlan) -> Result<(), HandoffError>;
}

/// A hand-off that writes the holder table on disk.
#[derive(Debug, Clone)]
pub struct HolderHandoff {
    path: PathBuf,
}

impl HolderHandoff {
    /// A hand-off for the holder table at `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The holder table this hand-off writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DisplacedHandoff for HolderHandoff {
    fn record(&mut self, plan: &SwitchPlan) -> Result<(), HandoffError> {
        let target = plan.target.guest().ok_or(HandoffError::HostTarget)?;
        let mut store = HolderStore::open(&self.path)
            .map_err(|error| HandoffError::Store(error.to_string()))?;
        let mut table = store
            .load()
            .map_err(|error| HandoffError::Store(error.to_string()))?;
        let holder = table
            .holders
            .iter_mut()
            .find(|holder| {
                holder.is_card_custodian()
                    && holder
                        .instance
                        .as_ref()
                        .is_some_and(|instance| instance == target)
            })
            .ok_or_else(|| HandoffError::NoCustodian {
                target: target.clone(),
            })?;
        holder.displaced = plan.displaced.clone();
        store
            .save(&table)
            .map_err(|error| HandoffError::Store(error.to_string()))
    }
}

/// A hand-off that records nothing; for a dry run and for tests that do not
/// exercise the holder table.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiscardHandoff;

impl DisplacedHandoff for DiscardHandoff {
    fn record(&mut self, _plan: &SwitchPlan) -> Result<(), HandoffError> {
        Ok(())
    }
}

/// Why a hand-off could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandoffError {
    /// The holder table could not be opened, read or written.
    #[error("holder store: {0}")]
    Store(String),
    /// No card custodian in the holder table holds this guest.
    #[error("the holder table has no card custodian for guest {target}")]
    NoCustodian {
        /// The guest the plan started.
        target: InstanceId,
    },
    /// A plan that returns the card to the host has no guest holder to
    /// annotate.
    #[error("a plan for the host kernel has no guest holder to annotate")]
    HostTarget,
}
