//! The hand-off back to `cdct`: record what a successful switch displaced.
//!
//! `cdct` documents that the runtime records a plan's `displaced` tenants with
//! the holder that took the card, so a later switch back to the host restores
//! exactly that set. While the conductor daemon runs it is the table's one
//! writer and holds its lock for life, so [`DaemonHandoff`] asks the daemon to
//! record the list (`record_displaced` on the local socket); [`HolderHandoff`]
//! writes the table file directly and is for hosts without a daemon.

use std::path::{Path, PathBuf};

use cdct::{Client, Endpoint, HolderStore, SwitchPlan};
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
        let mut store = HolderStore::open(&self.path)
            .map_err(|error| HandoffError::Store(error.to_string()))?;
        let mut table = store
            .load()
            .map_err(|error| HandoffError::Store(error.to_string()))?;
        let Some(target) = plan.target.guest() else {
            // A ladder shed: the card stays on the host, the counts go to the shed list.
            if plan.displaced.is_empty() {
                return Err(HandoffError::HostTarget);
            }
            for counted in &plan.displaced {
                if let Some(shed) = table
                    .shed
                    .iter_mut()
                    .find(|shed| shed.tenant == counted.tenant)
                {
                    shed.replicas = counted.replicas;
                }
            }
            return store
                .save(&table)
                .map_err(|error| HandoffError::Store(error.to_string()));
        };
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

/// A hand-off through the running conductor daemon, which owns the holder
/// table: the list is sent as one `record_displaced` request on its local
/// socket.
#[derive(Debug, Clone)]
pub struct DaemonHandoff {
    endpoint: Endpoint,
}

impl DaemonHandoff {
    /// A hand-off to the daemon listening on `socket`.
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            endpoint: Endpoint::LocalSocket(socket.into()),
        }
    }
}

impl DisplacedHandoff for DaemonHandoff {
    fn record(&mut self, plan: &SwitchPlan) -> Result<(), HandoffError> {
        // A host target records only a ladder shed's counts.
        if plan.target.is_host() && plan.displaced.is_empty() {
            return Err(HandoffError::HostTarget);
        }
        let mut client = Client::connect(&self.endpoint)
            .map_err(|error| HandoffError::Store(error.to_string()))?;
        client
            .record_displaced(&plan.displaced)
            .map(|_holder| ())
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
