//! The one host port `cdrv` does not define yet: the disk reset.
//!
//! Everything a plan needs on the host side is now a `cdrv` trait
//! ([`cdrv::HostInstances`], [`cdrv::HostDevices`] and [`cdrv::HostGpu`]).
//! Resetting an instance disk to its clean snapshot is the exception: the
//! storage component (`cdsk`, the next iteration) will own it, and until the
//! contract grows that trait `cwld` executes the reset through this port
//! instead of shelling out to `zfs`. This is the seam `cdsk` fills.

use cdrv::{InstanceId, Result};

/// The storage action `cdsk` will own: roll an instance disk back to clean.
///
/// `cwld` calls it before an ephemeral instance's start and after a CI job,
/// and never touches a dataset itself.
pub trait DiskReset {
    /// Reset the instance's disk to its clean snapshot.
    fn reset_to_clean(&self, instance: &InstanceId) -> Result<()>;
}
