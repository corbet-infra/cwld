//! The host ports `cdrv` does not define yet.
//!
//! `cdrv` is deliberately small and stable. It has no trait for the host GPU
//! tenants (the `gpu-desktop` session and the GPU pods), no trait for the
//! Sunshine service that follows the card, and no trait for resetting an
//! instance disk to its clean snapshot (`cdsk` will own that). `cwld` executes
//! those plan steps through these ports instead of shelling out to `kubectl`,
//! `systemctl` or `zfs`: an OS driver crate implements them beside the `cdrv`
//! traits, and each port moves into `cdrv` once the contract grows it.

use cdct::{GpuLadder, KernelOwner};
use cdrv::{InstanceId, Result};

/// The host-side platform actions a switch plan needs beyond `cdrv`.
pub trait HostPlatform {
    /// Take one host tenant off the card.
    ///
    /// A pod is scaled to zero; the desktop session must keep running and only
    /// release the card, as the GPU platform requires.
    fn stop_tenant(&self, tenant: &str, ladder: GpuLadder) -> Result<()>;

    /// Return one host tenant to the card.
    fn start_tenant(&self, tenant: &str, ladder: GpuLadder) -> Result<()>;

    /// Move Sunshine to the kernel that now holds the card.
    fn sunshine_follows(&self, kernel: &KernelOwner) -> Result<()>;
}

/// The storage action `cdsk` will own: roll an instance disk back to clean.
pub trait DiskReset {
    /// Reset the instance's disk to its clean snapshot.
    fn reset_to_clean(&self, instance: &InstanceId) -> Result<()>;
}
