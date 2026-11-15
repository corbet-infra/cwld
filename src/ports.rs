//! The disk-reset port, now a `cdrv` trait.
//!
//! Resetting an instance disk to its clean snapshot belongs to the storage
//! component (`cdsk`); `cwld` executes it through the contract's
//! [`DiskReset`] and never shells out to `zfs`.

pub use cdrv::DiskReset;
