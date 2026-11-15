//! The CI dispatcher (`cwld ci`): Forgejo jobs onto clean, one-job instances.
//!
//! It replaces `runner-dispatch`, `runner-slot` and `runner-vm-slot`. CI is a
//! `cwld` class (ephemeral, one job, then reset) with the Forgejo runner
//! inside; the deployment (a separate repository) supplies the configuration file, the unit
//! properties and the registration credential. CI does not join `ccid`.
//!
//! | Piece | Module | Port | Production adapter |
//! |---|---|---|---|
//! | Forgejo | [`forge`] | [`forge::Forge`] | [`forge::ForgeClient`] over [`http::CurlHttp`] |
//! | RAM admission | [`admission`] | [`admission::Admission`] | [`admission::ConductorAdmission`] (`cdct` rules) |
//! | host slot | [`exec`], [`units`] | [`units::UnitRunner`] | [`units::SystemdRun`] |
//! | VM slot | [`exec`] | [`exec::VmDriver`] | [`vmdriver::RuntimeVmDriver`] (`cdrv` `exec_stream`) |
//! | job flow, spares, drain | [`flow`] | | |

pub mod admission;
pub mod cancel;
pub mod commandguest;
pub mod config;
pub mod exec;
pub mod flow;
pub mod forge;
pub mod http;
pub mod secret;
pub mod units;
pub mod vmdriver;
