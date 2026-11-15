//! The workload runtime (`cwld`): it executes, it never decides.
//!
//! `cwld` acts on decisions from the conductor (`cdct`) and on the owner's
//! requests, and it acts only through the `cdrv` host and guest traits (plus
//! the one small port in [`ports`] for what `cdrv` does not define yet: the
//! disk reset `cdsk` will own). It never runs `virsh`, `zfs`, `systemctl` or
//! `kubectl`, and it never reads sysfs.
//!
//! The pieces:
//! - [`classes`] holds the four classes (`dev`, `desktop`, `ci`, `office`) as
//!   validated TOML or JSON data; invalid data is an error at load;
//! - [`lifecycle`] is the one state machine per instance, exactly the
//!   transitions of `CONTRACTS.md` section 6; illegal transitions are typed
//!   errors, and nothing ever destroys an instance on its own;
//! - [`plan`] executes a [`cdct::SwitchPlan`] step by step and stops at the
//!   first failed step with a report of what ran and what did not;
//! - [`handoff`] records a successful guest switch's displaced tenants in
//!   `cdct`'s holder table;
//! - [`dirty`] and [`journal`] are the crash-recovery marks: a dirty mark per
//!   instance and the journal of a plan in flight;
//! - [`ci`] runs one CI job as ensure clean, start, exec, stop, reset;
//! - [`dispatch`] is the CI dispatcher behind `cwld ci`;
//! - [`recovery`] reports what a crash left behind, without guessing;
//! - [`runtime`] ties the pieces together.
//!
//! ```
//! use cwld::{ClassCatalog, Runtime};
//!
//! let classes = ClassCatalog::defaults();
//! assert!(classes.is_ephemeral(cdrv::InstanceClass::Ci));
//! let dir = std::env::temp_dir().join(format!("cwld-doc-{}", std::process::id()));
//! let runtime = Runtime::new(&dir, classes).expect("the state directory opens");
//! assert!(runtime.marks().all().expect("no marks yet").is_empty());
//! let _ = std::fs::remove_dir_all(&dir);
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod ci;
pub mod classes;
pub mod dirty;
pub mod dispatch;
pub mod handoff;
pub mod journal;
pub mod lifecycle;
#[cfg(feature = "clnx")]
pub mod linux;
pub mod plan;
pub mod ports;
pub mod recovery;
pub mod runtime;
pub mod store;
pub mod wait;

pub use ci::{CiJob, CiJobReport};
pub use classes::{
    ClassCatalog, ClassRules, DeviceRule, Hibernation, LoadError, MemoryRule, ParseError,
    Persistence, ResetPolicy, ValidationError,
};
pub use dirty::{DirtyMark, DirtyMarks, DirtyReason};
pub use handoff::{DaemonHandoff, DiscardHandoff, DisplacedHandoff, HandoffError, HolderHandoff};
pub use journal::{Journals, PlanJournal};
pub use lifecycle::{Lifecycle, TransitionError, Trigger};
pub use plan::{HandoffStatus, Hosts, PlanReport, StepError, StepFailure};
pub use ports::DiskReset;
pub use recovery::{DirtyReport, HalfRunPlan, RecoveryReport};
pub use runtime::{CleanOutcome, Runtime, RuntimeError};
pub use store::StoreError;
pub use wait::{Clock, SystemClock, WaitError, WaitPolicy};
