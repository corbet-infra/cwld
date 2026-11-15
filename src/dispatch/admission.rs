//! RAM admission: the dispatcher asks, `cdct`'s rules decide.
//!
//! Admission gates RAM and PSI only, never CPU. A refusal means the job stays
//! with the forge; nothing running is displaced.

use std::io;

use cdct::{Conductor, MemoryDecision, MemorySnapshot, ProcReader};

/// The answer to one admission request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admit {
    /// Start the instance.
    Granted,
    /// Memory may free later; try again at the next cycle.
    Wait(String),
    /// The claim can never fit on this host.
    Decline(String),
}

/// Who decides whether a class instance may take its RAM now.
pub trait Admission: Send + Sync {
    /// `claim_mib` is this job's claim; `pending_mib` is the sum of claims
    /// granted but not yet resident (their instances have not started), which
    /// the host's free memory does not show yet.
    fn admit(&self, label: &str, claim_mib: u64, pending_mib: u64) -> Admit;
}

/// Admission by `cdct`'s memory decision on the host's live facts.
///
/// This links the conductor's rules in process (`Conductor::memory`). When the
/// `cdct` daemon grows an admission request, a client adapter replaces this
/// one behind the same trait; the rules stay `cdct`'s either way.
pub struct ConductorAdmission<F> {
    conductor: Conductor,
    snapshot: F,
}

impl<F> ConductorAdmission<F>
where
    F: Fn() -> io::Result<MemorySnapshot> + Send + Sync,
{
    /// Decide with `conductor` on the snapshots `snapshot` reads.
    pub fn new(conductor: Conductor, snapshot: F) -> Self {
        Self {
            conductor,
            snapshot,
        }
    }
}

/// The host's live memory facts (`/proc/meminfo`, ARC stats, PSI).
///
/// # Errors
/// The facts could not be read or parsed.
pub fn system_snapshot() -> io::Result<MemorySnapshot> {
    let raw = ProcReader::system().raw_facts()?;
    MemorySnapshot::parse(&raw).map_err(|e| io::Error::other(e.to_string()))
}

impl<F> Admission for ConductorAdmission<F>
where
    F: Fn() -> io::Result<MemorySnapshot> + Send + Sync,
{
    fn admit(&self, _label: &str, claim_mib: u64, pending_mib: u64) -> Admit {
        let snapshot = match (self.snapshot)() {
            Ok(s) => s,
            // fail closed: without facts nothing is granted
            Err(e) => return Admit::Wait(format!("memory facts unreadable: {e}")),
        };
        match self
            .conductor
            .memory(&snapshot, claim_mib.saturating_add(pending_mib))
        {
            MemoryDecision::Admit(_) => Admit::Granted,
            MemoryDecision::Wait(why) => Admit::Wait(format!("{why:?}")),
            MemoryDecision::Decline(why) => Admit::Decline(format!("{why:?}")),
        }
    }
}
