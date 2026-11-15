//! The cancel signal.
//!
//! A cancelled run makes Forgejo delete the job's EPHEMERAL runner
//! registration, so every report of the one-job runner is answered
//! "unauthenticated: unregistered runner" and the runner never learns that the
//! task was cancelled (measured 2026-11-14, forgejo-runner 13.1.0: the
//! cancelled job ran 11+ minutes on). The runner's own output is the event; no
//! poller is needed.

/// The message the forge answers once the registration is gone.
pub const UNREGISTERED: &str = "unauthenticated: unregistered runner";

/// Whether this output line says the run was cancelled.
#[must_use]
pub fn is_cancel_line(line: &str) -> bool {
    line.contains(UNREGISTERED)
}
