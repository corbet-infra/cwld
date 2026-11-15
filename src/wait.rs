//! Waiting for the lifecycle event a driver pushes.
//!
//! `cwld` does not poll. The driver pushes lifecycle events
//! ([`HostInstances::events`]) and the wait consumes them until the instance
//! reaches the state the lifecycle machine expects. The state is read once
//! before the stream is opened, because the event may already have happened,
//! and never again in the wait.
//!
//! A driver's lifecycle event addresses its instance through
//! [`cdrv::Subject::Instance`] and uses one of the dotted kinds
//! `instance.defined`, `instance.starting`, `instance.running`,
//! `instance.stopping`, `instance.stopped` and `instance.failed`; a failure's
//! text is in `data.detail` (or `data.reason`). Any other event on the stream
//! is ignored.
//!
//! The deadline is measured on the contract's one clock, the same clock the
//! events carry: an event that reports a state but arrives after the deadline
//! ends the wait as a timeout. A driver-reported `Failed` ends the wait at
//! once, and a stream that ends without the wanted state is a typed error:
//! nothing is retried blindly and nothing is destroyed. Tests drive a fake
//! stream and a fake [`Clock`], so no test waits on wall time.

use std::time::Duration;

use cdrv::{Event, HostInstances, InstanceId, InstanceState, Subject, Timestamp};
use serde_json::Value;
use thiserror::Error;

/// The clock a wait's deadline is measured on.
///
/// The contract keeps one clock for every component ([`Timestamp`]); a driver's
/// events carry it, and the runtime compares them with it.
pub trait Clock {
    /// The instant now.
    fn now(&mut self) -> Timestamp;
}

/// The system clock: milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&mut self) -> Timestamp {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        Timestamp::from_millis(millis)
    }
}

/// The runtime's waiting policy, used when an instance has no class rule to
/// take a timeout from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitPolicy {
    /// How long a start may take before it counts as failed.
    pub readiness_timeout: Duration,
    /// How long a clean stop may take before a human is needed.
    pub stop_timeout: Duration,
}

impl Default for WaitPolicy {
    fn default() -> Self {
        Self {
            readiness_timeout: Duration::from_secs(300),
            stop_timeout: Duration::from_secs(180),
        }
    }
}

/// Why a wait ended without the wanted state.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WaitError {
    /// The driver could not report the instance's state or open its event
    /// stream.
    #[error("cannot follow {instance}: {message}")]
    Driver {
        /// The instance.
        instance: InstanceId,
        /// The driver's message.
        message: String,
    },
    /// The deadline passed before the wanted state arrived.
    #[error("{instance} did not reach {want} within {timeout_secs}s (last observed {last:?})")]
    Timeout {
        /// The instance.
        instance: InstanceId,
        /// The state that was wanted.
        want: &'static str,
        /// The timeout, in seconds.
        timeout_secs: u64,
        /// The last state the driver reported.
        last: InstanceState,
    },
    /// The driver reported a failure while the instance was being waited for.
    #[error("{instance} reported failed while waiting to be {want}: {reason}")]
    Failed {
        /// The instance.
        instance: InstanceId,
        /// The state that was wanted.
        want: &'static str,
        /// The driver's failure reason.
        reason: String,
    },
    /// The driver's event stream ended before the wanted state arrived.
    #[error("the driver's event stream ended before {instance} was {want}")]
    Ended {
        /// The instance.
        instance: InstanceId,
        /// The state that was wanted.
        want: &'static str,
    },
}

/// Read the state once, then follow the driver's events until `ready` accepts
/// one, the deadline passes, or the stream ends.
///
/// The single state read covers the event that already happened; after that
/// only pushed events advance the wait. A driver-reported `Failed` ends the
/// wait at once, so the caller can record the failure instead of waiting for a
/// timeout that would hide it.
pub(crate) fn wait_for(
    instances: &dyn HostInstances,
    instance: &InstanceId,
    want: &'static str,
    ready: impl Fn(&InstanceState) -> bool,
    timeout: Duration,
    clock: &mut dyn Clock,
) -> Result<InstanceState, WaitError> {
    let observed = instances
        .state(instance)
        .map_err(|error| WaitError::Driver {
            instance: instance.clone(),
            message: error.to_string(),
        })?;
    if ready(&observed) {
        return Ok(observed);
    }
    if let InstanceState::Failed(reason) = &observed {
        return Err(WaitError::Failed {
            instance: instance.clone(),
            want,
            reason: reason.clone(),
        });
    }
    let deadline = clock
        .now()
        .as_millis()
        .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX));
    let events = instances.events().map_err(|error| WaitError::Driver {
        instance: instance.clone(),
        message: error.to_string(),
    })?;
    let mut last = observed;
    for event in events {
        if event.at.as_millis() > deadline {
            return Err(WaitError::Timeout {
                instance: instance.clone(),
                want,
                timeout_secs: timeout.as_secs(),
                last,
            });
        }
        let Some(state) = lifecycle_state(&event, instance) else {
            continue;
        };
        if ready(&state) {
            return Ok(state);
        }
        if let InstanceState::Failed(reason) = &state {
            return Err(WaitError::Failed {
                instance: instance.clone(),
                want,
                reason: reason.clone(),
            });
        }
        last = state;
    }
    Err(WaitError::Ended {
        instance: instance.clone(),
        want,
    })
}

/// The state a driver's lifecycle event reports for `instance`, if any.
fn lifecycle_state(event: &Event, instance: &InstanceId) -> Option<InstanceState> {
    if event.subject != Subject::Instance(instance.clone()) {
        return None;
    }
    match event.kind.as_str() {
        "instance.defined" => Some(InstanceState::Defined),
        "instance.starting" => Some(InstanceState::Starting),
        "instance.running" => Some(InstanceState::Running),
        "instance.stopping" => Some(InstanceState::Stopping),
        "instance.paused" => Some(InstanceState::Paused),
        "instance.stopped" => Some(InstanceState::Stopped),
        "instance.failed" => Some(InstanceState::Failed(failure_text(&event.data))),
        _ => None,
    }
}

fn failure_text(data: &Value) -> String {
    data.get("detail")
        .or_else(|| data.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("the driver reported a failure")
        .to_owned()
}
