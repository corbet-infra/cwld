//! Bounded waiting for a driver-reported instance state.
//!
//! `cwld` asks the driver for the state and waits for it to reach the state the
//! lifecycle machine expects. A driver-reported `Failed` ends the wait at once,
//! and exhausting the timeout is a typed error: nothing is retried blindly and
//! nothing is destroyed. The sleeping is behind [`Sleeper`], so tests wait
//! through a fake clock without spending wall-clock time.

use std::time::Duration;

use cdrv::{HostInstances, InstanceId, InstanceState};
use thiserror::Error;

/// How a wait advances between two state reads.
pub trait Sleeper {
    /// Wait for `duration`.
    fn nap(&mut self, duration: Duration);
}

/// A sleeper that really waits.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealSleeper;

impl Sleeper for RealSleeper {
    fn nap(&mut self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// A sleeper that returns immediately; for tests only.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSleeper;

impl Sleeper for NoSleeper {
    fn nap(&mut self, _duration: Duration) {}
}

/// The runtime's waiting policy, used when an instance has no class rule to
/// take a timeout from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitPolicy {
    /// How long to wait between two state reads.
    pub poll_interval: Duration,
    /// How long a start may take before it counts as failed.
    pub readiness_timeout: Duration,
    /// How long a clean stop may take before a human is needed.
    pub stop_timeout: Duration,
}

impl Default for WaitPolicy {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            readiness_timeout: Duration::from_secs(300),
            stop_timeout: Duration::from_secs(180),
        }
    }
}

/// Why a wait ended without the wanted state.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WaitError {
    /// The driver could not report the instance's state.
    #[error("cannot read the state of {instance}: {message}")]
    Driver {
        /// The instance.
        instance: InstanceId,
        /// The driver's message.
        message: String,
    },
    /// The timeout ran out first.
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
}

/// Read the state until `ready` accepts it, or the timeout runs out.
///
/// The first read happens immediately; the sleeper waits between later reads.
/// A driver-reported `Failed` ends the wait at once, so the caller can record
/// the failure instead of waiting for a timeout that would hide it.
pub(crate) fn wait_for(
    instances: &dyn HostInstances,
    instance: &InstanceId,
    want: &'static str,
    ready: impl Fn(&InstanceState) -> bool,
    timeout: Duration,
    interval: Duration,
    sleeper: &mut dyn Sleeper,
) -> Result<InstanceState, WaitError> {
    let rounds = attempts(timeout, interval);
    let mut last: Option<InstanceState> = None;
    for round in 0..rounds {
        let state = instances
            .state(instance)
            .map_err(|error| WaitError::Driver {
                instance: instance.clone(),
                message: error.to_string(),
            })?;
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
        last = Some(state);
        if round + 1 < rounds {
            sleeper.nap(interval);
        }
    }
    Err(WaitError::Timeout {
        instance: instance.clone(),
        want,
        timeout_secs: timeout.as_secs(),
        last: last.unwrap_or(InstanceState::Defined),
    })
}

fn attempts(timeout: Duration, interval: Duration) -> u64 {
    let timeout_ms = timeout.as_millis().max(1);
    let interval_ms = interval.as_millis().max(1);
    u64::try_from(timeout_ms.div_ceil(interval_ms))
        .unwrap_or(u64::MAX)
        .max(1)
}
