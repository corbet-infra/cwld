//! One lifecycle state machine per instance.
//!
//! The states and transitions are those of `CONTRACTS.md` section 6:
//! `Defined`, `Starting`, `Running`, `Stopping`, `Stopped` and `Failed`. A
//! transition the table does not name is a typed error, never a guess. The
//! machine has no `destroy`: `cwld` never destroys on its own, and a stop
//! timeout ends in `Failed` with an event that says a human is needed.
//!
//! One transition goes beyond the table: `Stopped` + `Start` → `Starting`. The
//! owner's rule is that a persistent instance (`desktop`, `dev`, `office`) that
//! was stopped starts again; only ephemeral classes go through a reset to
//! `Defined` first, which the runtime does before it asks the machine to
//! start.
//!
//! Every accepted transition emits one `cdrv` event. An observation from
//! reconcile (`Observe`) is legal from any state: it adopts what the driver
//! reports and emits `instance.drift` only when that differs from the state
//! the machine held.

use cdrv::{Event, EventLog, InstanceId, InstanceState, Severity, Subject, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

/// The reason recorded in a stop-timeout failure.
const STOP_TIMEOUT: &str = "the stop timed out; a human or a reboot is needed";

/// What moves an instance from one state to the next.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// The instance was defined from a spec.
    Define,
    /// A start was requested after `ensure_clean`.
    Start,
    /// The guest agent or SSH answered: the guest is ready.
    Ready,
    /// A start failed (readiness timeout or driver error), with the reason.
    Fail(String),
    /// A clean stop was requested.
    Stop,
    /// The driver observed a crash, with its reason.
    Crash(String),
    /// The guest is off after a clean stop.
    GuestOff,
    /// A clean stop did not finish in time.
    StopTimeout,
    /// A reset or cleanup returned a stopped instance to defined.
    Reset,
    /// A reconcile pass reports the state the driver observes.
    Observe(InstanceState),
}

impl Trigger {
    /// The trigger's name, for error and event text.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Define => "define",
            Self::Start => "start",
            Self::Ready => "ready",
            Self::Fail(_) => "fail",
            Self::Stop => "stop",
            Self::Crash(_) => "crash",
            Self::GuestOff => "guest_off",
            Self::StopTimeout => "stop_timeout",
            Self::Reset => "reset",
            Self::Observe(_) => "observe",
        }
    }
}

/// An illegal transition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransitionError {
    /// The trigger is not named for the state the instance is in.
    #[error("illegal transition for {instance}: {trigger} from {from:?}")]
    Illegal {
        /// The instance.
        instance: InstanceId,
        /// The trigger that was refused.
        trigger: &'static str,
        /// The state it was refused in; `None` means never defined.
        from: Option<InstanceState>,
    },
}

/// The lifecycle of one instance.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Lifecycle {
    state: Option<InstanceState>,
}

impl Lifecycle {
    /// A machine for an instance nothing has observed or defined yet.
    #[must_use]
    pub const fn unobserved() -> Self {
        Self { state: None }
    }

    /// A machine seeded with an observed state.
    #[must_use]
    pub fn observed(state: InstanceState) -> Self {
        Self { state: Some(state) }
    }

    /// The state the machine holds; `None` means never defined or observed.
    #[must_use]
    pub const fn state(&self) -> Option<&InstanceState> {
        self.state.as_ref()
    }

    /// Apply one trigger, emitting the transition's event.
    ///
    /// Returns `Ok(None)` when an `Observe` confirms the state the machine
    /// already holds: nothing changed, so nothing is emitted.
    pub fn advance(
        &mut self,
        trigger: Trigger,
        log: &mut EventLog,
        at: Timestamp,
        instance: &InstanceId,
    ) -> Result<Option<Event>, TransitionError> {
        if let Trigger::Observe(observed) = &trigger {
            let previous = self.state.clone();
            let changed = previous.as_ref().is_some_and(|state| state != observed);
            self.state = Some(observed.clone());
            if !changed {
                return Ok(None);
            }
            return Ok(Some(log.emit(
                at,
                "instance.drift",
                Subject::Instance(instance.clone()),
                Severity::Warn,
                json!({"from": previous, "to": observed}),
            )));
        }
        let current = self.state.clone();
        let (next, kind, severity, data) = match (current.as_ref(), &trigger) {
            (None, Trigger::Define) => (
                InstanceState::Defined,
                "instance.defined",
                Severity::Info,
                json!({}),
            ),
            (Some(InstanceState::Defined), Trigger::Start)
            | (Some(InstanceState::Stopped), Trigger::Start) => (
                InstanceState::Starting,
                "instance.starting",
                Severity::Info,
                json!({}),
            ),
            (Some(InstanceState::Starting), Trigger::Ready) => (
                InstanceState::Running,
                "instance.running",
                Severity::Info,
                json!({}),
            ),
            (Some(InstanceState::Starting), Trigger::Fail(reason)) => (
                InstanceState::Failed(reason.clone()),
                "instance.failed",
                Severity::Error,
                json!({"reason": "start_failed", "detail": reason}),
            ),
            (Some(InstanceState::Running), Trigger::Stop) => (
                InstanceState::Stopping,
                "instance.stopping",
                Severity::Info,
                json!({}),
            ),
            (Some(InstanceState::Running), Trigger::Crash(detail)) => (
                InstanceState::Failed(detail.clone()),
                "instance.failed",
                Severity::Error,
                json!({"reason": "crashed", "detail": detail}),
            ),
            (Some(InstanceState::Stopping), Trigger::GuestOff) => (
                InstanceState::Stopped,
                "instance.stopped",
                Severity::Info,
                json!({}),
            ),
            (Some(InstanceState::Stopping), Trigger::StopTimeout) => (
                InstanceState::Failed(STOP_TIMEOUT.to_owned()),
                "instance.failed",
                Severity::Error,
                json!({"reason": "stop_timeout", "needs_human": true, "detail": STOP_TIMEOUT}),
            ),
            (Some(InstanceState::Stopped), Trigger::Reset) => (
                InstanceState::Defined,
                "instance.reset",
                Severity::Info,
                json!({}),
            ),
            _ => {
                return Err(TransitionError::Illegal {
                    instance: instance.clone(),
                    trigger: trigger.name(),
                    from: current,
                });
            }
        };
        self.state = Some(next);
        Ok(Some(log.emit(
            at,
            kind,
            Subject::Instance(instance.clone()),
            severity,
            data,
        )))
    }
}
