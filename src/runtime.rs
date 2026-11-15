//! The runtime: specs, lifecycles, dirty marks and the plan journal in one
//! place.
//!
//! `Runtime` is the object `cwld` drives. It holds the class data it was given,
//! the specs of the instances it manages, one lifecycle per instance, the
//! dirty marks and the plan journals. It executes; every decision (which
//! instance starts, who gets the card, whether memory is admitted) comes from
//! `cdct` and arrives as a [`cdct::SwitchPlan`] or as a request from the owner.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use cdrv::{
    DeviceAddress, Event, EventLog, InstanceClass, InstanceId, InstanceSpec, InstanceState,
    Severity, StopMode, Subject, Timestamp,
};
use serde_json::json;
use thiserror::Error;

use crate::classes::ClassCatalog;
use crate::dirty::{DirtyMark, DirtyMarks, DirtyReason};
use crate::journal::Journals;
use crate::lifecycle::{Lifecycle, TransitionError, Trigger};
use crate::plan::{Hosts, StepError};
use crate::store::StoreError;
use crate::wait::{RealSleeper, Sleeper, WaitError, WaitPolicy, wait_for};

/// A runtime failure that is not a failed plan step.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A dirty mark or a plan journal could not be read or written.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A lifecycle transition was refused.
    #[error(transparent)]
    Transition(#[from] TransitionError),
    /// The instance has no registered spec, so its class is unknown.
    #[error("instance {instance} has no registered spec, so its class is unknown")]
    UnknownInstance {
        /// The instance.
        instance: InstanceId,
    },
    /// A reset was needed but no storage port is attached.
    #[error("cannot reset {instance}: no disk-reset port is attached")]
    NoDiskReset {
        /// The instance.
        instance: InstanceId,
    },
    /// The storage driver refused the reset.
    #[error("resetting {instance} failed: {message}")]
    ResetFailed {
        /// The instance.
        instance: InstanceId,
        /// The driver's message.
        message: String,
    },
    /// A CI job was asked for on an instance that is not in the CI class.
    #[error("{instance} is a {class:?} instance, not a CI instance")]
    NotACiInstance {
        /// The instance.
        instance: InstanceId,
        /// The class it actually has.
        class: InstanceClass,
    },
}

/// What `ensure_clean` found and did.
#[derive(Debug, Clone, PartialEq)]
pub enum CleanOutcome {
    /// No dirty mark: nothing to do.
    Clean,
    /// An ephemeral instance's disk was reset to its clean snapshot.
    Reset {
        /// The mark that justified the reset.
        mark: DirtyMark,
        /// The events the reset emitted.
        events: Vec<Event>,
    },
    /// A persistent instance is dirty: reported, never reset automatically.
    Reported {
        /// The mark.
        mark: DirtyMark,
        /// The warning event.
        event: Event,
    },
}

impl CleanOutcome {
    /// The events this outcome emitted.
    #[must_use]
    pub fn into_events(self) -> Vec<Event> {
        match self {
            Self::Clean => Vec::new(),
            Self::Reset { events, .. } => events,
            Self::Reported { event, .. } => vec![event],
        }
    }

    /// Whether the instance may start now.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        matches!(self, Self::Clean | Self::Reset { .. })
    }
}

/// The `cwld` runtime.
pub struct Runtime {
    classes: ClassCatalog,
    wait: WaitPolicy,
    gpu: Option<DeviceAddress>,
    marks: DirtyMarks,
    journals: Journals,
    specs: BTreeMap<InstanceId, InstanceSpec>,
    lifecycles: BTreeMap<InstanceId, Lifecycle>,
    sleeper: Box<dyn Sleeper>,
}

impl Runtime {
    /// Open the runtime state under `state_dir` with the given class data.
    pub fn new(state_dir: impl AsRef<Path>, classes: ClassCatalog) -> Result<Self, StoreError> {
        let root = state_dir.as_ref();
        Ok(Self {
            classes,
            wait: WaitPolicy::default(),
            gpu: None,
            marks: DirtyMarks::new(root.join("dirty"))?,
            journals: Journals::new(root.join("plans"))?,
            specs: BTreeMap::new(),
            lifecycles: BTreeMap::new(),
            sleeper: Box::new(RealSleeper),
        })
    }

    /// Set the GPU the handover steps bind and unbind.
    #[must_use]
    pub fn with_gpu(mut self, device: DeviceAddress) -> Self {
        self.gpu = Some(device);
        self
    }

    /// Replace the sleeper the waits use.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Box<dyn Sleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Replace the waiting policy used for instances without class rules.
    #[must_use]
    pub fn with_wait_policy(mut self, wait: WaitPolicy) -> Self {
        self.wait = wait;
        self
    }

    /// The class data the runtime executes.
    #[must_use]
    pub const fn classes(&self) -> &ClassCatalog {
        &self.classes
    }

    /// The dirty marks.
    #[must_use]
    pub const fn marks(&self) -> &DirtyMarks {
        &self.marks
    }

    /// The plan journals.
    #[must_use]
    pub const fn journals(&self) -> &Journals {
        &self.journals
    }

    /// The GPU address the handover steps use, when one is configured.
    #[must_use]
    pub const fn gpu(&self) -> Option<&DeviceAddress> {
        self.gpu.as_ref()
    }

    /// One instance's spec, when it is registered.
    #[must_use]
    pub fn spec(&self, instance: &InstanceId) -> Option<&InstanceSpec> {
        self.specs.get(instance)
    }

    /// One instance's class, when its spec is registered.
    #[must_use]
    pub fn class_of(&self, instance: &InstanceId) -> Option<InstanceClass> {
        self.specs.get(instance).map(|spec| spec.class)
    }

    /// The lifecycle state the runtime holds, when it has seen the instance.
    #[must_use]
    pub fn state(&self, instance: &InstanceId) -> Option<&InstanceState> {
        self.lifecycles.get(instance).and_then(Lifecycle::state)
    }

    /// Register an instance spec without changing its lifecycle.
    ///
    /// A runtime started after a crash registers its specs again before it
    /// reports the dirty marks; registration is configuration, not an
    /// observation.
    pub fn register(&mut self, spec: InstanceSpec) {
        self.specs.insert(spec.id.clone(), spec);
    }

    /// Define an instance: register its spec and move it to `Defined`.
    ///
    /// Defining an instance that is already `Defined` is idempotent and emits
    /// nothing. Returns the `instance.defined` event when one was emitted.
    pub fn define(
        &mut self,
        spec: InstanceSpec,
        log: &mut EventLog,
        at: Timestamp,
    ) -> Result<Option<Event>, RuntimeError> {
        let instance = spec.id.clone();
        self.specs.insert(instance.clone(), spec);
        let lifecycle = self.lifecycle_mut(&instance);
        if lifecycle.state() == Some(&InstanceState::Defined) {
            return Ok(None);
        }
        Ok(lifecycle.advance(Trigger::Define, log, at, &instance)?)
    }

    /// Make sure an instance may start.
    ///
    /// An ephemeral instance with a dirty mark has its disk reset to clean
    /// first, through the [`crate::ports::DiskReset`] port. A persistent
    /// instance is never reset automatically: a dirty persistent disk is
    /// reported for a human, and the runtime does not silently discard it.
    pub fn ensure_clean(
        &mut self,
        instance: &InstanceId,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
    ) -> Result<CleanOutcome, RuntimeError> {
        let ephemeral = self
            .class_of(instance)
            .is_some_and(|class| self.classes.is_ephemeral(class));
        self.ensure_clean_with(instance, ephemeral, hosts, log, at)
    }

    fn ensure_clean_with(
        &mut self,
        instance: &InstanceId,
        ephemeral: bool,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
    ) -> Result<CleanOutcome, RuntimeError> {
        let Some(mark) = self.marks.get(instance)? else {
            return Ok(CleanOutcome::Clean);
        };
        if !ephemeral {
            let event = log.emit(
                at,
                "instance.dirty",
                Subject::Instance(instance.clone()),
                Severity::Warn,
                json!({
                    "reason": mark.reason,
                    "at": mark.at.as_millis(),
                    "detail": mark.detail,
                    "action": "reported only; a persistent disk is never reset automatically",
                }),
            );
            return Ok(CleanOutcome::Reported { mark, event });
        }
        let reset = hosts.disk_reset.ok_or_else(|| RuntimeError::NoDiskReset {
            instance: instance.clone(),
        })?;
        reset
            .reset_to_clean(instance)
            .map_err(|error| RuntimeError::ResetFailed {
                instance: instance.clone(),
                message: error.to_string(),
            })?;
        self.marks.clear(instance)?;
        let mut events = Vec::new();
        if self.state(instance) == Some(&InstanceState::Stopped) {
            if let Some(event) =
                self.lifecycle_mut(instance)
                    .advance(Trigger::Reset, log, at, instance)?
            {
                events.push(event);
            }
        } else {
            events.push(log.emit(
                at,
                "instance.reset",
                Subject::Instance(instance.clone()),
                Severity::Info,
                json!({"source": "ensure_clean"}),
            ));
        }
        Ok(CleanOutcome::Reset { mark, events })
    }

    fn readiness_timeout_for(&self, instance: &InstanceId) -> Duration {
        self.class_of(instance)
            .map_or(self.wait.readiness_timeout, |class| {
                self.classes.rules(class).readiness_timeout()
            })
    }

    fn stop_timeout_for(&self, instance: &InstanceId) -> Duration {
        self.class_of(instance)
            .map_or(self.wait.stop_timeout, |class| {
                self.classes.rules(class).stop_timeout()
            })
    }

    fn lifecycle_mut(&mut self, instance: &InstanceId) -> &mut Lifecycle {
        self.lifecycles
            .entry(instance.clone())
            .or_insert_with(Lifecycle::unobserved)
    }

    /// Start one instance through the driver, driving its lifecycle.
    pub(crate) fn start_instance(
        &mut self,
        instance: &InstanceId,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
        events: &mut Vec<Event>,
    ) -> Result<(), StepError> {
        let ephemeral = self
            .class_of(instance)
            .is_some_and(|class| self.classes.is_ephemeral(class));
        let observed = hosts
            .instances
            .state(instance)
            .map_err(|error| StepError::Driver {
                action: format!("read the state of {instance}"),
                message: error.to_string(),
            })?;
        if let Some(event) = self.lifecycle_mut(instance).advance(
            Trigger::Observe(observed.clone()),
            log,
            at,
            instance,
        )? {
            events.push(event);
        }
        // Never touch a live instance's disk: if it is already starting,
        // running or stopping, the `Start` trigger below is refused instead.
        if !observed.is_live() {
            match self.ensure_clean_with(instance, ephemeral, hosts, log, at) {
                Ok(outcome) => events.extend(outcome.into_events()),
                Err(error) => {
                    return Err(StepError::Clean {
                        instance: instance.clone(),
                        message: error.to_string(),
                    });
                }
            }
            // An ephemeral clone always starts from clean, even when no dirty
            // mark survived: a stopped clone is reset and returns to defined.
            if ephemeral && self.state(instance) == Some(&InstanceState::Stopped) {
                self.reset_instance(instance, hosts, log, at, events)
                    .map_err(|error| StepError::Clean {
                        instance: instance.clone(),
                        message: error.to_string(),
                    })?;
            }
        }
        if let Some(event) =
            self.lifecycle_mut(instance)
                .advance(Trigger::Start, log, at, instance)?
        {
            events.push(event);
        }
        self.marks.mark(
            instance,
            DirtyReason::Starting,
            at,
            Some("start requested".to_owned()),
        )?;
        hosts
            .instances
            .start(instance)
            .map_err(|error| StepError::Driver {
                action: format!("start {instance}"),
                message: error.to_string(),
            })?;
        let interval = self.wait.poll_interval;
        let timeout = self.readiness_timeout_for(instance);
        match wait_for(
            hosts.instances,
            instance,
            "running",
            |state| matches!(state, InstanceState::Running),
            timeout,
            interval,
            self.sleeper.as_mut(),
        ) {
            Ok(_) => {
                if let Some(event) =
                    self.lifecycle_mut(instance)
                        .advance(Trigger::Ready, log, at, instance)?
                {
                    events.push(event);
                }
                Ok(())
            }
            Err(WaitError::Timeout { .. }) => {
                if let Some(event) = self.lifecycle_mut(instance).advance(
                    Trigger::Fail("readiness timed out".to_owned()),
                    log,
                    at,
                    instance,
                )? {
                    events.push(event);
                }
                Err(StepError::ReadinessTimeout {
                    instance: instance.clone(),
                    timeout_secs: timeout.as_secs(),
                })
            }
            Err(WaitError::Failed { reason, .. }) => {
                if let Some(event) = self.lifecycle_mut(instance).advance(
                    Trigger::Observe(InstanceState::Failed(reason.clone())),
                    log,
                    at,
                    instance,
                )? {
                    events.push(event);
                }
                Err(StepError::ObservedFailed {
                    instance: instance.clone(),
                    during: "starting",
                    reason,
                })
            }
            Err(WaitError::Driver { message, .. }) => Err(StepError::Driver {
                action: format!("read the state of {instance}"),
                message,
            }),
        }
    }

    /// Stop one instance cleanly through the driver, driving its lifecycle.
    pub(crate) fn stop_instance(
        &mut self,
        instance: &InstanceId,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
        events: &mut Vec<Event>,
    ) -> Result<(), StepError> {
        let observed = hosts
            .instances
            .state(instance)
            .map_err(|error| StepError::Driver {
                action: format!("read the state of {instance}"),
                message: error.to_string(),
            })?;
        if let Some(event) = self.lifecycle_mut(instance).advance(
            Trigger::Observe(observed.clone()),
            log,
            at,
            instance,
        )? {
            events.push(event);
        }
        match observed {
            InstanceState::Stopped | InstanceState::Defined => return Ok(()),
            InstanceState::Failed(reason) => {
                return Err(StepError::ObservedFailed {
                    instance: instance.clone(),
                    during: "stopping",
                    reason,
                });
            }
            InstanceState::Running => {
                if let Some(event) =
                    self.lifecycle_mut(instance)
                        .advance(Trigger::Stop, log, at, instance)?
                {
                    events.push(event);
                }
            }
            InstanceState::Stopping => {}
            InstanceState::Starting => {
                // The contract's table has no stop from Starting; the refusal
                // is typed and the plan stops here.
                let _refused =
                    self.lifecycle_mut(instance)
                        .advance(Trigger::Stop, log, at, instance)?;
            }
        }
        self.marks.mark(
            instance,
            DirtyReason::Stopping,
            at,
            Some("stop requested".to_owned()),
        )?;
        hosts
            .instances
            .stop(instance, StopMode::Clean)
            .map_err(|error| StepError::Driver {
                action: format!("stop {instance}"),
                message: error.to_string(),
            })?;
        let interval = self.wait.poll_interval;
        let timeout = self.stop_timeout_for(instance);
        match wait_for(
            hosts.instances,
            instance,
            "stopped",
            |state| matches!(state, InstanceState::Stopped),
            timeout,
            interval,
            self.sleeper.as_mut(),
        ) {
            Ok(_) => {
                if self.state(instance) == Some(&InstanceState::Stopping) {
                    if let Some(event) = self.lifecycle_mut(instance).advance(
                        Trigger::GuestOff,
                        log,
                        at,
                        instance,
                    )? {
                        events.push(event);
                    }
                } else if let Some(event) = self.lifecycle_mut(instance).advance(
                    Trigger::Observe(InstanceState::Stopped),
                    log,
                    at,
                    instance,
                )? {
                    events.push(event);
                }
                self.marks.clear(instance)?;
                Ok(())
            }
            Err(WaitError::Timeout { .. }) => {
                if self.state(instance) == Some(&InstanceState::Stopping) {
                    if let Some(event) = self.lifecycle_mut(instance).advance(
                        Trigger::StopTimeout,
                        log,
                        at,
                        instance,
                    )? {
                        events.push(event);
                    }
                } else if let Some(event) = self.lifecycle_mut(instance).advance(
                    Trigger::Observe(InstanceState::Stopping),
                    log,
                    at,
                    instance,
                )? {
                    events.push(event);
                }
                Err(StepError::StopTimeout {
                    instance: instance.clone(),
                    timeout_secs: timeout.as_secs(),
                })
            }
            Err(WaitError::Failed { reason, .. }) => {
                if let Some(event) = self.lifecycle_mut(instance).advance(
                    Trigger::Observe(InstanceState::Failed(reason.clone())),
                    log,
                    at,
                    instance,
                )? {
                    events.push(event);
                }
                Err(StepError::ObservedFailed {
                    instance: instance.clone(),
                    during: "stopping",
                    reason,
                })
            }
            Err(WaitError::Driver { message, .. }) => Err(StepError::Driver {
                action: format!("read the state of {instance}"),
                message,
            }),
        }
    }

    /// Reset a stopped instance's disk to clean, for the CI job's last step.
    pub(crate) fn reset_instance(
        &mut self,
        instance: &InstanceId,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
        events: &mut Vec<Event>,
    ) -> Result<(), RuntimeError> {
        let reset = hosts.disk_reset.ok_or_else(|| RuntimeError::NoDiskReset {
            instance: instance.clone(),
        })?;
        reset
            .reset_to_clean(instance)
            .map_err(|error| RuntimeError::ResetFailed {
                instance: instance.clone(),
                message: error.to_string(),
            })?;
        self.marks.clear(instance)?;
        if self.state(instance) == Some(&InstanceState::Stopped) {
            if let Some(event) =
                self.lifecycle_mut(instance)
                    .advance(Trigger::Reset, log, at, instance)?
            {
                events.push(event);
            }
        } else {
            events.push(log.emit(
                at,
                "instance.reset",
                Subject::Instance(instance.clone()),
                Severity::Info,
                json!({"source": "job_cleanup"}),
            ));
        }
        Ok(())
    }
}
