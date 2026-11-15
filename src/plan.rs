//! The plan executor: run a `cdct::SwitchPlan` step by step.
//!
//! The plan is data that `cdct` decided. `cwld` executes it in order and stops
//! at the first failed step: the report says which steps ran, which step failed
//! and which steps were never reached, so `cdct` can reconcile. A guest switch
//! that runs to the end records the plan's `displaced` tenants with the holder
//! table, the hand-off `cdct` documents.
//!
//! Every side effect goes through a trait: the `cdrv` host traits for
//! instances, devices, host GPU tenants and Sunshine, and the one port in
//! [`crate::ports`] for what `cdrv` does not define yet (the disk reset that
//! `cdsk` will own). `cwld` never runs `virsh`, `zfs`, `systemctl`, `kubectl`
//! or reads sysfs.

use cdct::{KernelOwner, PlanStep, SwitchPlan};
use cdrv::{
    DeviceAddress, DeviceFilter, Event, EventLog, HostDevices, HostGpu, HostInstances, InstanceId,
    Severity, StopMode, Subject, Timestamp,
};
use serde_json::json;
use thiserror::Error;

use crate::handoff::DisplacedHandoff;
use crate::lifecycle::TransitionError;
use crate::ports::DiskReset;
use crate::runtime::Runtime;
use crate::store::StoreError;

/// The host traits and ports one plan is executed against.
pub struct Hosts<'a> {
    /// Instance lifecycle on this host.
    pub instances: &'a dyn HostInstances,
    /// Exclusive devices on this host.
    pub devices: &'a dyn HostDevices,
    /// Host GPU tenants and the Sunshine service, when the host provides them.
    pub gpu: Option<&'a dyn HostGpu>,
    /// Disk reset to a clean snapshot, when the host provides it.
    pub disk_reset: Option<&'a dyn DiskReset>,
}

impl<'a> Hosts<'a> {
    /// The `cdrv` host traits a plan always needs.
    #[must_use]
    pub fn new(instances: &'a dyn HostInstances, devices: &'a dyn HostDevices) -> Self {
        Self {
            instances,
            devices,
            gpu: None,
            disk_reset: None,
        }
    }

    /// Attach the host GPU platform.
    #[must_use]
    pub fn with_gpu(mut self, gpu: &'a dyn HostGpu) -> Self {
        self.gpu = Some(gpu);
        self
    }

    /// Attach the disk-reset port.
    #[must_use]
    pub fn with_disk_reset(mut self, disk_reset: &'a dyn DiskReset) -> Self {
        self.disk_reset = Some(disk_reset);
        self
    }

    pub(crate) fn gpu(&self, action: &str) -> Result<&'a dyn HostGpu, StepError> {
        self.gpu.ok_or_else(|| StepError::NoPort {
            action: action.to_owned(),
            port: "HostGpu",
        })
    }
}

/// Why one plan step failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StepError {
    /// A driver call failed.
    #[error("{action} failed: {message}")]
    Driver {
        /// What was being done.
        action: String,
        /// The driver's message.
        message: String,
    },
    /// The lifecycle machine refused the transition the step asked for.
    #[error(transparent)]
    Transition(#[from] TransitionError),
    /// A step needs a port the caller did not attach.
    #[error("{action} needs the {port} port, which is not attached")]
    NoPort {
        /// What was being done.
        action: String,
        /// The port that is missing.
        port: &'static str,
    },
    /// A plan tried to stop an instance by hibernating it.
    #[error("a GPU handover never hibernates ({instance})")]
    HibernateForbidden {
        /// The instance.
        instance: InstanceId,
    },
    /// The instance did not become ready in time.
    #[error("{instance} was not ready within {timeout_secs}s")]
    ReadinessTimeout {
        /// The instance.
        instance: InstanceId,
        /// The readiness timeout, in seconds.
        timeout_secs: u64,
    },
    /// The instance did not stop in time; it is `Failed` and a human is needed.
    #[error("{instance} did not stop within {timeout_secs}s; a human or a reboot is needed")]
    StopTimeout {
        /// The instance.
        instance: InstanceId,
        /// The stop timeout, in seconds.
        timeout_secs: u64,
    },
    /// The driver reported a failure while the step waited.
    #[error("{instance} reported failed while {during}: {reason}")]
    ObservedFailed {
        /// The instance.
        instance: InstanceId,
        /// What the step was doing.
        during: &'static str,
        /// The driver's reason.
        reason: String,
    },
    /// `ensure_clean` could not make the instance safe to start.
    #[error("cannot make {instance} clean: {message}")]
    Clean {
        /// The instance.
        instance: InstanceId,
        /// What went wrong.
        message: String,
    },
    /// The hardware inventory names no PCI function for the card.
    #[error("the hardware inventory names no PCI functions for the card {device:?}")]
    UnknownDevice {
        /// The card's inventory name, from the plan.
        device: String,
    },
    /// A dirty mark or a plan journal could not be written.
    #[error("state store: {0}")]
    Store(String),
}

impl From<StoreError> for StepError {
    fn from(error: StoreError) -> Self {
        Self::Store(error.to_string())
    }
}

/// One step that failed, with its position in the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepFailure {
    /// The step's index in the plan.
    pub index: usize,
    /// The step itself.
    pub step: PlanStep,
    /// Why it failed.
    pub error: StepError,
}

/// Whether a successful guest switch recorded its displaced tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffStatus {
    /// The plan returned the card to the host; no guest holder to annotate.
    NotRequired,
    /// The plan stopped at a failed step, so nothing was handed off.
    NotAttempted,
    /// The displaced tenants are recorded in the holder table.
    Recorded,
    /// The plan ran but the hand-off could not be recorded.
    Failed(String),
}

/// What ran, what failed, and what was never reached.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanReport {
    /// The kernel the plan was switching the card to.
    pub target: KernelOwner,
    /// The steps that completed, in order.
    pub executed: Vec<PlanStep>,
    /// The first step that failed, when one did.
    pub failed: Option<StepFailure>,
    /// The steps after the failure that were never attempted.
    pub not_run: Vec<PlanStep>,
    /// The hand-off outcome for a guest switch.
    pub handoff: HandoffStatus,
    /// Every event the execution emitted.
    pub events: Vec<Event>,
}

impl PlanReport {
    /// Whether every step ran.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.failed.is_none()
    }
}

impl Runtime {
    /// Execute a plan in order, stopping at the first failed step.
    ///
    /// The only error is a failure to write the plan journal: a failed step is
    /// part of the returned [`PlanReport`], because `cdct` must see what ran
    /// and what did not rather than an opaque error.
    pub fn execute_plan<H: DisplacedHandoff>(
        &mut self,
        plan: &SwitchPlan,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
        handoff: &mut H,
    ) -> Result<PlanReport, StoreError> {
        let mut events = vec![log.emit(
            at,
            "plan.started",
            Subject::Host,
            Severity::Info,
            json!({"target": plan.target, "steps": plan.steps.len()}),
        )];
        let mut journal = self.journals().begin(plan, at)?;
        let mut executed = Vec::new();
        for (index, step) in plan.steps.iter().enumerate() {
            self.journals().before_step(&mut journal, index)?;
            let mut step_events = Vec::new();
            match self.run_step(step, hosts, log, at, &mut step_events) {
                Ok(()) => {
                    events.append(&mut step_events);
                    self.journals().after_step(&mut journal, index)?;
                    executed.push(step.clone());
                }
                Err(error) => {
                    events.append(&mut step_events);
                    self.journals().finish(&journal)?;
                    events.push(log.emit(
                        at,
                        "plan.step_failed",
                        Subject::Host,
                        Severity::Error,
                        json!({
                            "target": plan.target,
                            "index": index,
                            "step": step,
                            "error": error.to_string(),
                        }),
                    ));
                    return Ok(PlanReport {
                        target: plan.target.clone(),
                        executed,
                        failed: Some(StepFailure {
                            index,
                            step: step.clone(),
                            error,
                        }),
                        not_run: plan.steps[index + 1..].to_vec(),
                        handoff: HandoffStatus::NotAttempted,
                        events,
                    });
                }
            }
        }
        self.journals().finish(&journal)?;
        let handoff_status = if plan.target.is_host() {
            HandoffStatus::NotRequired
        } else {
            match handoff.record(plan) {
                Ok(()) => HandoffStatus::Recorded,
                Err(error) => {
                    events.push(log.emit(
                        at,
                        "plan.handoff_failed",
                        Subject::Host,
                        Severity::Error,
                        json!({"target": plan.target, "error": error.to_string()}),
                    ));
                    HandoffStatus::Failed(error.to_string())
                }
            }
        };
        events.push(log.emit(
            at,
            "plan.completed",
            Subject::Host,
            Severity::Info,
            json!({"target": plan.target, "steps": plan.steps.len()}),
        ));
        Ok(PlanReport {
            target: plan.target.clone(),
            executed,
            failed: None,
            not_run: Vec::new(),
            handoff: handoff_status,
            events,
        })
    }

    fn run_step(
        &mut self,
        step: &PlanStep,
        hosts: &Hosts<'_>,
        log: &mut EventLog,
        at: Timestamp,
        events: &mut Vec<Event>,
    ) -> Result<(), StepError> {
        match step {
            PlanStep::StopHostTenant { tenant, ladder } => {
                let action = format!("stop host tenant {tenant}");
                hosts
                    .gpu(&action)?
                    .stop_tenant(tenant, *ladder)
                    .map_err(|error| StepError::Driver {
                        action,
                        message: error.to_string(),
                    })
            }
            PlanStep::StartHostTenant { tenant, ladder } => {
                let action = format!("start host tenant {tenant}");
                hosts
                    .gpu(&action)?
                    .start_tenant(tenant, *ladder)
                    .map_err(|error| StepError::Driver {
                        action,
                        message: error.to_string(),
                    })
            }
            PlanStep::StopInstance { instance, mode } => {
                if *mode != StopMode::Clean {
                    return Err(StepError::HibernateForbidden {
                        instance: instance.clone(),
                    });
                }
                self.stop_instance(instance, hosts, log, at, events)
            }
            PlanStep::HandOverCard { device, from, to } => hand_over(device, from, to, hosts),
            PlanStep::StartInstance { instance } => {
                self.start_instance(instance, hosts, log, at, events)
            }
            PlanStep::SunshineFollows { kernel } => {
                let action = format!("move Sunshine to {kernel}");
                hosts
                    .gpu(&action)?
                    .sunshine_follows(kernel)
                    .map_err(|error| StepError::Driver {
                        action,
                        message: error.to_string(),
                    })
            }
        }
    }
}

/// Hand the card the plan names over between kernels.
///
/// The plan names the card as the hardware inventory does; the inventory
/// resolves that name to every PCI function the card presents (a graphics card
/// is its GPU function plus the audio and USB functions beside it). Each
/// function is unbound and reset when the card leaves a guest, and bound when
/// it goes to one. A name that resolves to nothing is a typed error, never a
/// guessed address.
fn hand_over(
    device: &str,
    from: &KernelOwner,
    to: &KernelOwner,
    hosts: &Hosts<'_>,
) -> Result<(), StepError> {
    let functions = card_functions(device, hosts)?;
    for address in &functions {
        let label = device_label(*address);
        if from.guest().is_some() {
            hosts
                .devices
                .unbind_passthrough(address)
                .map_err(|error| StepError::Driver {
                    action: format!("unbind {label} from passthrough"),
                    message: error.to_string(),
                })?;
            hosts
                .devices
                .reset(address)
                .map_err(|error| StepError::Driver {
                    action: format!("reset {label}"),
                    message: error.to_string(),
                })?;
        }
        if to.guest().is_some() {
            hosts
                .devices
                .bind_passthrough(address)
                .map_err(|error| StepError::Driver {
                    action: format!("bind {label} to passthrough"),
                    message: error.to_string(),
                })?;
        }
    }
    Ok(())
}

/// Resolve a card's inventory name to the PCI functions it presents.
fn card_functions(device: &str, hosts: &Hosts<'_>) -> Result<Vec<DeviceAddress>, StepError> {
    let filter = DeviceFilter {
        class: None,
        name: Some(device.to_owned()),
    };
    let facts = hosts
        .devices
        .enumerate(filter)
        .map_err(|error| StepError::Driver {
            action: format!("enumerate the functions of {device}"),
            message: error.to_string(),
        })?;
    let mut addresses: Vec<DeviceAddress> =
        facts.iter().filter_map(|facts| facts.address).collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(StepError::UnknownDevice {
            device: device.to_owned(),
        });
    }
    Ok(addresses)
}

fn device_label(device: DeviceAddress) -> String {
    format!(
        "{:04x}:{:02x}:{:02x}.{}",
        device.domain, device.bus, device.slot, device.function
    )
}
