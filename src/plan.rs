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
        // The replica counts are read BEFORE the first tenant is stopped and recorded durably
        // with the holder: a crash after a stop would otherwise restore one replica instead
        // of the original count. Nothing has been stopped yet when this fails.
        let counted;
        let plan = if plan
            .steps
            .iter()
            .any(|step| matches!(step, PlanStep::StopHostTenant { .. }))
        {
            match Self::count_displaced(plan, hosts, handoff) {
                Ok(with_counts) => {
                    counted = with_counts;
                    &counted
                }
                Err(error) => {
                    events.push(log.emit(
                        at,
                        "plan.step_failed",
                        Subject::Host,
                        Severity::Error,
                        json!({"target": plan.target, "index": 0, "error": error.to_string()}),
                    ));
                    let first = plan.steps[0].clone();
                    return Ok(PlanReport {
                        target: plan.target.clone(),
                        executed: Vec::new(),
                        failed: Some(StepFailure {
                            index: 0,
                            step: first,
                            error,
                        }),
                        not_run: plan.steps.clone(),
                        handoff: HandoffStatus::NotAttempted,
                        events,
                    });
                }
            }
        } else {
            plan
        };
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
                    events.push(step_failed(log, at, &plan.target, index, step, &error));
                    // Once the card is back on the host, one tenant that fails to
                    // start must not leave the others down: every remaining
                    // restore still runs, and each failure is reported. Before
                    // that point the plan stops at the first failure.
                    let mut not_run = plan.steps[index + 1..].to_vec();
                    if plan.target.is_host() && is_restore(step) {
                        for (later_index, later) in plan.steps.iter().enumerate().skip(index + 1) {
                            self.journals().before_step(&mut journal, later_index)?;
                            let mut later_events = Vec::new();
                            let outcome = self.run_step(later, hosts, log, at, &mut later_events);
                            events.append(&mut later_events);
                            match outcome {
                                Ok(()) => {
                                    self.journals().after_step(&mut journal, later_index)?;
                                    executed.push(later.clone());
                                }
                                Err(later_error) => events.push(step_failed(
                                    log,
                                    at,
                                    &plan.target,
                                    later_index,
                                    later,
                                    &later_error,
                                )),
                            }
                        }
                        not_run.clear();
                    }
                    self.journals().finish(&journal)?;
                    return Ok(PlanReport {
                        target: plan.target.clone(),
                        executed,
                        failed: Some(StepFailure {
                            index,
                            step: step.clone(),
                            error,
                        }),
                        not_run,
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

    /// The plan with every displaced tenant's live replica count filled in, after the
    /// counts were handed to the hand-off (a guest target only; a host target restores
    /// the stored counts and has nothing to read).
    ///
    /// A tenant read at zero replicas holds nothing on the card: it is not displaced,
    /// its stop step is dropped, and the switch back does not start it (a start with
    /// zero replicas is refused by the driver).
    fn count_displaced<H: DisplacedHandoff>(
        plan: &SwitchPlan,
        hosts: &Hosts<'_>,
        handoff: &mut H,
    ) -> Result<SwitchPlan, StepError> {
        let mut counted = plan.clone();
        let action = "read the replica counts of the displaced tenants".to_owned();
        for displaced in &mut counted.displaced {
            displaced.replicas = hosts
                .gpu(&action)?
                .tenant_replicas(&displaced.tenant, displaced.ladder)
                .map_err(|error| StepError::Driver {
                    action: format!("read the replicas of host tenant {}", displaced.tenant),
                    message: error.to_string(),
                })?;
        }
        let idle: Vec<String> = counted
            .displaced
            .iter()
            .filter(|displaced| displaced.replicas == 0)
            .map(|displaced| displaced.tenant.clone())
            .collect();
        counted.displaced.retain(|displaced| displaced.replicas > 0);
        counted.steps.retain(|step| {
            !matches!(step, PlanStep::StopHostTenant { tenant, .. } if idle.contains(tenant))
        });
        if !counted.target.is_host() {
            handoff
                .record(&counted)
                .map_err(|error| StepError::Driver {
                    action: "record the displaced tenants before stopping them".to_owned(),
                    message: error.to_string(),
                })?;
        }
        Ok(counted)
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
            PlanStep::StartHostTenant {
                tenant,
                ladder,
                replicas,
            } => {
                let action = format!("start host tenant {tenant}");
                hosts
                    .gpu(&action)?
                    .start_tenant(tenant, *ladder, *replicas)
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

/// Whether a step restores the host after the card is back on it.
const fn is_restore(step: &PlanStep) -> bool {
    matches!(
        step,
        PlanStep::StartHostTenant { .. } | PlanStep::SunshineFollows { .. }
    )
}

fn step_failed(
    log: &mut EventLog,
    at: Timestamp,
    target: &KernelOwner,
    index: usize,
    step: &PlanStep,
    error: &StepError,
) -> Event {
    log.emit(
        at,
        "plan.step_failed",
        Subject::Host,
        Severity::Error,
        json!({
            "target": target,
            "index": index,
            "step": step,
            "error": error.to_string(),
        }),
    )
}

/// Hand the card the plan names over between kernels.
///
/// The plan names the card as the hardware inventory does; the inventory
/// resolves that name to the PCI functions the card presents (a graphics card
/// is its GPU function plus the audio and USB functions beside it). A name that
/// resolves to nothing is a typed error, never a guessed address.
///
/// The driver hands the WHOLE card over in one call and in the proven order
/// (audio, USB and i2c before the GPU function on the way to a guest; the GPU
/// function first on the way back), so the card is named once, by its GPU
/// function. There is no explicit reset: the proven return recipe has none,
/// and a reset written after the return would hit a function the host driver
/// already holds again. Between two guests the card stays on the passthrough
/// driver.
fn hand_over(
    device: &str,
    from: &KernelOwner,
    to: &KernelOwner,
    hosts: &Hosts<'_>,
) -> Result<(), StepError> {
    let functions = card_functions(device, hosts)?;
    let card = functions
        .iter()
        .copied()
        .find(|address| address.function == 0)
        .unwrap_or(functions[0]);
    let label = device_label(card);
    match (from.guest().is_some(), to.guest().is_some()) {
        (false, true) => hosts
            .devices
            .bind_passthrough(&card)
            .map_err(|error| StepError::Driver {
                action: format!("bind the card {label} to passthrough"),
                message: error.to_string(),
            }),
        (true, false) => {
            hosts
                .devices
                .unbind_passthrough(&card)
                .map_err(|error| StepError::Driver {
                    action: format!("return the card {label} to the host driver"),
                    message: error.to_string(),
                })
        }
        _ => Ok(()),
    }
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
