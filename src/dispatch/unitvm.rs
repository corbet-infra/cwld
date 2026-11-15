//! A VM pool whose every operation is a command run as a unit of the pool's
//! account.
//!
//! The owner's rule (2026-11-13) stands: a guest escape must not land in a
//! personal account, and the build clones live in the RUNNER account's libvirt
//! session. The dispatcher therefore never touches that session. Each VM
//! operation (clean, wake, sync, the guest session, reset, standby) is a
//! transient unit started like a host job, as `run_as`, with the pool's
//! sandbox; what the commands are (`vm-dev <clone> wake`, `virsh --connect
//! qemu:///session ...`, a zfs step unit, ...) is the deployment's, and cwld
//! links no libvirt. The guest session itself is the same `bash -s` payload as
//! [`super::vmdriver`], carried on the unit's standard input.
//!
//! Exit status 75 from the wake command means the memory guard refused the
//! start (`vm-dev`'s convention); the same status from the guest job means no
//! task showed up within the pickup deadline.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::exec::{Assignment, Flow, VmDriver, VmError, VmExit};
use super::units::{UnitEvent, UnitRunner, UnitSpec};
use super::vmdriver::{NO_TASK_EXIT, VmSettings, guest_payload};

/// The commands of one VM pool (argv, with `{slot}` replaced by the instance).
#[derive(Debug, Clone, Default)]
pub struct VmCommands {
    /// The guest transport; the guest command is appended
    /// (`["vm-dev", "{slot}", "pipe", "--"]` gives `vm-dev mac-0 pipe -- bash -s`).
    pub transport: Vec<String>,
    /// Roll a dirty clone back before the job (optional).
    pub ensure_clean: Vec<String>,
    /// Start the clone; status 75 = the memory guard refused.
    pub wake: Vec<String>,
    /// Install the guest tools after the clone is up (optional).
    pub sync: Vec<String>,
    /// Power off and roll the disk back to its clean snapshot.
    pub reset: Vec<String>,
    /// Boot clean and hibernate, so the next wake is a restore (optional).
    pub standby: Vec<String>,
}

/// The driver: units of `run_as` for every operation.
pub struct UnitVmDriver {
    units: Arc<dyn UnitRunner>,
    commands: VmCommands,
    settings: VmSettings,
    properties: Vec<String>,
    environment: Vec<(String, String)>,
    workdir: PathBuf,
    seq: AtomicU64,
    log: Box<dyn Fn(&str) + Send + Sync>,
}

impl UnitVmDriver {
    /// A driver. `properties` are the pool's unit properties (`User=`, the
    /// sandbox), `workdir` a directory the account can enter.
    pub fn new(
        units: Arc<dyn UnitRunner>,
        commands: VmCommands,
        settings: VmSettings,
        properties: Vec<String>,
        environment: Vec<(String, String)>,
        workdir: PathBuf,
        log: impl Fn(&str) + Send + Sync + 'static,
    ) -> Self {
        Self {
            units,
            commands,
            settings,
            properties,
            environment,
            workdir,
            seq: AtomicU64::new(0),
            log: Box::new(log),
        }
    }

    fn expand(argv: &[String], instance: &str) -> Vec<String> {
        argv.iter().map(|a| a.replace("{slot}", instance)).collect()
    }

    fn spec(
        &self,
        instance: &str,
        verb: &str,
        argv: Vec<String>,
        stdin: Option<Vec<u8>>,
    ) -> UnitSpec {
        let n = self.seq.fetch_add(1, Ordering::SeqCst);
        UnitSpec {
            unit: format!("cwld-ci-vm-{instance}-{verb}-{n}"),
            argv,
            workdir: self.workdir.clone(),
            properties: self.properties.clone(),
            environment: self.environment.clone(),
            stdin,
        }
    }

    /// Run one command to its end; the exit status. Its output goes to the log.
    fn command(&self, instance: &str, verb: &str, argv: &[String]) -> Result<i32, VmError> {
        let spec = self.spec(instance, verb, Self::expand(argv, instance), None);
        let mut running = self
            .units
            .start(&spec)
            .map_err(|e| VmError::Failed(format!("cannot start the {verb} command: {e}")))?;
        loop {
            match running.next(Duration::from_secs(3600)) {
                UnitEvent::Line(l) => (self.log)(&format!("vm slot={instance} {verb}={l}")),
                UnitEvent::Timeout => {}
                UnitEvent::Closed => break,
            }
        }
        running
            .wait()
            .map_err(|e| VmError::Failed(format!("the {verb} command: {e}")))
    }

    fn must(&self, instance: &str, verb: &str, argv: &[String]) -> Result<(), VmError> {
        if argv.is_empty() {
            return Ok(());
        }
        match self.command(instance, verb, argv)? {
            0 => Ok(()),
            code => Err(VmError::Failed(format!(
                "the {verb} command ended with status {code}"
            ))),
        }
    }
}

impl VmDriver for UnitVmDriver {
    fn ensure_clean(&self, instance: &str) -> Result<(), VmError> {
        self.must(instance, "clean", &self.commands.ensure_clean)
    }

    fn start(&self, instance: &str) -> Result<(), VmError> {
        match self.command(instance, "wake", &self.commands.wake)? {
            0 => {}
            75 => return Err(VmError::NoMemory),
            code => {
                return Err(VmError::Failed(format!(
                    "the wake command ended with status {code}"
                )));
            }
        }
        self.must(instance, "sync", &self.commands.sync)
    }

    fn run_job(
        &self,
        instance: &str,
        assignment: &Assignment,
        pickup: Duration,
        on_line: &mut dyn FnMut(&str) -> Flow,
    ) -> Result<VmExit, VmError> {
        let payload = guest_payload(&self.settings, assignment, pickup)?;
        let mut argv = Self::expand(&self.commands.transport, instance);
        argv.extend(["bash".to_owned(), "-s".to_owned()]);
        let spec = self.spec(instance, "job", argv, Some(payload.into_bytes()));
        let unit = spec.unit.clone();
        let mut running = self
            .units
            .start(&spec)
            .map_err(|e| VmError::Failed(format!("cannot start the guest session: {e}")))?;
        let deadline = Instant::now() + self.settings.job_timeout;
        let (mut aborted, mut timed_out) = (false, false);
        loop {
            match running.next(deadline.saturating_duration_since(Instant::now())) {
                UnitEvent::Line(l) => {
                    if !aborted && on_line(&l) == Flow::Abort {
                        aborted = true;
                        let _ = self.units.kill_all(&unit);
                    }
                }
                UnitEvent::Timeout => {
                    timed_out = true;
                    let _ = self.units.kill_all(&unit);
                    break;
                }
                UnitEvent::Closed => break,
            }
        }
        let code = running.wait().unwrap_or(-1);
        if aborted {
            Ok(VmExit::Code(-1))
        } else if timed_out {
            Ok(VmExit::Code(124))
        } else if code == NO_TASK_EXIT {
            Ok(VmExit::NoTask)
        } else {
            Ok(VmExit::Code(code))
        }
    }

    fn stop_and_reset(&self, instance: &str) -> Result<(), VmError> {
        self.must(instance, "reset", &self.commands.reset)?;
        if !self.commands.standby.is_empty()
            && self
                .must(instance, "standby", &self.commands.standby)
                .is_err()
        {
            // a standby that failed leaves a half-booted clone: roll it back again
            self.must(instance, "reset", &self.commands.reset)?;
        }
        Ok(())
    }
}
