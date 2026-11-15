//! The production [`VmDriver`]: a ci-class instance driven through the
//! runtime and the `cdrv` traits.
//!
//! One CI job in a VM is: roll the clone back to clean, start it, stream the
//! guest job (`GuestAgent::exec_stream`: output chunks become lines, the
//! cancel line ends the guest session), then stop it cleanly and reset its
//! disk. Start, stop and reset go through [`Runtime`], so the lifecycle rules,
//! the event log and the readiness waits are the ones every other plan uses.
//!
//! The guest side is a small wrapper the guest image ships (its path is
//! [`VmSettings::guest_program`]). Its contract:
//!
//! * arguments: `one-job --handle <h> --scope <name> --url <forge root>
//!   --uuid <runner uuid> --pickup-s <n>`;
//! * the single-use registration token arrives on standard input only (never
//!   in an argument list, which any process list shows);
//! * it runs `forgejo-runner one-job --wait --handle <h>` for that job, and
//!   exits with [`NO_TASK_EXIT`] when no task showed up within the pickup
//!   deadline, otherwise with the runner's status.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cdrv::{
    DiskReset, EventLog, ExecChunk, ExecOutcome, ExecStream, GuestAgent, GuestExec, HostDevices,
    HostInstances, InstanceId, Timestamp,
};

use super::exec::{Assignment, Flow, VmDriver, VmError, VmExit};
use crate::plan::Hosts;
use crate::runtime::Runtime;

/// The guest wrapper's exit status for "no task within the pickup deadline"
/// (`EX_TEMPFAIL`).
pub const NO_TASK_EXIT: i32 = 75;

/// The host side of a VM driver: lifecycle, devices and disk reset.
pub trait VmHost: HostInstances + HostDevices + DiskReset + Send + Sync {}
impl<T: HostInstances + HostDevices + DiskReset + Send + Sync> VmHost for T {}

/// A guest agent shared between threads.
pub type SharedGuest = Arc<dyn GuestAgent + Send + Sync>;

/// How guest jobs are run.
#[derive(Debug, Clone)]
pub struct VmSettings {
    /// The wrapper program inside the guest.
    pub guest_program: String,
    /// The longest a guest job may run before the driver ends it.
    pub job_timeout: Duration,
}

impl Default for VmSettings {
    fn default() -> Self {
        Self {
            guest_program: "/usr/local/bin/cwld-ci-job".to_owned(),
            // longer than the runner's own job timeout (3 h)
            job_timeout: Duration::from_secs(3 * 3600 + 600),
        }
    }
}

/// The driver over a runtime, a host and one guest agent per instance.
pub struct RuntimeVmDriver<H: VmHost> {
    runtime: Mutex<Runtime>,
    log: Mutex<EventLog>,
    host: Arc<H>,
    guests: Box<dyn Fn(&str) -> SharedGuest + Send + Sync>,
    settings: VmSettings,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now() -> Timestamp {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    Timestamp::from_millis(ms)
}

impl<H: VmHost> RuntimeVmDriver<H> {
    /// A driver. `runtime` holds the registered ci instances; `guests` gives
    /// the guest agent of an instance by name.
    pub fn new(
        runtime: Runtime,
        log: EventLog,
        host: Arc<H>,
        guests: impl Fn(&str) -> SharedGuest + Send + Sync + 'static,
        settings: VmSettings,
    ) -> Self {
        Self {
            runtime: Mutex::new(runtime),
            log: Mutex::new(log),
            host,
            guests: Box::new(guests),
            settings,
        }
    }

    fn hosts(&self) -> Hosts<'_> {
        Hosts::new(&*self.host, &*self.host).with_disk_reset(&*self.host)
    }
}

/// Output chunks to lines, per stream (a line never spans two streams).
#[derive(Default)]
struct Lines {
    out: Vec<u8>,
    err: Vec<u8>,
}

impl Lines {
    fn push(&mut self, chunk: &ExecChunk) -> Vec<String> {
        let buf = match chunk.stream {
            ExecStream::Stdout => &mut self.out,
            ExecStream::Stderr => &mut self.err,
        };
        buf.extend_from_slice(&chunk.bytes);
        let mut lines = Vec::new();
        while let Some(end) = buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buf.drain(..=end).collect();
            lines.push(text(&line));
        }
        lines
    }

    /// The unfinished last line of each stream.
    fn rest(&mut self) -> Vec<String> {
        [&mut self.out, &mut self.err]
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(|b| text(&std::mem::take(b)))
            .collect()
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_end_matches(['\n', '\r'])
        .to_owned()
}

impl<H: VmHost> VmDriver for RuntimeVmDriver<H> {
    fn ensure_clean(&self, instance: &str) -> Result<(), VmError> {
        let id = InstanceId::from(instance);
        let hosts = self.hosts();
        lock(&self.runtime)
            .ensure_clean(&id, &hosts, &mut lock(&self.log), now())
            .map(|_| ())
            .map_err(|e| VmError::Failed(e.to_string()))
    }

    fn start(&self, instance: &str) -> Result<(), VmError> {
        // RAM was admitted before this call (the dispatcher asks `Admission` first), so a
        // refusal for memory does not come from here
        let id = InstanceId::from(instance);
        let hosts = self.hosts();
        lock(&self.runtime)
            .start_instance(&id, &hosts, &mut lock(&self.log), now(), &mut Vec::new())
            .map_err(|e| VmError::Failed(e.to_string()))
    }

    fn run_job(
        &self,
        instance: &str,
        assignment: &Assignment,
        pickup: Duration,
        on_line: &mut dyn FnMut(&str) -> Flow,
    ) -> Result<VmExit, VmError> {
        let guest = (self.guests)(instance);
        let request = GuestExec {
            program: self.settings.guest_program.clone(),
            args: vec![
                "one-job".to_owned(),
                "--handle".to_owned(),
                assignment.job.handle.clone(),
                "--scope".to_owned(),
                assignment.scope.name.clone(),
                "--url".to_owned(),
                assignment.runner_url.clone(),
                "--uuid".to_owned(),
                assignment.registration.uuid.clone(),
                "--pickup-s".to_owned(),
                pickup.as_secs().to_string(),
            ],
            timeout: self.settings.job_timeout,
            // the registration token travels on standard input only
            stdin: Some(assignment.registration.token.expose().as_bytes().to_vec()),
        };
        let mut lines = Lines::default();
        let mut aborted = false;
        let outcome = guest
            .exec_stream(&request, &mut |chunk| {
                for line in lines.push(&chunk) {
                    if on_line(&line) == Flow::Abort {
                        aborted = true;
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            })
            .map_err(|e| VmError::Failed(e.to_string()))?;
        if !aborted {
            for line in lines.rest() {
                if on_line(&line) == Flow::Abort {
                    aborted = true;
                    break;
                }
            }
        }
        match outcome {
            ExecOutcome::Exited(NO_TASK_EXIT) => Ok(VmExit::NoTask),
            ExecOutcome::Exited(code) => Ok(VmExit::Code(code)),
            // the caller asked for it (the cancel line): the executor reports Cancelled
            ExecOutcome::Cancelled if aborted => Ok(VmExit::Code(-1)),
            ExecOutcome::Cancelled => Err(VmError::Failed(
                "the guest session was cancelled by the driver".to_owned(),
            )),
            ExecOutcome::TimedOut => Ok(VmExit::Code(124)),
        }
    }

    fn stop_and_reset(&self, instance: &str) -> Result<(), VmError> {
        let id = InstanceId::from(instance);
        let hosts = self.hosts();
        let mut runtime = lock(&self.runtime);
        let mut log = lock(&self.log);
        // a stop that fails skips the reset: resetting a guest that may still run is unsafe
        runtime
            .stop_instance(&id, &hosts, &mut log, now(), &mut Vec::new())
            .map_err(|e| VmError::Failed(e.to_string()))?;
        runtime
            .reset_instance(&id, &hosts, &mut log, now(), &mut Vec::new())
            .map_err(|e| VmError::Failed(e.to_string()))
    }
}
