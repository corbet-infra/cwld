//! The production [`VmDriver`]: a ci-class instance driven through the
//! runtime and the `cdrv` traits.
//!
//! One CI job in a VM is: roll the clone back to clean, start it, stream the
//! guest job (`GuestAgent::exec_stream`: output chunks become lines, the
//! cancel line ends the guest session), then stop it cleanly and reset its
//! disk. Start, stop and reset go through [`Runtime`], so the lifecycle rules,
//! the event log and the readiness waits are the ones every other plan uses.
//!
//! The guest side is the one the macOS build clone already has (`hosts/vm-dev/guest-job.sh`,
//! installed in the guest as `vm-guest-job`), driven exactly as `runner-vm-slot` drives it:
//! ONE guest session runs `bash -s`, and the payload script on its standard input unpacks the
//! job (the runner configuration with the single-use registration, the token file, the
//! credentials, the placement view) into a private job directory and runs
//! `vm-guest-job <dir> <handle> <pickup>`. The registration therefore travels on standard
//! input only, never in an argument list and never in a file on the host. The guest job exits
//! with [`NO_TASK_EXIT`] (75) when no task showed up within the pickup deadline, otherwise
//! with the runner's status.

use std::ops::ControlFlow;
use std::path::PathBuf;
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
    /// The runner configuration without connection (JSON), as for a host pool.
    pub template: serde_json::Value,
    /// The guest path of the job script (`vm-guest-job`).
    pub guest_job: String,
    /// The guest directory below which a job directory is made.
    pub guest_tmp: String,
    /// Credential files shipped into the job directory (`sccache-aws`,
    /// `forgejo-ci-read`): name and host path; a file that is unreadable is skipped.
    pub credentials: Vec<(String, PathBuf)>,
    /// The placement view ccid reads (optional, not a secret).
    pub placement: Option<PathBuf>,
    /// The longest a guest job may run before the driver ends it.
    pub job_timeout: Duration,
}

impl Default for VmSettings {
    fn default() -> Self {
        Self {
            template: serde_json::json!({"server": {}, "host": {}, "cache": {}}),
            guest_job: "/Volumes/cache/bin/vm-guest-job".to_owned(),
            guest_tmp: "/tmp".to_owned(),
            credentials: Vec::new(),
            placement: None,
            // longer than the runner's own job timeout (3 h)
            job_timeout: Duration::from_secs(3 * 3600 + 600),
        }
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding (the guest decodes with `base64 -D`).
#[must_use]
pub fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(B64[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn valid_token(token: &str) -> bool {
    (8..=200).contains(&token.len())
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// The payload script the guest session runs (`bash -s`); a port of `runner-vm-slot`'s
/// `build_payload`. Everything that is not plain text travels base64-encoded in a quoted
/// here-document, so no value is ever interpreted by the guest's shell.
fn payload(settings: &VmSettings, a: &Assignment, pickup: Duration) -> Result<String, VmError> {
    let bad = |what: &str| VmError::Failed(format!("refusing the assignment: {what}"));
    let handle = &a.job.handle;
    if !super::forge::is_handle(handle) {
        return Err(bad("the handle"));
    }
    if !super::forge::is_name(&a.scope.name) {
        return Err(bad("the scope"));
    }
    if !valid_token(a.registration.token.expose()) {
        return Err(bad("the registration token"));
    }
    let gd = format!(
        "{}/rv-job-{handle}",
        settings.guest_tmp.trim_end_matches('/')
    );
    if gd.contains(['\'', '"', '\n', '$', '`', '\\', ' ']) {
        return Err(bad("the guest directory"));
    }
    let mut cfg = settings.template.clone();
    cfg["server"]["connections"] = serde_json::json!({
        a.scope.name.clone(): {
            "url": a.runner_url,
            "uuid": a.registration.uuid,
            "token_url": format!("file:{gd}/token"),
        }
    });
    cfg["host"]["workdir_parent"] = serde_json::json!(format!("{gd}/work"));
    cfg["cache"]["dir"] = serde_json::json!(format!("{gd}/cache"));
    let mut out = String::new();
    out.push_str("set -eu\numask 077\n");
    out.push_str(&format!("d={gd}\n"));
    out.push_str("mkdir -p \"$d/work\" \"$d/cache\" \"$d/credentials\"\n");
    out.push_str("dec() { base64 -D > \"$1\"; }\n");
    let mut put = |name: &str, bytes: &[u8]| {
        out.push_str(&format!(
            "dec \"$d/{name}\" <<'RV_B64'\n{}\nRV_B64\n",
            base64_encode(bytes)
        ));
    };
    put("config.yml", cfg.to_string().as_bytes());
    put("token", a.registration.token.expose().as_bytes());
    for (name, path) in &settings.credentials {
        if let Ok(bytes) = std::fs::read(path) {
            put(&format!("credentials/{name}"), &bytes);
        }
    }
    if let Some(path) = &settings.placement
        && let Ok(bytes) = std::fs::read(path)
    {
        put("placement.json", &bytes);
    }
    out.push_str("rc=0\n");
    // the job reads no stdin (the rest of this script would be its input)
    out.push_str(&format!(
        "{} \"$d\" {handle} {} < /dev/null || rc=$?\n",
        settings.guest_job,
        pickup.as_secs()
    ));
    out.push_str("rm -r \"$d\" 2> /dev/null || true\nexit \"$rc\"\n");
    Ok(out)
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
            program: "bash".to_owned(),
            args: vec!["-s".to_owned()],
            timeout: self.settings.job_timeout,
            // the registration travels on standard input only
            stdin: Some(payload(&self.settings, assignment, pickup)?.into_bytes()),
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
