//! Executors: one clean instance of a class, one job, then teardown.
//!
//! `prepare` brings up a clean instance for a job that is known (admission
//! already granted its RAM), `run` carries exactly that job under the
//! ephemeral registration, `teardown` leaves nothing behind. A slot takes its
//! next job only after `teardown` succeeded.

use std::fs;
use std::io;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::cancel::is_cancel_line;
use super::forge::{Registration, Scope};
use super::units::{UnitEvent, UnitRunner, UnitSpec};

/// The job a slot is preparing for (known before it is registered).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRef {
    /// The job's handle.
    pub handle: String,
    /// The label it asked for.
    pub label: String,
    /// The slot that takes it.
    pub slot: String,
}

/// A job under its registration.
#[derive(Debug, Clone)]
pub struct Assignment {
    /// The job.
    pub job: JobRef,
    /// Its scope.
    pub scope: Scope,
    /// The forge's root URL the runner connects to.
    pub runner_url: String,
    /// The single-use registration.
    pub registration: Registration,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    /// The runner took the task and ended with this status.
    Finished {
        /// The runner's exit status.
        exit: i32,
    },
    /// No task within the pickup deadline: the registration is still valid.
    NoTask,
    /// The runner never started: the registration is still valid.
    NotStarted(String),
    /// The registration vanished mid-run (a cancel): the job was ended.
    Cancelled,
}

/// An executor failure.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// The instance could not be brought up. `memory` marks the memory
    /// guard's refusal, which backs the slot off instead of retrying hard.
    #[error("prepare: {reason}")]
    Prepare {
        /// What failed.
        reason: String,
        /// Whether RAM was the reason.
        memory: bool,
    },
    /// Teardown left something behind: the slot stays out of service.
    #[error("teardown: {0}")]
    Teardown(String),
}

/// One slot's executor.
pub trait Executor: Send {
    /// Bring up a clean instance for `job`.
    ///
    /// # Errors
    /// The instance could not be brought up.
    fn prepare(&mut self, job: &JobRef) -> Result<(), ExecError>;
    /// Run exactly the assigned job; `log` receives the job's output lines.
    fn run(&mut self, assignment: &Assignment, log: &mut dyn FnMut(&str)) -> RunOutcome;
    /// Leave nothing behind (cgroup gone, or VM stopped and reset). Safe to
    /// call again after a failure.
    ///
    /// # Errors
    /// Something would not go away.
    fn teardown(&mut self) -> Result<(), ExecError>;
}

/// Remove a tree that a job may have left read-only; symlinks are not followed.
pub fn scrub(path: &Path) -> io::Result<()> {
    fn open_up(path: &Path) {
        let Ok(meta) = fs::symlink_metadata(path) else {
            return;
        };
        if meta.is_dir() {
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    open_up(&entry.path());
                }
            }
        }
    }
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
        Ok(_) => {
            open_up(path);
            fs::remove_dir_all(path)
        }
    }
}

/// Settings of the host executor.
#[derive(Debug, Clone)]
pub struct HostSettings {
    /// The forgejo-runner binary.
    pub runner: PathBuf,
    /// The runner configuration without connection (JSON is YAML).
    pub template: Value,
    /// The dispatcher's own directory: per job a `jobs/<unit>` directory with
    /// the configuration and the credential file (nothing the runner writes).
    pub state_dir: PathBuf,
    /// Where systemd's state directories live (`/var/lib`).
    pub state_root: PathBuf,
    /// The runner account's state directory, relative to `state_root`
    /// (`forgejo-runner`). The job unit owns `slots/<slot>/job.<handle>` and
    /// `cache/<slot>` below it, writable in the unit as part of its `StateDirectory=` (the whole directory); the job's tree
    /// is removed by the unit's `ExecStopPost=` as the runner account.
    pub state_directory: String,
    /// A command (argv) run as its own unit, as the job's account, before the
    /// runner starts: it gets the resource the job owns (the Windows test clone
    /// is woken). Failing keeps the job with the forge and backs the slot off.
    pub pre: Vec<String>,
    /// A command run after the job's unit has ended, WHATEVER happened (the
    /// clone is rolled back). The slot is not free until it succeeded.
    pub post: Vec<String>,
    /// The binary `ExecStopPost=` runs (`<it> ci scrub <dir>`).
    pub scrub_bin: PathBuf,
    /// Seconds without a task before the registration is given back.
    pub pickup: Duration,
    /// Unit properties from the deployment.
    pub properties: Vec<String>,
    /// The job's environment, shared by every slot of the pool. A value may
    /// hold `{unit}`, `{slot}`, `{job_dir}` and `{credentials}` (the unit's
    /// credentials directory).
    pub environment: Vec<(String, String)>,
    /// This slot's own environment values (they replace the pool's by name).
    pub slot_environment: Vec<(String, String)>,
    /// Where systemd exposes a unit's credentials (`/run/credentials`).
    pub credentials_root: PathBuf,
    /// Kill rounds before a teardown gives up.
    pub kill_rounds: u32,
    /// The pause between kill rounds.
    pub kill_pause: Duration,
}

/// The host-slot executor: the job's runner is a transient unit.
pub struct HostExecutor {
    units: Arc<dyn UnitRunner>,
    cfg: HostSettings,
    slot: String,
    job_dir: Option<PathBuf>,
    own_dir: Option<PathBuf>,
    unit: Option<String>,
    /// Whether the pre-job command ran (the post-job command is then owed).
    pre_ran: bool,
}

impl HostExecutor {
    /// An executor for `slot`.
    #[must_use]
    pub fn new(units: Arc<dyn UnitRunner>, cfg: HostSettings, slot: &str) -> Self {
        Self {
            units,
            cfg,
            slot: slot.to_owned(),
            job_dir: None,
            own_dir: None,
            unit: None,
            pre_ran: false,
        }
    }

    /// The runner-owned part of a job, relative to `state_root`.
    fn job_state(&self, handle: &str) -> String {
        format!(
            "{}/slots/{}/job.{handle}",
            self.cfg.state_directory, self.slot
        )
    }

    /// Run a hook command as a unit of its own (same account and sandbox as the
    /// job), wait for it and report its status.
    fn run_hook(
        &self,
        suffix: &str,
        argv: &[String],
        unit_base: &str,
        dir: &Path,
    ) -> Result<(), String> {
        let spec = UnitSpec {
            unit: format!("{unit_base}-{suffix}"),
            argv: argv.to_vec(),
            workdir: dir.to_path_buf(),
            properties: self.cfg.properties.clone(),
            environment: self.environment(unit_base, dir),
            stdin: None,
        };
        let mut running = self
            .units
            .start(&spec)
            .map_err(|e| format!("cannot start the {suffix}-job command: {e}"))?;
        while !matches!(running.next(Duration::from_secs(3600)), UnitEvent::Closed) {}
        match running.wait() {
            Ok(0) => Ok(()),
            Ok(code) => Err(format!("the {suffix}-job command ended with status {code}")),
            Err(e) => Err(format!("the {suffix}-job command: {e}")),
        }
    }

    fn cache_dir(&self) -> PathBuf {
        self.cfg
            .state_root
            .join(&self.cfg.state_directory)
            .join("cache")
            .join(&self.slot)
    }

    /// The job's configuration: the template plus exactly one connection.
    fn config(&self, a: &Assignment, unit: &str, job_dir: &Path) -> Value {
        let mut cfg = self.cfg.template.clone();
        // the token is read from the unit's credential, never written into the configuration
        let token_url = format!(
            "file:{}/{unit}.service/reg",
            self.cfg.credentials_root.display()
        );
        cfg["server"]["connections"] = json!({
            a.scope.name.clone(): {"url": a.runner_url, "uuid": a.registration.uuid, "token_url": token_url}
        });
        cfg["host"]["workdir_parent"] = json!(job_dir.join("work"));
        cfg["cache"]["dir"] = json!(self.cache_dir());
        cfg
    }

    /// The job's environment with the placeholders filled in.
    fn environment(&self, unit: &str, job_dir: &Path) -> Vec<(String, String)> {
        let credentials = format!("{}/{unit}.service", self.cfg.credentials_root.display());
        let fill = |value: &str| {
            value
                .replace("{unit}", unit)
                .replace("{slot}", &self.slot)
                .replace("{job_dir}", &job_dir.display().to_string())
                .replace("{credentials}", &credentials)
        };
        let mut out: Vec<(String, String)> = Vec::new();
        for (name, value) in self
            .cfg
            .environment
            .iter()
            .chain(&self.cfg.slot_environment)
        {
            let value = fill(value);
            match out.iter_mut().find(|(n, _)| n == name) {
                Some(entry) => entry.1 = value,
                None => out.push((name.clone(), value)),
            }
        }
        out
    }

    fn workdir_started(work: &Path) -> bool {
        fs::read_dir(work).is_ok_and(|mut d| d.next().is_some())
    }
}

impl Executor for HostExecutor {
    fn prepare(&mut self, job: &JobRef) -> Result<(), ExecError> {
        let fail = |reason: String| ExecError::Prepare {
            reason,
            memory: false,
        };
        let unit = format!("cwld-ci-{}-{}", self.slot, job.handle);
        // the dispatcher's own directory: configuration and credential file only
        let own = self.cfg.state_dir.join("jobs").join(&unit);
        scrub(&own).map_err(|e| fail(e.to_string()))?;
        fs::create_dir_all(&own).map_err(|e| fail(e.to_string()))?;
        fs::set_permissions(&own, fs::Permissions::from_mode(0o755))
            .map_err(|e| fail(e.to_string()))?;
        // the runner-owned directory is made by systemd when the unit starts
        self.job_dir = Some(self.cfg.state_root.join(self.job_state(&job.handle)));
        self.own_dir = Some(own.clone());
        self.unit = Some(unit.clone());
        if !self.cfg.pre.is_empty() {
            // owed from the first attempt on: a half-run pre command is also undone by the post one
            self.pre_ran = true;
            // `memory: true` is the dispatcher's "back the slot off" mark; the memory guard and a
            // failed pre-job command both ask for it
            self.run_hook("pre", &self.cfg.pre, &unit, &own)
                .map_err(|reason| ExecError::Prepare {
                    reason,
                    memory: true,
                })?;
        }
        Ok(())
    }

    fn run(&mut self, a: &Assignment, log: &mut dyn FnMut(&str)) -> RunOutcome {
        let (Some(dir), Some(own), Some(unit)) = (
            self.job_dir.clone(),
            self.own_dir.clone(),
            self.unit.clone(),
        ) else {
            return RunOutcome::NotStarted("run without prepare".to_owned());
        };
        // the single-use token: a 0600 file the unit loads as a credential; it is in no argument list
        let token_file = own.join("token");
        let written = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&token_file)
            .and_then(|mut f| {
                io::Write::write_all(&mut f, a.registration.token.expose().as_bytes())
            });
        let config_file = own.join("config.yml");
        let config = self.config(a, &unit, &dir);
        if let Err(e) = written.and_then(|()| fs::write(&config_file, config.to_string())) {
            return RunOutcome::NotStarted(format!("cannot write the job files: {e}"));
        }
        let mut properties = self.cfg.properties.clone();
        properties.push(format!("LoadCredential=reg:{}", token_file.display()));
        // the job's directory and the runner's cache are the unit's own state directories (owned
        // by its account, made by systemd); the tree is removed by the unit itself, as that account
        properties.push(format!("StateDirectory={}", self.cfg.state_directory));
        properties.push("StateDirectoryMode=0755".to_owned());
        properties.push(format!(
            "ExecStopPost={} ci scrub {}",
            self.cfg.scrub_bin.display(),
            dir.display()
        ));
        // the unit's state directory is the runner account's WHOLE one (what the old slot units had): HOME, the XDG cache (nix fetcher
        // locks), cargo and rustup binds, `cache/<slot>`, `slots/<slot>` are all writable in the unit's sandbox (ProtectSystem=strict).
        // The job's tree `job.<handle>` below it is made by the runner and removed by ExecStopPost; a per-job state directory could
        // neither be removed from inside the unit nor leave HOME's caches writable
        let slot_dir = self
            .cfg
            .state_root
            .join(&self.cfg.state_directory)
            .join("slots")
            .join(&self.slot);
        let spec = UnitSpec {
            unit: unit.clone(),
            argv: vec![
                self.cfg.runner.display().to_string(),
                "one-job".to_owned(),
                "--wait".to_owned(),
                "--handle".to_owned(),
                a.job.handle.clone(),
                "--config".to_owned(),
                config_file.display().to_string(),
            ],
            workdir: slot_dir,
            properties,
            environment: self.environment(&unit, &dir),
            stdin: None,
        };
        let mut running = match self.units.start(&spec) {
            Ok(r) => r,
            Err(e) => return RunOutcome::NotStarted(format!("cannot start the unit: {e}")),
        };
        let work = dir.join("work");
        let deadline = Instant::now() + self.cfg.pickup;
        let (mut picked, mut cancelled, mut no_task) = (false, false, false);
        loop {
            // a deadline, not a poll: the pickup timer only runs until the task shows up
            let wait = if picked || no_task {
                Duration::from_secs(3600)
            } else {
                deadline.saturating_duration_since(Instant::now())
            };
            match running.next(wait) {
                UnitEvent::Line(line) => {
                    log(&line);
                    if !cancelled && is_cancel_line(&line) {
                        cancelled = true;
                        let _ = self.units.kill_all(&unit);
                    } else if !picked && !no_task && Instant::now() >= deadline {
                        // a chatty runner must not push the pickup check back for ever
                        if Self::workdir_started(&work) {
                            picked = true;
                        } else {
                            no_task = true;
                            let _ = self.units.kill_all(&unit);
                        }
                    }
                }
                UnitEvent::Timeout if !picked && !no_task => {
                    if Self::workdir_started(&work) {
                        picked = true;
                    } else {
                        no_task = true;
                        let _ = self.units.kill_all(&unit);
                    }
                }
                UnitEvent::Timeout => {}
                UnitEvent::Closed => break,
            }
        }
        let exit = running.wait().unwrap_or(-1);
        if cancelled {
            RunOutcome::Cancelled
        } else if no_task && !Self::workdir_started(&work) {
            RunOutcome::NoTask
        } else {
            RunOutcome::Finished { exit }
        }
    }

    fn teardown(&mut self) -> Result<(), ExecError> {
        if let Some(unit) = self.unit.clone() {
            let mut gone = false;
            for _ in 0..self.cfg.kill_rounds.max(1) {
                let _ = self.units.kill_all(&unit);
                if self.units.gone(&unit) {
                    gone = true;
                    break;
                }
                std::thread::sleep(self.cfg.kill_pause);
            }
            if !gone {
                return Err(ExecError::Teardown(format!(
                    "the cgroup of {unit} would not go away"
                )));
            }
        }
        if self.pre_ran && !self.cfg.post.is_empty() {
            let dir = self
                .own_dir
                .clone()
                .unwrap_or_else(|| self.cfg.state_dir.clone());
            let unit = self.unit.clone().unwrap_or_default();
            self.run_hook("post", &self.cfg.post, &unit, &dir)
                .map_err(ExecError::Teardown)?;
        }
        self.pre_ran = false;
        if let Some(own) = self.own_dir.clone() {
            scrub(&own).map_err(|e| {
                ExecError::Teardown(format!("cannot remove {}: {e}", own.display()))
            })?;
        }
        // the unit removed the runner's tree itself (ExecStopPost): it must be gone
        if let Some(dir) = self.job_dir.clone()
            && dir.exists()
        {
            return Err(ExecError::Teardown(format!(
                "{} is still there after the unit ended",
                dir.display()
            )));
        }
        self.own_dir = None;
        self.job_dir = None;
        self.unit = None;
        Ok(())
    }
}

// --- VM slots ---------------------------------------------------------------------------------------------------------------

/// Why a VM driver call failed.
#[derive(Debug, thiserror::Error)]
pub enum VmError {
    /// The memory guard refused the start.
    #[error("not enough memory")]
    NoMemory,
    /// Anything else.
    #[error("{0}")]
    Failed(String),
}

/// What the guest job ended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmExit {
    /// The guest runner ended with this status.
    Code(i32),
    /// The guest runner found no task within the pickup deadline.
    NoTask,
}

/// Whether the VM driver should go on after an output line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep the job running.
    Continue,
    /// End the guest job now (the stop and reset follow).
    Abort,
}

/// The VM driver a CI-class instance needs: `cwld`'s runtime over the `cdrv`
/// host and guest traits implements it. The guest side must STREAM the job's
/// output (today's `GuestAgent::exec` returns only at the end), because the
/// cancel signal is a line of that output.
pub trait VmDriver: Send + Sync {
    /// Roll a dirty clone back to clean.
    ///
    /// # Errors
    /// The rollback failed.
    fn ensure_clean(&self, instance: &str) -> Result<(), VmError>;
    /// Start the clone.
    ///
    /// # Errors
    /// [`VmError::NoMemory`] when the memory guard refused.
    fn start(&self, instance: &str) -> Result<(), VmError>;
    /// Run the guest runner for the assignment; the registration travels on
    /// the guest session's standard input only. Every output line goes to
    /// `on_line`; [`Flow::Abort`] ends the session.
    ///
    /// # Errors
    /// The session could not be run.
    fn run_job(
        &self,
        instance: &str,
        assignment: &Assignment,
        pickup: Duration,
        on_line: &mut dyn FnMut(&str) -> Flow,
    ) -> Result<VmExit, VmError>;
    /// Stop the clone cleanly and roll it back to clean. Safe to repeat.
    ///
    /// # Errors
    /// The stop or the reset failed.
    fn stop_and_reset(&self, instance: &str) -> Result<(), VmError>;
}

/// The VM-slot executor: the runner lives inside a ci-class instance.
pub struct VmExecutor {
    driver: Arc<dyn VmDriver>,
    instance: String,
    pickup: Duration,
    started: bool,
}

impl VmExecutor {
    /// An executor for the instance behind a slot.
    #[must_use]
    pub fn new(driver: Arc<dyn VmDriver>, instance: &str, pickup: Duration) -> Self {
        Self {
            driver,
            instance: instance.to_owned(),
            pickup,
            started: false,
        }
    }
}

impl Executor for VmExecutor {
    fn prepare(&mut self, _job: &JobRef) -> Result<(), ExecError> {
        let fail = |e: VmError| ExecError::Prepare {
            memory: matches!(e, VmError::NoMemory),
            reason: e.to_string(),
        };
        self.started = true; // from the first call on a stop and reset are owed, whatever happens
        self.driver.ensure_clean(&self.instance).map_err(fail)?;
        self.driver.start(&self.instance).map_err(fail)
    }

    fn run(&mut self, a: &Assignment, log: &mut dyn FnMut(&str)) -> RunOutcome {
        let mut cancelled = false;
        let result = self
            .driver
            .run_job(&self.instance, a, self.pickup, &mut |line| {
                log(line);
                if !cancelled && is_cancel_line(line) {
                    cancelled = true;
                    Flow::Abort
                } else {
                    Flow::Continue
                }
            });
        match result {
            _ if cancelled => RunOutcome::Cancelled,
            Ok(VmExit::Code(exit)) => RunOutcome::Finished { exit },
            Ok(VmExit::NoTask) => RunOutcome::NoTask,
            Err(e) => RunOutcome::NotStarted(e.to_string()),
        }
    }

    fn teardown(&mut self) -> Result<(), ExecError> {
        if self.started {
            self.driver
                .stop_and_reset(&self.instance)
                .map_err(|e| ExecError::Teardown(e.to_string()))?;
            self.started = false;
        }
        Ok(())
    }
}
