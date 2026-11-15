//! Host slots: a job runs as a transient unit with its own cgroup, and
//! `cgroup.kill` ends everything in it.
//!
//! The kernel's own mechanism, as in the Bash executor it replaces: one cgroup
//! per job, everything the job forks (setsid and double forks included) stays
//! in it, and `cgroup.kill` ends every process at once. A cancelled run that
//! was left alone used to hold the target-cache flock and the CPUs.

use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

/// What to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitSpec {
    /// The unit name (`cwld-ci-<slot>-<handle>`).
    pub unit: String,
    /// The command and its arguments; never a secret.
    pub argv: Vec<String>,
    /// The job's work directory (below it the runner makes one directory per
    /// task: its appearance is the start of the job).
    pub workdir: PathBuf,
    /// Unit properties from the deployment (`User=`, `Delegate=yes`,
    /// `ProtectProc=`, `LoadCredential=`, ...). cwld knows no sandbox detail.
    pub properties: Vec<String>,
}

/// One event of a running unit's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitEvent {
    /// One output line.
    Line(String),
    /// Nothing arrived within the wait.
    Timeout,
    /// Output ended: the unit's main process is gone.
    Closed,
}

/// A started unit.
pub trait RunningUnit: Send {
    /// The next output line, or why there is none within `wait`.
    fn next(&mut self, wait: Duration) -> UnitEvent;
    /// The unit's exit status (call after [`UnitEvent::Closed`]).
    ///
    /// # Errors
    /// The status could not be collected.
    fn wait(&mut self) -> io::Result<i32>;
}

/// The thing that starts, kills and checks units.
pub trait UnitRunner: Send + Sync {
    /// Start a unit.
    ///
    /// # Errors
    /// The unit could not be started.
    fn start(&self, spec: &UnitSpec) -> io::Result<Box<dyn RunningUnit>>;
    /// End every process of the unit at once (`cgroup.kill`).
    ///
    /// # Errors
    /// The kill file could not be written (a unit that is gone is no error).
    fn kill_all(&self, unit: &str) -> io::Result<()>;
    /// Whether the unit and its cgroup are gone.
    fn gone(&self, unit: &str) -> bool;
}

/// Units through `systemd-run` (interim adapter).
///
/// The adopted-software table names `zbus` for systemd; this adapter shells to
/// `systemd-run --pipe --wait` because that is the one call that streams the
/// unit's output back, and it sits behind [`UnitRunner`] so the swap is one
/// file. The kill never goes through `systemctl`: it writes the unit's
/// `cgroup.kill` directly.
#[derive(Debug, Clone)]
pub struct SystemdRun {
    /// The `systemd-run` binary.
    pub systemd_run: String,
    /// The cgroup v2 mount (`/sys/fs/cgroup`).
    pub cgroup_root: PathBuf,
    /// The slice transient system units live in.
    pub slice: String,
}

impl Default for SystemdRun {
    fn default() -> Self {
        Self {
            systemd_run: "systemd-run".to_owned(),
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
            slice: "system.slice".to_owned(),
        }
    }
}

impl SystemdRun {
    fn cgroup(&self, unit: &str) -> PathBuf {
        self.cgroup_root
            .join(&self.slice)
            .join(format!("{unit}.service"))
    }
}

struct Piped {
    lines: mpsc::Receiver<String>,
    child: std::process::Child,
}

impl RunningUnit for Piped {
    fn next(&mut self, wait: Duration) -> UnitEvent {
        match self.lines.recv_timeout(wait) {
            Ok(line) => UnitEvent::Line(line),
            Err(RecvTimeoutError::Timeout) => UnitEvent::Timeout,
            Err(RecvTimeoutError::Disconnected) => UnitEvent::Closed,
        }
    }

    fn wait(&mut self) -> io::Result<i32> {
        Ok(self.child.wait()?.code().unwrap_or(-1))
    }
}

fn pump(stream: impl Read + Send + 'static, tx: mpsc::Sender<String>) {
    std::thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
}

impl UnitRunner for SystemdRun {
    fn start(&self, spec: &UnitSpec) -> io::Result<Box<dyn RunningUnit>> {
        let mut cmd = Command::new(&self.systemd_run);
        cmd.arg(format!("--unit={}", spec.unit))
            .args(["--collect", "--pipe", "--wait", "--quiet"])
            .arg(format!("--working-directory={}", spec.workdir.display()));
        for property in &spec.properties {
            cmd.arg(format!("--property={property}"));
        }
        cmd.arg("--").args(&spec.argv);
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let (tx, rx) = mpsc::channel();
        if let Some(out) = child.stdout.take() {
            pump(out, tx.clone());
        }
        if let Some(err) = child.stderr.take() {
            pump(err, tx.clone());
        }
        drop(tx);
        Ok(Box::new(Piped { lines: rx, child }))
    }

    fn kill_all(&self, unit: &str) -> io::Result<()> {
        let file = self.cgroup(unit).join("cgroup.kill");
        match std::fs::write(&file, b"1") {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    fn gone(&self, unit: &str) -> bool {
        !self.cgroup(unit).exists()
    }
}
