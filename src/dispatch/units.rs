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
    /// The job's environment (`NAME`, value). Paths and ports, never a secret:
    /// they travel in an argument list.
    pub environment: Vec<(String, String)>,
    /// Bytes for the unit's standard input, written once and then closed
    /// (`systemd-run --pipe` carries them): a credential travels here, never
    /// in `argv`. `None` gives the unit no input.
    pub stdin: Option<Vec<u8>>,
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
    /// End every process of the unit at once (SIGKILL to the whole cgroup).
    ///
    /// # Errors
    /// The kill failed (a unit that is gone is no error).
    fn kill_all(&self, unit: &str) -> io::Result<()>;
    /// Whether the unit and its cgroup are gone.
    fn gone(&self, unit: &str) -> bool;
    /// End every leftover unit whose name starts with `prefix` (what a crashed
    /// predecessor left); returns their names.
    ///
    /// # Errors
    /// The list could not be read.
    fn sweep(&self, prefix: &str) -> io::Result<Vec<String>>;
}

/// Units through `systemd-run` and `systemctl` (interim adapter).
///
/// The adopted-software table names `zbus` for systemd; this adapter shells to
/// `systemd-run --pipe --wait` because that is the one call that streams the
/// unit's output back, and it sits behind [`UnitRunner`] so the swap is one
/// file. The kill is `systemctl kill --kill-whom=all
/// --signal=SIGKILL`, so the dispatcher needs no privilege beyond the polkit
/// rule that lets its account manage `cwld-ci-*` units; systemd ends the
/// whole cgroup.
#[derive(Debug, Clone)]
pub struct SystemdRun {
    /// The `systemd-run` binary.
    pub systemd_run: String,
    /// The `systemctl` binary.
    pub systemctl: String,
    /// The cgroup v2 mount (`/sys/fs/cgroup`).
    pub cgroup_root: PathBuf,
    /// The slice transient system units live in.
    pub slice: String,
}

impl Default for SystemdRun {
    fn default() -> Self {
        Self {
            systemd_run: "systemd-run".to_owned(),
            systemctl: "systemctl".to_owned(),
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
        // Bytes, not `lines()`: a line that is not UTF-8 must not end the pump
        // (the unit would then block on a full pipe and the cancel line would
        // never be seen).
        let mut reader = BufReader::new(stream);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&buf);
                    if tx
                        .send(text.trim_end_matches(['\n', '\r']).to_owned())
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
}

impl UnitRunner for SystemdRun {
    fn start(&self, spec: &UnitSpec) -> io::Result<Box<dyn RunningUnit>> {
        let mut cmd = Command::new(&self.systemd_run);
        cmd.arg(format!("--unit={}", spec.unit))
            .args(["--system", "--no-ask-password"])
            .arg(format!("--slice={}", self.slice))
            .args(["--collect", "--pipe", "--wait", "--quiet"])
            .arg(format!("--working-directory={}", spec.workdir.display()));
        for property in &spec.properties {
            cmd.arg(format!("--property={property}"));
        }
        for (name, value) in &spec.environment {
            cmd.arg(format!("--setenv={name}={value}"));
        }
        cmd.arg("--").args(&spec.argv);
        let mut child = cmd
            .stdin(if spec.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let (Some(bytes), Some(mut stdin)) = (spec.stdin.clone(), child.stdin.take()) {
            // a thread: a full pipe must not block the output reader
            std::thread::spawn(move || {
                let _ = std::io::Write::write_all(&mut stdin, &bytes);
            });
        }
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
        if self.gone(unit) {
            return Ok(());
        }
        let out = Command::new(&self.systemctl)
            .args(["--system", "--no-ask-password", "kill", "--kill-whom=all"])
            .arg("--signal=SIGKILL")
            .arg(format!("{unit}.service"))
            .stdin(Stdio::null())
            .output()?;
        // a unit that vanished between the check and the call is not an error
        if out.status.success() || self.gone(unit) {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "systemctl kill {unit}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    fn gone(&self, unit: &str) -> bool {
        !self.cgroup(unit).exists()
    }

    fn sweep(&self, prefix: &str) -> io::Result<Vec<String>> {
        let dir = self.cgroup_root.join(&self.slice);
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&dir)?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(unit) = name.strip_suffix(".service")
                && unit.starts_with(prefix)
            {
                self.kill_all(unit)?;
                found.push(unit.to_owned());
            }
        }
        found.sort();
        Ok(found)
    }
}
