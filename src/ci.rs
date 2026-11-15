//! The CI class: one job's whole life.
//!
//! One CI job is: `ensure_clean` (reset a dirty ephemeral disk) → start → run
//! the job through the guest agent's `exec` → stop → reset. The Forgejo runner
//! itself runs inside the instance and is out of scope here. The cycle always
//! tries to stop and reset after the command ran, even when the command failed,
//! because the next job must start from a clean clone; if the stop fails the
//! reset is skipped, since resetting a guest that may still be running would be
//! unsafe.

use cdrv::{
    Event, EventLog, ExecResult, GuestAgent, GuestExec, InstanceClass, InstanceId, Severity,
    Subject, Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::plan::{Hosts, StepError};
use crate::runtime::{Runtime, RuntimeError};

/// One CI job: which instance runs which command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiJob {
    /// The CI instance.
    pub instance: InstanceId,
    /// The command to run through the guest agent.
    pub command: GuestExec,
}

/// What one CI job did.
#[derive(Debug, Clone, PartialEq)]
pub struct CiJobReport {
    /// The instance the job ran on.
    pub instance: InstanceId,
    /// The command's result, when it ran.
    pub exec: Option<ExecResult>,
    /// Why the command did not run, when the driver refused it.
    pub exec_error: Option<String>,
    /// Whether the instance started.
    pub started: bool,
    /// Whether the instance stopped cleanly.
    pub stopped: bool,
    /// Whether the disk was reset to clean afterwards.
    pub reset: bool,
    /// The first lifecycle step that failed.
    pub failure: Option<StepError>,
    /// Every event the cycle emitted.
    pub events: Vec<Event>,
}

impl Runtime {
    /// Run one CI job: ensure clean, start, exec, stop, reset.
    pub fn run_ci_job(
        &mut self,
        job: &CiJob,
        hosts: &Hosts<'_>,
        guest: &dyn GuestAgent,
        log: &mut EventLog,
        at: Timestamp,
    ) -> Result<CiJobReport, RuntimeError> {
        let class = self
            .class_of(&job.instance)
            .ok_or_else(|| RuntimeError::UnknownInstance {
                instance: job.instance.clone(),
            })?;
        if class != InstanceClass::Ci {
            return Err(RuntimeError::NotACiInstance {
                instance: job.instance.clone(),
                class,
            });
        }
        let mut events = vec![log.emit(
            at,
            "job.started",
            Subject::Instance(job.instance.clone()),
            Severity::Info,
            json!({"program": job.command.program, "args": job.command.args}),
        )];
        let mut failure = self
            .start_instance(&job.instance, hosts, log, at, &mut events)
            .err();
        let started = failure.is_none();
        let mut exec = None;
        let mut exec_error = None;
        if started {
            match guest.exec(&job.command) {
                Ok(result) => exec = Some(result),
                Err(error) => exec_error = Some(error.to_string()),
            }
        }
        let mut stopped = false;
        if started {
            match self.stop_instance(&job.instance, hosts, log, at, &mut events) {
                Ok(()) => stopped = true,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        let mut reset = false;
        if stopped {
            match self.reset_instance(&job.instance, hosts, log, at, &mut events) {
                Ok(()) => reset = true,
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(StepError::Clean {
                            instance: job.instance.clone(),
                            message: error.to_string(),
                        });
                    }
                }
            }
        }
        let ok = failure.is_none() && exec_error.is_none();
        events.push(log.emit(
            at,
            "job.finished",
            Subject::Instance(job.instance.clone()),
            if ok { Severity::Info } else { Severity::Warn },
            json!({
                "status": if ok { "ok" } else { "failed" },
                "exit": exec.as_ref().map(|result| result.status),
                "reset": reset,
            }),
        ));
        Ok(CiJobReport {
            instance: job.instance.clone(),
            exec,
            exec_error,
            started,
            stopped,
            reset,
            failure,
            events,
        })
    }
}
