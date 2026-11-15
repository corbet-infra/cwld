//! Crash recovery: report, never guess.
//!
//! After a crash the runtime reads its dirty marks and the plan journals that
//! were left behind. It reports both as events and returns them, and changes
//! nothing: a half-run plan is not resumed, an uncertain step is not assumed
//! done or undone, and a dirty instance is not started or reset until the
//! normal path (`ensure_clean`, then a start) decides to. Reporting a journal
//! removes it, so the same crash is not reported twice; the report is the
//! record.

use cdct::{KernelOwner, PlanStep};
use cdrv::{Event, EventLog, Severity, Subject, Timestamp};
use serde_json::json;

use crate::dirty::DirtyMark;
use crate::runtime::Runtime;
use crate::store::StoreError;

/// One instance whose dirty mark survived a crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyReport {
    /// The mark.
    pub mark: DirtyMark,
    /// Whether the instance's class resets its disk before the next start.
    pub ephemeral: bool,
}

/// One plan that did not finish before the runtime died.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HalfRunPlan {
    /// The kernel the plan was switching the card to.
    pub target: KernelOwner,
    /// The steps that completed.
    pub executed: Vec<PlanStep>,
    /// The one step that was in flight and may be partly done.
    pub uncertain: Option<PlanStep>,
    /// The steps nothing was attempted for.
    pub not_started: Vec<PlanStep>,
    /// When the plan began.
    pub started_at: Timestamp,
}

/// What recovery found.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryReport {
    /// The dirty instances.
    pub dirty: Vec<DirtyReport>,
    /// The half-run plans.
    pub half_run: Vec<HalfRunPlan>,
    /// Every event recovery emitted.
    pub events: Vec<Event>,
}

impl Runtime {
    /// Report the dirty marks and half-run plans left by a crash.
    pub fn recover(
        &mut self,
        log: &mut EventLog,
        at: Timestamp,
    ) -> Result<RecoveryReport, StoreError> {
        let mut events = Vec::new();
        let mut dirty = Vec::new();
        for mark in self.marks().all()? {
            let ephemeral = self
                .class_of(&mark.instance)
                .is_some_and(|class| self.classes().is_ephemeral(class));
            events.push(log.emit(
                at,
                "instance.dirty",
                Subject::Instance(mark.instance.clone()),
                Severity::Warn,
                json!({
                    "reason": mark.reason,
                    "at": mark.at.as_millis(),
                    "detail": &mark.detail,
                    "ephemeral": ephemeral,
                    "action": if ephemeral {
                        "reset before the next start"
                    } else {
                        "reported; a human decides"
                    },
                }),
            ));
            dirty.push(DirtyReport { mark, ephemeral });
        }
        let mut half_run = Vec::new();
        for journal in self.journals().scan()? {
            let plan = HalfRunPlan {
                target: journal.target.clone(),
                executed: journal.executed().to_vec(),
                uncertain: journal.uncertain().cloned(),
                not_started: journal.not_started().to_vec(),
                started_at: journal.started_at,
            };
            events.push(log.emit(
                at,
                "plan.incomplete",
                Subject::Host,
                Severity::Warn,
                json!({
                    "target": &plan.target,
                    "executed": &plan.executed,
                    "uncertain": &plan.uncertain,
                    "not_started": &plan.not_started,
                    "started_at": plan.started_at,
                }),
            ));
            self.journals().finish(&journal)?;
            half_run.push(plan);
        }
        Ok(RecoveryReport {
            dirty,
            half_run,
            events,
        })
    }
}
