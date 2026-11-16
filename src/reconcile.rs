//! Instance reconcile: the declared instances drive the defined domains.
//!
//! For every declared instance the host's renderer produces the domain
//! definition. The last definition this reconcile put in place is kept in
//! `<state_dir>/reconcile/<id>.xml`; libvirt's own copy is not compared
//! because it is normalised (addresses added). The rule, with no timer (a
//! unit runs it when the deployed configuration changes):
//!
//! | domain | rendered == recorded | action |
//! |---|---|---|
//! | not defined | any | define, record |
//! | shut off | yes | nothing |
//! | shut off | no | redefine, record |
//! | anything else (running, starting, stopping, paused, failed) | no | report `Pending`, change nothing |
//!
//! A running domain is never touched: the difference is applied at the next
//! stop, by the next run.

use std::fs;
use std::path::{Path, PathBuf};

use cdrv::{InstanceId, InstanceSpec, InstanceState};

/// What the reconcile needs from the host: render, observe, define.
pub trait Domains {
    /// The definition the spec renders to.
    ///
    /// # Errors
    ///
    /// The spec is refused by the platform rules.
    fn render(&self, spec: &InstanceSpec) -> Result<String, String>;

    /// The domain's state, `None` when it is not defined.
    ///
    /// # Errors
    ///
    /// The host cannot be asked.
    fn state(&self, id: &InstanceId) -> Result<Option<InstanceState>, String>;

    /// Define (or redefine) the domain from the spec; never start it.
    ///
    /// # Errors
    ///
    /// The host refuses the definition.
    fn define(&self, spec: &InstanceSpec) -> Result<(), String>;
}

/// The result for one instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The domain already has this definition.
    Unchanged,
    /// The domain did not exist and is defined now.
    Defined,
    /// A shut-off domain got the new definition.
    Redefined,
    /// The domain is live and its definition differs: redefine at its next stop.
    Pending,
    /// The instance could not be reconciled; nothing was changed for it.
    Failed(String),
}

impl Outcome {
    /// One line for the journal.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Unchanged => "unchanged".to_owned(),
            Self::Defined => "defined".to_owned(),
            Self::Redefined => "redefined".to_owned(),
            Self::Pending => "pending: redefine at next stop".to_owned(),
            Self::Failed(why) => format!("failed: {why}"),
        }
    }
}

fn ledger_path(dir: &Path, id: &InstanceId) -> PathBuf {
    dir.join("reconcile").join(format!("{}.xml", id.as_str()))
}

fn is_shut_off(state: &InstanceState) -> bool {
    matches!(state, InstanceState::Defined | InstanceState::Stopped)
}

/// Reconcile every spec; one outcome per instance, in order.
pub fn reconcile<D: Domains>(
    domains: &D,
    specs: &[InstanceSpec],
    state_dir: &Path,
) -> Vec<(InstanceId, Outcome)> {
    specs
        .iter()
        .map(|spec| (spec.id.clone(), reconcile_one(domains, spec, state_dir)))
        .collect()
}

fn reconcile_one<D: Domains>(domains: &D, spec: &InstanceSpec, state_dir: &Path) -> Outcome {
    let rendered = match domains.render(spec) {
        Ok(xml) => xml,
        Err(why) => return Outcome::Failed(why),
    };
    let path = ledger_path(state_dir, &spec.id);
    let recorded = fs::read_to_string(&path).ok();
    let state = match domains.state(&spec.id) {
        Ok(state) => state,
        Err(why) => return Outcome::Failed(why),
    };
    let same = recorded.as_deref() == Some(rendered.as_str());
    let outcome = match &state {
        None => Outcome::Defined,
        Some(_) if same => return Outcome::Unchanged,
        Some(s) if is_shut_off(s) => Outcome::Redefined,
        Some(_) => return Outcome::Pending,
    };
    if let Err(why) = domains.define(spec) {
        return Outcome::Failed(why);
    }
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        return Outcome::Failed(format!("cannot create {}: {error}", parent.display()));
    }
    // write-then-rename: a crash leaves the old record, so the next run redefines (idempotent)
    let tmp = path.with_extension("xml.tmp");
    if let Err(error) = fs::write(&tmp, &rendered).and_then(|()| fs::rename(&tmp, &path)) {
        return Outcome::Failed(format!("defined, but the record failed: {error}"));
    }
    outcome
}
