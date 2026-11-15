//! The plan journal: what a plan was doing when the runtime died.
//!
//! Before each step the runtime writes which step it is about to attempt; after
//! the step succeeds it writes that the step completed. A journal left behind
//! therefore says exactly which steps completed and which single step was in
//! flight; the step in flight is reported as uncertain, never assumed done or
//! undone. A plan that finishes (or fails in a way the caller sees) removes its
//! journal.

use std::path::{Path, PathBuf};

use cdct::{KernelOwner, PlanStep, SwitchPlan};
use cdrv::Timestamp;
use serde::{Deserialize, Serialize};

use crate::store::{self, StoreError};

/// One plan's progress on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanJournal {
    /// The kernel the plan was switching the card to.
    pub target: KernelOwner,
    /// Every step of the plan, in order.
    pub steps: Vec<PlanStep>,
    /// How many leading steps completed.
    pub completed: usize,
    /// The step that was attempted when the journal was last written.
    pub in_flight: Option<usize>,
    /// When the plan began.
    pub started_at: Timestamp,
}

impl PlanJournal {
    /// The steps that completed.
    #[must_use]
    pub fn executed(&self) -> &[PlanStep] {
        let end = self.completed.min(self.steps.len());
        self.steps.get(..end).unwrap_or_default()
    }

    /// The one step that may have partially run.
    #[must_use]
    pub fn uncertain(&self) -> Option<&PlanStep> {
        self.in_flight.and_then(|index| self.steps.get(index))
    }

    /// The steps nothing was attempted for.
    #[must_use]
    pub fn not_started(&self) -> &[PlanStep] {
        let start = match self.in_flight {
            Some(index) => index.saturating_add(1),
            None => self.completed,
        };
        self.steps.get(start..).unwrap_or_default()
    }
}

/// The plan journals of one state directory, one file per target kernel.
#[derive(Debug, Clone)]
pub struct Journals {
    dir: PathBuf,
}

impl Journals {
    /// Open (creating if needed) the journal directory.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let dir = dir.into();
        store::ensure_dir(&dir)?;
        Ok(Self { dir })
    }

    /// The directory the journals live in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Start a journal for a plan, replacing any journal for the same target.
    pub fn begin(&self, plan: &SwitchPlan, at: Timestamp) -> Result<PlanJournal, StoreError> {
        let journal = PlanJournal {
            target: plan.target.clone(),
            steps: plan.steps.clone(),
            completed: 0,
            in_flight: None,
            started_at: at,
        };
        self.save(&journal)?;
        Ok(journal)
    }

    /// Record that `index` is about to be attempted.
    pub fn before_step(&self, journal: &mut PlanJournal, index: usize) -> Result<(), StoreError> {
        journal.completed = index;
        journal.in_flight = Some(index);
        self.save(journal)
    }

    /// Record that `index` completed.
    pub fn after_step(&self, journal: &mut PlanJournal, index: usize) -> Result<(), StoreError> {
        journal.completed = index.saturating_add(1);
        journal.in_flight = None;
        self.save(journal)
    }

    /// Remove the journal; `Ok(false)` means there was none.
    pub fn finish(&self, journal: &PlanJournal) -> Result<bool, StoreError> {
        store::remove_file(&self.path(&journal.target))
    }

    /// One target's journal, when a plan for it did not finish.
    pub fn load(&self, target: &KernelOwner) -> Result<Option<PlanJournal>, StoreError> {
        store::read_json(&self.path(target))
    }

    /// Every unfinished journal, in filename order.
    pub fn scan(&self) -> Result<Vec<PlanJournal>, StoreError> {
        let mut journals = Vec::new();
        for path in store::json_files(&self.dir)? {
            if let Some(journal) = store::read_json(&path)? {
                journals.push(journal);
            }
        }
        Ok(journals)
    }

    fn save(&self, journal: &PlanJournal) -> Result<(), StoreError> {
        store::write_json(&self.path(&journal.target), journal)
    }

    fn path(&self, target: &KernelOwner) -> PathBuf {
        let key = match target {
            KernelOwner::Host => "host".to_owned(),
            KernelOwner::Guest(instance) => {
                format!("guest-{}", store::file_key(instance.as_str()))
            }
        };
        self.dir.join(format!("plan-{key}.json"))
    }
}
