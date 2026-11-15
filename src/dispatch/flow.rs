//! The dispatcher: waiting job in, one clean instance, one ephemeral runner,
//! one job, teardown.
//!
//! Per job: the oldest waiting job of a label with a free slot is claimed;
//! admission (RAM) is asked; the slot's executor brings up a clean instance;
//! only then is an ephemeral registration minted (a start that fails burns no
//! registration); the runner runs exactly that job; teardown always follows
//! and the slot takes its next job only after it succeeded.
//!
//! Wake-ups. A freed slot and a shutdown are events. The one exception is the
//! waiting-job listing, which Forgejo only offers by request: it runs at the
//! supervisor's existing cadence (fast while work was seen, slow when idle,
//! backing off on errors) and ONLY while a slot is free. With every slot busy
//! the dispatcher sleeps until a slot frees.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::admission::{Admission, Admit};
use super::exec::{Assignment, ExecError, Executor, JobRef, RunOutcome};
use super::forge::{Forge, Registration, Scope, WaitingJob};

/// Builds the executor of one slot (by slot id).
pub type ExecutorFactory = Arc<dyn Fn(&str) -> Box<dyn Executor> + Send + Sync>;

/// One slot: its id and the name its runners register under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotCfg {
    /// The slot id (digits for host slots, an instance name for VM slots).
    pub id: String,
    /// The registered runner name.
    pub name: String,
}

/// One pool: a label, its slots, its RAM claim per job and how to run a job.
pub struct Pool {
    /// The label jobs ask for.
    pub label: String,
    /// The RAM one job of this label claims, in MiB.
    pub claim_mib: u64,
    /// The slots.
    pub slots: Vec<SlotCfg>,
    /// How a slot's executor is made.
    pub factory: ExecutorFactory,
}

/// Timing knobs.
#[derive(Debug, Clone)]
pub struct Tuning {
    /// Listing cadence while work was seen lately.
    pub poll: Duration,
    /// Listing cadence while idle.
    pub poll_idle: Duration,
    /// How long "lately" is.
    pub busy_window: Duration,
    /// How long the scope derivation is reused.
    pub scope_ttl: Duration,
    /// How long a claimed job is not offered to another slot.
    pub claim_ttl: Duration,
    /// The pause between teardown retries.
    pub teardown_retry: Duration,
    /// Back-off of a slot after the memory guard refused its start.
    pub memory_backoff: Duration,
    /// The forgejo root URL runners connect to.
    pub runner_url: String,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(1500),
            poll_idle: Duration::from_secs(4),
            busy_window: Duration::from_secs(60),
            scope_ttl: Duration::from_secs(60),
            claim_ttl: Duration::from_secs(120),
            teardown_retry: Duration::from_secs(5),
            memory_backoff: Duration::from_secs(60),
            runner_url: "https://forge.invalid/".to_owned(),
        }
    }
}

/// Counters, for the log and the tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Jobs handed to an executor.
    pub dispatched: u64,
    /// Registrations the forge refused.
    pub register_failed: u64,
    /// Unused registrations kept for the next job of the same scope and slot.
    pub spare_kept: u64,
    /// Kept registrations handed to a later job.
    pub spare_reused: u64,
    /// Unused registrations dropped (a second miss, never kept twice).
    pub spare_dropped: u64,
    /// Jobs ended because their run was cancelled.
    pub cancelled: u64,
    /// Admissions refused (wait or decline).
    pub not_admitted: u64,
}

struct State {
    busy: HashSet<String>,
    claims: HashMap<String, Instant>,
    spares: HashMap<(Scope, String), Registration>,
    scopes: Vec<Scope>,
    scopes_at: Option<Instant>,
    work_seen: Option<Instant>,
    threads: usize,
    stopping: bool,
    woken: bool,
    stats: Stats,
}

struct Inner {
    forge: Arc<dyn Forge>,
    admission: Arc<dyn Admission>,
    pools: Vec<Pool>,
    tuning: Tuning,
    state: Mutex<State>,
    cv: Condvar,
    pending_mib: AtomicU64,
    log: Box<dyn Fn(&str) + Send + Sync>,
}

/// The CI dispatcher.
pub struct Dispatcher {
    inner: Arc<Inner>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Dispatcher {
    /// A dispatcher over the ports. `log` receives one `event key=value` line
    /// per event and never a credential.
    ///
    /// # Panics
    /// Never; pool labels and slot ids are expected to be validated by the
    /// configuration loader.
    pub fn new(
        forge: Arc<dyn Forge>,
        admission: Arc<dyn Admission>,
        pools: Vec<Pool>,
        tuning: Tuning,
        log: impl Fn(&str) + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                forge,
                admission,
                pools,
                tuning,
                state: Mutex::new(State {
                    busy: HashSet::new(),
                    claims: HashMap::new(),
                    spares: HashMap::new(),
                    scopes: Vec::new(),
                    scopes_at: None,
                    work_seen: None,
                    threads: 0,
                    stopping: false,
                    woken: false,
                    stats: Stats::default(),
                }),
                cv: Condvar::new(),
                pending_mib: AtomicU64::new(0),
                log: Box::new(log),
            }),
            handles: Mutex::new(Vec::new()),
        }
    }

    /// A drain: no new job; a job in progress is never cut.
    pub fn stop(&self) {
        let mut st = lock(&self.inner.state);
        st.stopping = true;
        st.woken = true;
        self.inner.cv.notify_all();
    }

    /// The counters.
    #[must_use]
    pub fn stats(&self) -> Stats {
        lock(&self.inner.state).stats.clone()
    }

    /// Slots that are working or out of service.
    #[must_use]
    pub fn busy_slots(&self) -> Vec<String> {
        let mut v: Vec<String> = lock(&self.inner.state).busy.iter().cloned().collect();
        v.sort();
        v
    }

    /// Block until no job thread is left (jobs in progress finish first).
    pub fn wait_idle(&self) {
        let mut st = lock(&self.inner.state);
        while st.threads > 0 {
            st = self
                .inner
                .cv
                .wait(st)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(st);
        for h in lock(&self.handles).drain(..) {
            let _ = h.join();
        }
    }

    /// Serve until [`Dispatcher::stop`], then wait for the jobs in progress.
    pub fn run(&self) {
        let mut failures = 0u32;
        self.inner.say("start", &[]);
        loop {
            if lock(&self.inner.state).stopping {
                break;
            }
            let free = match self.cycle() {
                Ok(free) => {
                    failures = 0;
                    free
                }
                Err(e) => {
                    failures += 1;
                    self.inner.say(
                        "cycle-failed",
                        &[("error", e), ("failures", failures.to_string())],
                    );
                    true
                }
            };
            let wait = if free {
                self.inner
                    .interval()
                    .saturating_mul(1 << failures.min(4))
                    .min(Duration::from_secs(30))
            } else {
                // every slot is busy: nothing to ask the forge, a freed slot wakes us
                Duration::from_secs(3600)
            };
            let mut st = lock(&self.inner.state);
            if !st.woken && !st.stopping {
                st = self
                    .inner
                    .cv
                    .wait_timeout(st, wait)
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
            }
            st.woken = false;
        }
        self.wait_idle();
        self.inner.say("stop", &[]);
    }

    /// One pass: list, admit, hand out. Returns whether a slot is still free.
    ///
    /// # Errors
    /// The forge could not be asked.
    pub fn cycle(&self) -> Result<bool, String> {
        let inner = &self.inner;
        let now = Instant::now();
        let free = inner.free_slots();
        if free.is_empty() {
            return Ok(false);
        }
        let labels: Vec<String> = inner
            .pools
            .iter()
            .filter(|p| p.slots.iter().any(|s| free.contains(&s.id)))
            .map(|p| p.label.clone())
            .collect();
        let scopes = inner.scopes(now)?;
        let mut jobs: Vec<(Scope, WaitingJob)> = Vec::new();
        for scope in &scopes {
            match inner.forge.waiting_jobs(scope, &labels) {
                Ok(found) => jobs.extend(found.into_iter().map(|j| (scope.clone(), j))),
                Err(e) => inner.say(
                    "jobs-failed",
                    &[("scope", scope.to_string()), ("error", e.to_string())],
                ),
            }
        }
        jobs.sort_by_key(|(_, j)| j.id);
        {
            let mut st = lock(&inner.state);
            st.claims.retain(|_, until| *until > now);
            if !jobs.is_empty() {
                st.work_seen = Some(now);
            }
        }
        for (scope, job) in jobs {
            let Some((pool_idx, slot)) = inner.choose(&scope, &job) else {
                continue;
            };
            let pool = &inner.pools[pool_idx];
            let pending = inner.pending_mib.load(Ordering::SeqCst);
            match inner.admission.admit(&pool.label, pool.claim_mib, pending) {
                Admit::Granted => {}
                Admit::Wait(why) | Admit::Decline(why) => {
                    lock(&inner.state).stats.not_admitted += 1;
                    inner.say(
                        "not-admitted",
                        &[
                            ("label", pool.label.clone()),
                            ("job", job.handle.clone()),
                            ("why", why),
                        ],
                    );
                    continue;
                }
            }
            inner
                .pending_mib
                .fetch_add(pool.claim_mib, Ordering::SeqCst);
            {
                let mut st = lock(&inner.state);
                st.busy.insert(slot.id.clone());
                st.claims
                    .insert(job.handle.clone(), now + inner.tuning.claim_ttl);
                st.threads += 1;
                st.stats.dispatched += 1;
            }
            let worker = Arc::clone(inner);
            let handle = std::thread::spawn(move || worker.run_slot(pool_idx, &slot, &scope, &job));
            lock(&self.handles).push(handle);
        }
        Ok(!inner.free_slots().is_empty())
    }
}

impl Inner {
    fn say(&self, event: &str, fields: &[(&str, String)]) {
        let mut line = event.to_owned();
        for (k, v) in fields {
            line.push(' ');
            line.push_str(k);
            line.push('=');
            line.push_str(v);
        }
        (self.log)(&line);
    }

    fn interval(&self) -> Duration {
        let recent = lock(&self.state)
            .work_seen
            .is_some_and(|t| t.elapsed() < self.tuning.busy_window);
        if recent {
            self.tuning.poll
        } else {
            self.tuning.poll_idle
        }
    }

    fn free_slots(&self) -> HashSet<String> {
        let st = lock(&self.state);
        self.pools
            .iter()
            .flat_map(|p| p.slots.iter())
            .filter(|s| !st.busy.contains(&s.id))
            .map(|s| s.id.clone())
            .collect()
    }

    fn scopes(&self, now: Instant) -> Result<Vec<Scope>, String> {
        {
            let st = lock(&self.state);
            if let Some(at) = st.scopes_at
                && now.duration_since(at) < self.tuning.scope_ttl
                && !st.scopes.is_empty()
            {
                return Ok(st.scopes.clone());
            }
        }
        match self.forge.scopes() {
            Ok(fresh) => {
                let mut st = lock(&self.state);
                if st.scopes != fresh {
                    let names: Vec<String> = fresh.iter().map(Scope::to_string).collect();
                    drop(st);
                    self.say("scopes", &[("names", names.join(","))]);
                    st = lock(&self.state);
                }
                st.scopes = fresh.clone();
                st.scopes_at = Some(now);
                Ok(fresh)
            }
            Err(e) => {
                // keep serving the last derivation through a forge hiccup
                let st = lock(&self.state);
                if st.scopes.is_empty() {
                    Err(e.to_string())
                } else {
                    Ok(st.scopes.clone())
                }
            }
        }
    }

    /// The pool and the free slot for a job: its label's pool, preferring a
    /// slot that holds an unused registration of the job's scope.
    fn choose(&self, scope: &Scope, job: &WaitingJob) -> Option<(usize, SlotCfg)> {
        let st = lock(&self.state);
        if st.claims.contains_key(&job.handle) {
            return None;
        }
        let (idx, pool) = self
            .pools
            .iter()
            .enumerate()
            .find(|(_, p)| p.label == job.label)?;
        let free = pool.slots.iter().filter(|s| !st.busy.contains(&s.id));
        let mut first = None;
        for slot in free {
            if st.spares.contains_key(&(scope.clone(), slot.id.clone())) {
                return Some((idx, slot.clone()));
            }
            first.get_or_insert(slot);
        }
        first.map(|s| (idx, s.clone()))
    }

    fn teardown_until_clean(&self, exec: &mut dyn Executor, slot: &str) -> bool {
        let mut attempts = 0u32;
        loop {
            match exec.teardown() {
                Ok(()) => return true,
                Err(e) => {
                    attempts += 1;
                    self.say(
                        "teardown-failed",
                        &[
                            ("slot", slot.to_owned()),
                            ("attempt", attempts.to_string()),
                            ("error", e.to_string()),
                        ],
                    );
                    // a drain gives up after three tries; the slot then stays out of service
                    if lock(&self.state).stopping && attempts >= 3 {
                        return false;
                    }
                    std::thread::sleep(self.tuning.teardown_retry);
                }
            }
        }
    }

    fn finish(&self, slot: &str, release: bool) {
        let mut st = lock(&self.state);
        if release {
            st.busy.remove(slot);
        }
        st.threads -= 1;
        st.woken = true;
        self.cv.notify_all();
    }

    fn run_slot(&self, pool_idx: usize, slot: &SlotCfg, scope: &Scope, job: &WaitingJob) {
        let pool = &self.pools[pool_idx];
        let jobref = JobRef {
            handle: job.handle.clone(),
            label: pool.label.clone(),
            slot: slot.id.clone(),
        };
        let mut exec = (pool.factory)(&slot.id);
        // 1. a clean instance, BEFORE a registration is minted
        let prepared = exec.prepare(&jobref);
        self.pending_mib.fetch_sub(pool.claim_mib, Ordering::SeqCst);
        if let Err(e) = prepared {
            self.say(
                "prepare-failed",
                &[
                    ("slot", slot.id.clone()),
                    ("job", job.handle.clone()),
                    ("error", e.to_string()),
                ],
            );
            let clean = self.teardown_until_clean(&mut *exec, &slot.id);
            if matches!(e, ExecError::Prepare { memory: true, .. }) {
                std::thread::sleep(self.tuning.memory_backoff);
            }
            self.finish(&slot.id, clean);
            return;
        }
        // 2. the registration: a kept one of this scope and slot, or a fresh ephemeral one
        let key = (scope.clone(), slot.id.clone());
        let spare = lock(&self.state).spares.remove(&key);
        let (registration, reused) = if let Some(r) = spare {
            lock(&self.state).stats.spare_reused += 1;
            (r, true)
        } else {
            match self.forge.register_ephemeral(scope, &slot.name) {
                Ok(r) => (r, false),
                Err(e) => {
                    lock(&self.state).stats.register_failed += 1;
                    self.say(
                        "register-failed",
                        &[
                            ("slot", slot.id.clone()),
                            ("scope", scope.to_string()),
                            ("error", e.to_string()),
                        ],
                    );
                    let clean = self.teardown_until_clean(&mut *exec, &slot.id);
                    self.finish(&slot.id, clean);
                    return;
                }
            }
        };
        let assignment = Assignment {
            job: jobref,
            scope: scope.clone(),
            runner_url: self.tuning.runner_url.clone(),
            registration: registration.clone(),
        };
        self.say(
            if reused { "dispatch-spare" } else { "dispatch" },
            &[
                ("slot", slot.id.clone()),
                ("scope", scope.to_string()),
                ("job", job.handle.clone()),
                ("pool", pool.label.clone()),
            ],
        );
        // 3. the job
        let slot_id = slot.id.clone();
        let outcome = exec.run(&assignment, &mut |line| {
            self.say(
                "job",
                &[("slot", slot_id.clone()), ("out", line.to_owned())],
            )
        });
        // 4. the registration's fate
        match &outcome {
            RunOutcome::NoTask | RunOutcome::NotStarted(_) => {
                let mut st = lock(&self.state);
                if reused {
                    st.stats.spare_dropped += 1;
                    drop(st);
                    self.say(
                        "spare-dropped",
                        &[("slot", slot.id.clone()), ("scope", scope.to_string())],
                    );
                } else {
                    st.spares.insert(key, registration);
                    st.stats.spare_kept += 1;
                    drop(st);
                    self.say(
                        "spare-kept",
                        &[("slot", slot.id.clone()), ("scope", scope.to_string())],
                    );
                }
            }
            RunOutcome::Cancelled => {
                lock(&self.state).stats.cancelled += 1;
                self.say(
                    "cancelled",
                    &[("slot", slot.id.clone()), ("job", job.handle.clone())],
                );
            }
            RunOutcome::Finished { exit } => {
                self.say(
                    "done",
                    &[
                        ("slot", slot.id.clone()),
                        ("job", job.handle.clone()),
                        ("exit", exit.to_string()),
                    ],
                );
            }
        }
        // 5. teardown, whatever happened; the slot is free only when it worked
        let clean = self.teardown_until_clean(&mut *exec, &slot.id);
        self.finish(&slot.id, clean);
    }
}
