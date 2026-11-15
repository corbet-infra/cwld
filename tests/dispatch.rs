//! The CI dispatcher's specification: the scenarios of the Bash/Python tests it
//! replaces (`runner-slot`, `runner-vm-slot`, `runner-dispatch`), with fakes for
//! Forgejo, the unit runner, the VM driver and admission. Synthetic names only.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cwld::dispatch::admission::{Admission, Admit, ConductorAdmission};
use cwld::dispatch::config::CiConfig;
use cwld::dispatch::exec::{
    Assignment, ExecError, Executor, Flow, HostExecutor, HostSettings, JobRef, RunOutcome,
    VmDriver, VmError, VmExecutor, VmExit,
};
use cwld::dispatch::flow::{Dispatcher, ExecutorFactory, Pool, SlotCfg, Tuning};
use cwld::dispatch::forge::{
    Forge, ForgeClient, ForgeError, Registration, Scope, ScopeKind, WaitingJob,
};
use cwld::dispatch::http::{CurlHttp, Http, HttpRequest};
use cwld::dispatch::secret::Secret;
use cwld::dispatch::units::{RunningUnit, UnitEvent, UnitRunner, UnitSpec};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cwld-dispatch-test-{}-{}-{name}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

const HANDLE_A: &str = "aaaaaaaa-0000-0000-0000-000000000001";
const HANDLE_B: &str = "bbbbbbbb-0000-0000-0000-000000000002";
const HANDLE_C: &str = "cccccccc-0000-0000-0000-000000000003";

fn org(name: &str) -> Scope {
    Scope {
        kind: ScopeKind::Org,
        name: name.to_owned(),
    }
}

fn job(id: u64, handle: &str, label: &str) -> WaitingJob {
    WaitingJob {
        id,
        handle: handle.to_owned(),
        label: label.to_owned(),
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct FakeForge {
    jobs: Mutex<Vec<(Scope, WaitingJob)>>,
    registered: Mutex<Vec<(String, String)>>, // (scope, runner name)
    fail_register: Mutex<bool>,
    counter: AtomicU32,
    known: Mutex<std::collections::BTreeSet<Scope>>,
}

impl FakeForge {
    fn with(jobs: Vec<(Scope, WaitingJob)>) -> Arc<Self> {
        let known = jobs.iter().map(|(s, _)| s.clone()).collect();
        Arc::new(Self {
            jobs: Mutex::new(jobs),
            known: Mutex::new(known),
            ..Self::default()
        })
    }
    fn add(&self, scope: Scope, j: WaitingJob) {
        lock(&self.known).insert(scope.clone());
        lock(&self.jobs).push((scope, j));
    }
    fn registrations(&self) -> usize {
        lock(&self.registered).len()
    }
}

impl Forge for FakeForge {
    fn scopes(&self) -> Result<Vec<Scope>, ForgeError> {
        let mut scopes: Vec<Scope> = lock(&self.known).iter().cloned().collect();
        if scopes.is_empty() {
            scopes.push(org("o1"));
        }
        Ok(scopes)
    }
    fn waiting_jobs(
        &self,
        scope: &Scope,
        labels: &[String],
    ) -> Result<Vec<WaitingJob>, ForgeError> {
        Ok(lock(&self.jobs)
            .iter()
            .filter(|(s, j)| s == scope && labels.contains(&j.label))
            .map(|(_, j)| j.clone())
            .collect())
    }
    fn register_ephemeral(&self, scope: &Scope, name: &str) -> Result<Registration, ForgeError> {
        if *lock(&self.fail_register) {
            return Err(ForgeError::Transport("forge unwell".to_owned()));
        }
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        lock(&self.registered).push((scope.name.clone(), name.to_owned()));
        Ok(Registration {
            uuid: format!("00000000-0000-0000-0000-{n:012}"),
            token: Secret::new(format!("single-use-token-{n}")),
        })
    }
}

struct FakeAdmission {
    answers: Mutex<VecDeque<Admit>>,
    seen: Mutex<Vec<(u64, u64)>>,
}

impl FakeAdmission {
    fn granting() -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(VecDeque::new()),
            seen: Mutex::new(Vec::new()),
        })
    }
    fn script(answers: Vec<Admit>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into()),
            seen: Mutex::new(Vec::new()),
        })
    }
}

impl Admission for FakeAdmission {
    fn admit(&self, _label: &str, claim_mib: u64, pending_mib: u64) -> Admit {
        lock(&self.seen).push((claim_mib, pending_mib));
        lock(&self.answers).pop_front().unwrap_or(Admit::Granted)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Behaviour {
    Exit(i32),
    /// Runs until killed or released.
    Hang,
}

#[derive(Clone)]
struct Script {
    lines: Vec<String>,
    /// The runner took the task: a directory appears below the work directory.
    pick: bool,
    behaviour: Behaviour,
}

#[derive(Default)]
struct UnitState {
    started: Vec<UnitSpec>,
    token_seen: Vec<(String, u32)>, // (content, mode) of the credential file at start
    config_seen: Vec<String>,
    killed: Vec<String>,
    kill_flag: bool,
    release_flag: bool,
    gone_calls: usize,
    workdirs: Vec<PathBuf>,
}

struct FakeUnits {
    state: Arc<Mutex<UnitState>>,
    scripts: Mutex<VecDeque<Script>>,
    /// `gone` answers false this many times (a cgroup that takes rounds to empty).
    stubborn: usize,
}

impl FakeUnits {
    fn new(scripts: Vec<Script>, stubborn: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(UnitState::default())),
            scripts: Mutex::new(scripts.into()),
            stubborn,
        })
    }
    fn release(&self) {
        lock(&self.state).release_flag = true;
    }
}

struct FakeRunning {
    lines: VecDeque<String>,
    behaviour: Behaviour,
    state: Arc<Mutex<UnitState>>,
}

impl RunningUnit for FakeRunning {
    fn next(&mut self, wait: Duration) -> UnitEvent {
        if let Some(l) = self.lines.pop_front() {
            return UnitEvent::Line(l);
        }
        match self.behaviour {
            Behaviour::Exit(_) => UnitEvent::Closed,
            Behaviour::Hang => {
                let until = std::time::Instant::now() + wait.min(Duration::from_secs(20));
                while std::time::Instant::now() < until {
                    {
                        let st = lock(&self.state);
                        if st.kill_flag || st.release_flag {
                            return UnitEvent::Closed;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                UnitEvent::Timeout
            }
        }
    }
    fn wait(&mut self) -> std::io::Result<i32> {
        Ok(match self.behaviour {
            Behaviour::Exit(c) => c,
            Behaviour::Hang => {
                if lock(&self.state).kill_flag {
                    137
                } else {
                    0
                }
            }
        })
    }
}

impl UnitRunner for FakeUnits {
    fn start(&self, spec: &UnitSpec) -> std::io::Result<Box<dyn RunningUnit>> {
        let script = lock(&self.scripts).pop_front().unwrap_or(Script {
            lines: vec![],
            pick: true,
            behaviour: Behaviour::Exit(0),
        });
        let mut st = lock(&self.state);
        // the credential the unit would load
        if let Some(p) = spec
            .properties
            .iter()
            .find_map(|p| p.strip_prefix("LoadCredential=reg:"))
        {
            let mode = std::fs::metadata(p)?.permissions().mode() & 0o777;
            st.token_seen.push((std::fs::read_to_string(p)?, mode));
        }
        if spec.argv.iter().any(|a| a == "--config")
            && let Some(c) = spec.argv.last()
        {
            st.config_seen.push(std::fs::read_to_string(c)?);
        }
        st.started.push(spec.clone());
        st.workdirs.push(spec.workdir.clone());
        st.kill_flag = false;
        st.release_flag = false;
        drop(st);
        if script.pick {
            std::fs::create_dir_all(spec.workdir.join("work").join("task-1"))?;
        }
        Ok(Box::new(FakeRunning {
            lines: script.lines.into(),
            behaviour: script.behaviour,
            state: Arc::clone(&self.state),
        }))
    }
    fn kill_all(&self, unit: &str) -> std::io::Result<()> {
        let mut st = lock(&self.state);
        st.killed.push(unit.to_owned());
        st.kill_flag = true;
        Ok(())
    }
    fn gone(&self, _unit: &str) -> bool {
        let mut st = lock(&self.state);
        st.gone_calls += 1;
        let gone = st.gone_calls > self.stubborn;
        if gone {
            // the unit's ExecStopPost removes the runner's tree
            for dir in &st.workdirs {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
        gone
    }
    fn sweep(&self, _prefix: &str) -> std::io::Result<Vec<String>> {
        Ok(Vec::new())
    }
}

fn settings(dir: &std::path::Path, pickup_ms: u64) -> HostSettings {
    HostSettings {
        runner: PathBuf::from("/usr/bin/forgejo-runner"),
        template: serde_json::json!({"server": {}, "host": {}, "cache": {}}),
        state_dir: dir.to_path_buf(),
        state_root: dir.join("var-lib"),
        state_directory: "runner".to_owned(),
        scrub_bin: PathBuf::from("/usr/bin/cwld"),
        pre: Vec::new(),
        post: Vec::new(),
        pickup: Duration::from_millis(pickup_ms),
        properties: vec!["Delegate=yes".to_owned(), "User=jobs".to_owned()],
        environment: vec![
            ("HOME".to_owned(), "/var/lib/jobs".to_owned()),
            (
                "AWS_SHARED_CREDENTIALS_FILE".to_owned(),
                "{credentials}/sccache-aws".to_owned(),
            ),
            ("SCCACHE_SERVER_PORT".to_owned(), "1".to_owned()),
        ],
        slot_environment: vec![("SCCACHE_SERVER_PORT".to_owned(), "4230".to_owned())],
        credentials_root: PathBuf::from("/run/credentials"),
        kill_rounds: 5,
        kill_pause: Duration::from_millis(1),
    }
}

fn tuning() -> Tuning {
    Tuning {
        teardown_retry: Duration::from_millis(5),
        memory_backoff: Duration::from_millis(1),
        scope_ttl: Duration::ZERO,
        runner_url: "https://forge.example/".to_owned(),
        ..Tuning::default()
    }
}

struct Rig {
    dispatcher: Arc<Dispatcher>,
    forge: Arc<FakeForge>,
    units: Arc<FakeUnits>,
    admission: Arc<FakeAdmission>,
    log: Arc<Mutex<Vec<String>>>,
    dir: PathBuf,
}

impl Rig {
    fn host(
        slots: &[&str],
        scripts: Vec<Script>,
        stubborn: usize,
        jobs: Vec<(Scope, WaitingJob)>,
        admission: Arc<FakeAdmission>,
        pickup_ms: u64,
    ) -> Self {
        Self::host_with(slots, scripts, stubborn, jobs, admission, pickup_ms, |_| {})
    }

    fn host_with(
        slots: &[&str],
        scripts: Vec<Script>,
        stubborn: usize,
        jobs: Vec<(Scope, WaitingJob)>,
        admission: Arc<FakeAdmission>,
        pickup_ms: u64,
        tweak: impl FnOnce(&mut HostSettings),
    ) -> Self {
        let dir = temp("host");
        let forge = FakeForge::with(jobs);
        let units = FakeUnits::new(scripts, stubborn);
        let mut cfg = settings(&dir, pickup_ms);
        tweak(&mut cfg);
        let u = Arc::clone(&units);
        let factory: ExecutorFactory = Arc::new(move |slot: &str| -> Box<dyn Executor> {
            Box::new(HostExecutor::new(u.clone(), cfg.clone(), slot))
        });
        let pool = Pool {
            label: "linux".to_owned(),
            aliases: vec!["heavy".to_owned()],
            claim_mib: 1000,
            slots: slots
                .iter()
                .map(|s| SlotCfg {
                    id: (*s).to_owned(),
                    name: format!("slot-{s}"),
                })
                .collect(),
            factory,
        };
        let log = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&log);
        let dispatcher = Arc::new(Dispatcher::new(
            forge.clone(),
            admission.clone(),
            vec![pool],
            tuning(),
            move |l| lock(&sink).push(l.to_owned()),
        ));
        Self {
            dispatcher,
            forge,
            units,
            admission,
            log,
            dir,
        }
    }
    fn run_cycle(&self) {
        self.dispatcher.cycle().expect("cycle");
        self.dispatcher.wait_idle();
    }
    fn log_text(&self) -> String {
        lock(&self.log).join("\n")
    }
    fn secrets_leaked(&self) -> bool {
        let text = self.log_text();
        let st = lock(&self.units.state);
        let in_spec = st.started.iter().any(|s| {
            s.argv.join(" ").contains("single-use-token")
                || s.properties.join(" ").contains("single-use-token")
        });
        text.contains("single-use-token") || in_spec
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn plain(lines: &[&str], exit: i32) -> Script {
    Script {
        lines: lines.iter().map(|s| (*s).to_owned()).collect(),
        pick: true,
        behaviour: Behaviour::Exit(exit),
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Host slots
// ---------------------------------------------------------------------------------------------------------------------

#[test]
fn a_job_runs_in_a_fresh_unit_and_leaves_nothing() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&["job output"], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert_eq!(st.started.len(), 1);
    let spec = &st.started[0];
    assert_eq!(spec.unit, format!("cwld-ci-0-{HANDLE_A}"));
    assert_eq!(
        &spec.argv[1..5],
        ["one-job", "--wait", "--handle", HANDLE_A]
    );
    assert!(
        spec.properties.contains(&"Delegate=yes".to_owned()),
        "the deployment's properties reach the unit"
    );
    // the job's environment: the pool's values, the slot's override, the unit's credentials directory
    let env = |n: &str| {
        spec.environment
            .iter()
            .find(|(k, _)| k == n)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(env("HOME").as_deref(), Some("/var/lib/jobs"));
    assert_eq!(env("SCCACHE_SERVER_PORT").as_deref(), Some("4230"));
    assert_eq!(
        env("AWS_SHARED_CREDENTIALS_FILE"),
        Some(format!(
            "/run/credentials/cwld-ci-0-{HANDLE_A}.service/sccache-aws"
        ))
    );
    assert_eq!(
        spec.environment.len(),
        3,
        "an override replaces, it does not repeat"
    );
    // the runner's tree is the unit's own state directory, removed by the unit as its account
    let has = |p: String| spec.properties.contains(&p);
    assert!(has(format!(
        "StateDirectory=runner/slots/0/job.{HANDLE_A} runner/cache/0"
    )));
    assert!(has(format!(
        "ExecStopPost=/usr/bin/cwld ci scrub {}",
        rig.dir
            .join(format!("var-lib/runner/slots/0/job.{HANDLE_A}"))
            .display()
    )));
    // the single-use token is a 0600 credential file, in no argument and no log line
    assert_eq!(
        st.token_seen,
        vec![("single-use-token-0".to_owned(), 0o600)]
    );
    drop(st);
    assert!(!rig.secrets_leaked());
    assert!(
        !rig.dir
            .join("var-lib/runner/slots/0")
            .join(format!("job.{HANDLE_A}"))
            .exists(),
        "the job directory is gone"
    );
    assert!(
        rig.dispatcher.busy_slots().is_empty(),
        "the slot is free again"
    );
    assert_eq!(rig.forge.registrations(), 1);
    assert!(rig.log_text().contains("job slot=0 out=job output"));
}

#[test]
fn the_runner_config_has_one_connection_and_no_token() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    let seen = Arc::new(Mutex::new(lock(&rig.units.state).config_seen[0].clone()));
    let cfg: serde_json::Value = serde_json::from_str(&lock(&seen)).expect("config is JSON (YAML)");
    let conn = &cfg["server"]["connections"]["o1"];
    assert_eq!(conn["url"], "https://forge.example/");
    assert_eq!(conn["uuid"], "00000000-0000-0000-0000-000000000000");
    assert_eq!(
        conn["token_url"],
        format!("file:/run/credentials/cwld-ci-0-{HANDLE_A}.service/reg")
    );
    assert!(!lock(&seen).contains("single-use-token"));
}

#[test]
fn the_oldest_job_goes_first_and_a_freed_slot_is_reused() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0), plain(&[], 0)],
        0,
        vec![
            (org("o1"), job(7, HANDLE_B, "linux")),
            (org("o1"), job(3, HANDLE_A, "linux")),
        ],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    // one slot: only the oldest job (id 3) ran
    assert_eq!(lock(&rig.units.state).started.len(), 1);
    assert_eq!(
        lock(&rig.units.state).started[0].unit,
        format!("cwld-ci-0-{HANDLE_A}")
    );
    // the slot is free again after teardown; the next cycle takes the other job on the same slot
    lock(&rig.forge.jobs).retain(|(_, j)| j.handle != HANDLE_A);
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert_eq!(st.started.len(), 2);
    assert_eq!(st.started[1].unit, format!("cwld-ci-0-{HANDLE_B}"));
}

#[test]
fn a_cancelled_run_ends_its_cgroup_at_once_and_keeps_no_registration() {
    let hang = Script {
        lines: vec!["level=warning msg=\"ReportState error: unauthenticated: unregistered runner\" task_id=1".to_owned()],
        pick: true,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0"],
        vec![hang],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        5000,
    );
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert!(
        !st.killed.is_empty(),
        "cgroup.kill on the unregistered-runner line"
    );
    assert!(st.killed[0].contains(HANDLE_A));
    drop(st);
    assert_eq!(rig.dispatcher.stats().cancelled, 1);
    assert_eq!(
        rig.dispatcher.stats().spare_kept,
        0,
        "a cancelled job's registration is gone, nothing to keep"
    );
    assert!(rig.dispatcher.busy_slots().is_empty());
}

#[test]
fn a_job_nobody_picks_up_is_ended_and_its_registration_kept_for_the_next_job_of_the_scope() {
    let nobody = Script {
        lines: vec![],
        pick: false,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0"],
        vec![nobody.clone(), plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        30,
    );
    rig.run_cycle();
    assert_eq!(rig.dispatcher.stats().spare_kept, 1);
    assert!(
        !lock(&rig.units.state).killed.is_empty(),
        "the idle runner process was ended"
    );
    // the next job of the SAME scope reuses the registration: the forge is not asked again
    lock(&rig.forge.jobs).clear();
    rig.forge.add(org("o1"), job(2, HANDLE_B, "linux"));
    rig.run_cycle();
    assert_eq!(
        rig.forge.registrations(),
        1,
        "one registration served both jobs"
    );
    assert_eq!(rig.dispatcher.stats().spare_reused, 1);
    assert!(rig.log_text().contains("dispatch-spare"));
}

#[test]
fn a_spare_that_finds_no_task_again_is_dropped_not_kept_twice() {
    let nobody = Script {
        lines: vec![],
        pick: false,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0"],
        vec![nobody.clone(), nobody.clone(), plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        20,
    );
    rig.run_cycle(); // registration 0 kept
    lock(&rig.forge.jobs).clear();
    rig.forge.add(org("o1"), job(2, HANDLE_B, "linux"));
    rig.run_cycle(); // the spare is handed over and again finds nothing: dropped
    assert_eq!(rig.dispatcher.stats().spare_dropped, 1);
    lock(&rig.forge.jobs).clear();
    rig.forge.add(org("o1"), job(3, HANDLE_C, "linux"));
    rig.run_cycle(); // a fresh registration
    assert_eq!(rig.forge.registrations(), 2);
}

#[test]
fn a_spare_belongs_to_one_scope() {
    let nobody = Script {
        lines: vec![],
        pick: false,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0"],
        vec![nobody, plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        20,
    );
    rig.run_cycle();
    lock(&rig.forge.jobs).clear();
    rig.forge.add(org("o2"), job(2, HANDLE_B, "linux"));
    rig.run_cycle();
    assert_eq!(
        rig.forge.registrations(),
        2,
        "another scope never gets o1's registration"
    );
    assert_eq!(rig.dispatcher.stats().spare_reused, 0);
}

#[test]
fn a_cgroup_that_takes_rounds_to_empty_is_killed_again_until_it_is_gone() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0)],
        2,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert!(
        st.killed.len() >= 3,
        "kill is repeated each round: {}",
        st.killed.len()
    );
    drop(st);
    assert!(
        rig.dispatcher.busy_slots().is_empty(),
        "the slot returns once the cgroup is gone"
    );
}

#[test]
fn a_cgroup_that_never_goes_keeps_the_slot_out_of_service() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0), plain(&[], 0)],
        usize::MAX,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
    );
    let d = Arc::clone(&rig.dispatcher);
    let t = std::thread::spawn(move || {
        d.cycle().unwrap();
    });
    t.join().unwrap();
    // give the job thread time to try; it retries teardown forever, so do not wait_idle
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        rig.dispatcher.busy_slots(),
        vec!["0".to_owned()],
        "still out of service"
    );
    // a drain gives up after three tries and lets the process end; the slot stays out of service
    rig.dispatcher.stop();
    rig.dispatcher.wait_idle();
    assert_eq!(rig.dispatcher.busy_slots(), vec!["0".to_owned()]);
}

#[test]
fn a_drain_never_cuts_a_job_in_progress() {
    let hang = Script {
        lines: vec![],
        pick: true,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0"],
        vec![hang],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        5000,
    );
    let d = Arc::clone(&rig.dispatcher);
    let served = std::thread::spawn(move || d.run());
    // wait until the job runs
    for _ in 0..500 {
        if !lock(&rig.units.state).started.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    rig.dispatcher.stop(); // SIGTERM
    std::thread::sleep(Duration::from_millis(80));
    assert!(!served.is_finished(), "run() waits for the job");
    assert!(
        lock(&rig.units.state).killed.is_empty(),
        "the job was not cut"
    );
    rig.units.release();
    served.join().unwrap();
    assert!(rig.dispatcher.busy_slots().is_empty());
    assert!(rig.log_text().contains("\nstop") || rig.log_text().ends_with("stop"));
}

#[test]
fn an_idle_drain_ends_at_once() {
    let rig = Rig::host(&["0"], vec![], 0, vec![], FakeAdmission::granting(), 100);
    let d = Arc::clone(&rig.dispatcher);
    let served = std::thread::spawn(move || d.run());
    std::thread::sleep(Duration::from_millis(30));
    rig.dispatcher.stop();
    served.join().unwrap();
}

// ---------------------------------------------------------------------------------------------------------------------
// Admission and registration order
// ---------------------------------------------------------------------------------------------------------------------

#[test]
fn a_refused_admission_starts_nothing_and_mints_nothing() {
    let admission = FakeAdmission::script(vec![Admit::Wait("not enough free".to_owned())]);
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        admission,
        100,
    );
    rig.run_cycle();
    assert!(lock(&rig.units.state).started.is_empty());
    assert_eq!(rig.forge.registrations(), 0);
    assert_eq!(rig.dispatcher.stats().not_admitted, 1);
    // memory frees: the job (still waiting at the forge) is admitted at the next cycle
    rig.run_cycle();
    assert_eq!(lock(&rig.units.state).started.len(), 1);
}

#[test]
fn claims_granted_but_not_yet_resident_count_against_the_next_admission() {
    let hang = Script {
        lines: vec![],
        pick: true,
        behaviour: Behaviour::Hang,
    };
    let rig = Rig::host(
        &["0", "1"],
        vec![hang.clone(), hang],
        0,
        vec![
            (org("o1"), job(1, HANDLE_A, "linux")),
            (org("o1"), job(2, HANDLE_B, "linux")),
        ],
        FakeAdmission::granting(),
        5000,
    );
    rig.dispatcher.cycle().unwrap();
    // both were admitted in one pass: the second saw the first's claim as pending (0 once resident, 1000 before)
    let seen = lock(&rig.admission.seen).clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], (1000, 0));
    assert!(
        seen[1].1 == 0 || seen[1].1 == 1000,
        "pending is the first claim until its instance is up: {seen:?}"
    );
    for _ in 0..1000 {
        if lock(&rig.units.state).started.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    rig.units.release();
    rig.dispatcher.wait_idle();
}

struct FailingPrepare;

impl Executor for FailingPrepare {
    fn prepare(&mut self, _job: &JobRef) -> Result<(), ExecError> {
        Err(ExecError::Prepare {
            reason: "no clean instance".to_owned(),
            memory: true,
        })
    }
    fn run(&mut self, _a: &Assignment, _log: &mut dyn FnMut(&str)) -> RunOutcome {
        unreachable!("a failed prepare never runs")
    }
    fn teardown(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
}

#[test]
fn a_start_that_fails_burns_no_registration() {
    let forge = FakeForge::with(vec![(org("o1"), job(1, HANDLE_A, "linux"))]);
    let factory: ExecutorFactory = Arc::new(|_| Box::new(FailingPrepare));
    let pool = Pool {
        label: "linux".to_owned(),
        aliases: Vec::new(),
        claim_mib: 10,
        slots: vec![SlotCfg {
            id: "0".to_owned(),
            name: "slot-0".to_owned(),
        }],
        factory,
    };
    let d = Dispatcher::new(
        forge.clone(),
        FakeAdmission::granting(),
        vec![pool],
        tuning(),
        |_| {},
    );
    d.cycle().unwrap();
    d.wait_idle();
    assert_eq!(forge.registrations(), 0);
    assert!(d.busy_slots().is_empty());
}

#[test]
fn a_refused_registration_is_not_retried_every_cycle() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
    );
    *lock(&rig.forge.fail_register) = true;
    rig.run_cycle();
    rig.run_cycle(); // the claim holds the job back for its lifetime
    assert_eq!(rig.dispatcher.stats().register_failed, 1);
    assert!(
        lock(&rig.units.state).started.is_empty(),
        "no instance is left running for a job without a registration"
    );
    assert!(rig.dispatcher.busy_slots().is_empty());
}

#[test]
fn a_job_for_another_label_is_nobodys() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&[], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "windows"))],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    assert!(lock(&rig.units.state).started.is_empty());
}

// ---------------------------------------------------------------------------------------------------------------------
// VM slots (class ci)
// ---------------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct FakeVm {
    calls: Mutex<Vec<String>>,
    start_error: Mutex<Option<VmError>>,
    exit: Mutex<Option<VmExit>>,
    lines: Mutex<Vec<String>>,
    reset_failures: Mutex<u32>,
    saw_abort: Mutex<bool>,
}

impl FakeVm {
    fn call(&self, c: &str) {
        lock(&self.calls).push(c.to_owned());
    }
    fn calls(&self) -> Vec<String> {
        lock(&self.calls).clone()
    }
}

impl VmDriver for FakeVm {
    fn ensure_clean(&self, i: &str) -> Result<(), VmError> {
        self.call(&format!("ensure_clean {i}"));
        Ok(())
    }
    fn start(&self, i: &str) -> Result<(), VmError> {
        self.call(&format!("start {i}"));
        match lock(&self.start_error).take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    fn run_job(
        &self,
        i: &str,
        a: &Assignment,
        _p: Duration,
        on_line: &mut dyn FnMut(&str) -> Flow,
    ) -> Result<VmExit, VmError> {
        self.call(&format!("run_job {i} {}", a.job.handle));
        for l in lock(&self.lines).clone() {
            if on_line(&l) == Flow::Abort {
                *lock(&self.saw_abort) = true;
                return Ok(VmExit::Code(143));
            }
        }
        Ok((*lock(&self.exit)).unwrap_or(VmExit::Code(0)))
    }
    fn stop_and_reset(&self, i: &str) -> Result<(), VmError> {
        self.call(&format!("stop_and_reset {i}"));
        let mut n = lock(&self.reset_failures);
        if *n > 0 {
            *n -= 1;
            return Err(VmError::Failed("zfs busy".to_owned()));
        }
        Ok(())
    }
}

fn vm_rig(vm: Arc<FakeVm>, jobs: Vec<(Scope, WaitingJob)>) -> (Arc<Dispatcher>, Arc<FakeForge>) {
    let forge = FakeForge::with(jobs);
    let factory: ExecutorFactory =
        Arc::new(move |_| Box::new(VmExecutor::new(vm.clone(), "ci-0", Duration::from_secs(30))));
    let pool = Pool {
        label: "macos".to_owned(),
        aliases: Vec::new(),
        claim_mib: 8192,
        slots: vec![SlotCfg {
            id: "mac-0".to_owned(),
            name: "dev-0.mac.vm".to_owned(),
        }],
        factory,
    };
    (
        Arc::new(Dispatcher::new(
            forge.clone(),
            FakeAdmission::granting(),
            vec![pool],
            tuning(),
            |_| {},
        )),
        forge,
    )
}

#[test]
fn a_vm_job_is_clean_start_run_stop_reset() {
    let vm = Arc::new(FakeVm::default());
    let (d, forge) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    assert_eq!(
        vm.calls(),
        [
            "ensure_clean ci-0",
            "start ci-0",
            &format!("run_job ci-0 {HANDLE_A}"),
            "stop_and_reset ci-0"
        ]
    );
    assert_eq!(forge.registrations(), 1);
    assert!(d.busy_slots().is_empty());
}

#[test]
fn a_failing_vm_job_is_still_reset() {
    let vm = Arc::new(FakeVm::default());
    *lock(&vm.exit) = Some(VmExit::Code(1));
    let (d, _f) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    assert_eq!(vm.calls().last().unwrap(), "stop_and_reset ci-0");
}

#[test]
fn a_cancelled_vm_job_ends_the_guest_session_then_resets() {
    let vm = Arc::new(FakeVm::default());
    *lock(&vm.lines) = vec![
        "x".to_owned(),
        "ReportState error: unauthenticated: unregistered runner".to_owned(),
        "never reached".to_owned(),
    ];
    let (d, _f) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    assert!(*lock(&vm.saw_abort));
    assert_eq!(d.stats().cancelled, 1);
    assert_eq!(vm.calls().last().unwrap(), "stop_and_reset ci-0");
}

#[test]
fn a_vm_with_no_task_keeps_its_registration_and_is_reset() {
    let vm = Arc::new(FakeVm::default());
    *lock(&vm.exit) = Some(VmExit::NoTask);
    let (d, _f) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    assert_eq!(d.stats().spare_kept, 1);
    assert_eq!(vm.calls().last().unwrap(), "stop_and_reset ci-0");
}

#[test]
fn the_memory_guards_refusal_mints_nothing_and_the_clone_is_reset() {
    let vm = Arc::new(FakeVm::default());
    *lock(&vm.start_error) = Some(VmError::NoMemory);
    let (d, forge) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    assert_eq!(forge.registrations(), 0);
    assert!(!vm.calls().iter().any(|c| c.starts_with("run_job")));
    assert_eq!(vm.calls().last().unwrap(), "stop_and_reset ci-0");
}

#[test]
fn a_failed_reset_keeps_the_clone_out_of_service_until_it_works() {
    let vm = Arc::new(FakeVm::default());
    *lock(&vm.reset_failures) = 2;
    let (d, _f) = vm_rig(vm.clone(), vec![(org("o1"), job(1, HANDLE_A, "macos"))]);
    d.cycle().unwrap();
    d.wait_idle();
    let resets = vm
        .calls()
        .iter()
        .filter(|c| c.starts_with("stop_and_reset"))
        .count();
    assert_eq!(resets, 3, "retried until it worked");
    assert!(d.busy_slots().is_empty());
}

// ---------------------------------------------------------------------------------------------------------------------
// The forge over HTTP (curl against a local fake)
// ---------------------------------------------------------------------------------------------------------------------

/// Serve `routes` (method + path prefix -> status, body) on a local port for `count` connections.
fn fake_forge_server(
    routes: Vec<(&'static str, u16, String)>,
    count: usize,
) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        let mut seen = Vec::new();
        for _ in 0..count {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = stream.read(&mut chunk).unwrap();
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(end) = text.find("\r\n\r\n") {
                    let len = text
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if buf.len() >= end + 4 + len {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&buf).to_string();
            let first = request.lines().next().unwrap_or_default().to_owned();
            let key = {
                let mut parts = first.split(' ');
                format!(
                    "{} {}",
                    parts.next().unwrap_or_default(),
                    parts.next().unwrap_or_default()
                )
            };
            let (status, body) = routes
                .iter()
                .find(|(prefix, _, _)| key.starts_with(prefix))
                .map_or((404, "{}".to_owned()), |(_, s, b)| (*s, b.clone()));
            let reply = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).unwrap();
            seen.push(request);
        }
        seen
    });
    (format!("http://127.0.0.1:{}", addr.port()), handle)
}

fn client(base: &str) -> ForgeClient {
    ForgeClient::new(
        Arc::new(CurlHttp::default()),
        &format!("{base}/api/v1"),
        &format!("{base}/"),
        Secret::new("READ-SCOPE-TOKEN"),
        "v13.1.0",
        true,
    )
}

#[test]
fn scopes_are_the_owners_namespace_and_the_organisations_the_owner_owns() {
    let routes = vec![
        (
            "GET /api/v1/user/orgs",
            200,
            r#"[{"username":"orga"},{"username":"orgb"}]"#.to_owned(),
        ),
        (
            "GET /api/v1/users/me/orgs/orga/permissions",
            200,
            r#"{"is_owner":true}"#.to_owned(),
        ),
        (
            "GET /api/v1/users/me/orgs/orgb/permissions",
            200,
            r#"{"is_owner":false}"#.to_owned(),
        ),
        ("GET /api/v1/user", 200, r#"{"login":"me"}"#.to_owned()),
    ];
    let (base, server) = fake_forge_server(routes, 4);
    let scopes = client(&base).scopes().expect("scopes");
    assert_eq!(
        scopes,
        vec![
            Scope {
                kind: ScopeKind::User,
                name: "me".to_owned()
            },
            org("orga")
        ]
    );
    let seen = server.join().unwrap();
    assert!(
        seen.iter()
            .all(|r| r.contains("Authorization: token READ-SCOPE-TOKEN")),
        "every read carries the credential"
    );
}

#[test]
fn only_waiting_single_label_jobs_with_a_handle_are_listed() {
    let body = format!(
        r#"[{{"id":5,"status":"waiting","runs_on":["linux"],"handle":"{HANDLE_A}"}},
            {{"id":6,"status":"blocked","runs_on":["linux"],"handle":"{HANDLE_B}"}},
            {{"id":7,"status":"waiting","runs_on":["linux","macos"],"handle":"{HANDLE_C}"}},
            {{"id":8,"status":"waiting","runs_on":["linux"],"handle":"not a handle"}}]"#
    );
    let (base, server) = fake_forge_server(
        vec![(
            "GET /api/v1/orgs/orga/actions/runners/jobs?labels=linux",
            200,
            body,
        )],
        1,
    );
    let jobs = client(&base)
        .waiting_jobs(&org("orga"), &["linux".to_owned()])
        .expect("jobs");
    assert_eq!(jobs, vec![job(5, HANDLE_A, "linux")]);
    server.join().unwrap();
}

#[test]
fn a_registration_is_ephemeral_and_its_secrets_stay_off_the_wire_headers_it_must_not_use() {
    let routes = vec![
        (
            "GET /api/v1/orgs/orga/actions/runners/registration-token",
            200,
            r#"{"token":"REGTOKEN"}"#.to_owned(),
        ),
        (
            "POST /api/actions/runner.v1.RunnerService/Register",
            200,
            r#"{"runner":{"uuid":"11111111-1111-1111-1111-111111111111","token":"SINGLEUSE"}}"#
                .to_owned(),
        ),
    ];
    let (base, server) = fake_forge_server(routes, 2);
    let reg = client(&base)
        .register_ephemeral(&org("orga"), "slot-0")
        .expect("register");
    assert_eq!(reg.uuid, "11111111-1111-1111-1111-111111111111");
    assert_eq!(reg.token.expose(), "SINGLEUSE");
    assert_eq!(format!("{:?}", reg), format!("{:?}", reg.clone()));
    assert!(
        !format!("{reg:?}").contains("SINGLEUSE"),
        "Debug never shows the token"
    );
    let seen = server.join().unwrap();
    let post = &seen[1];
    assert!(post.contains(r#""ephemeral":true"#));
    assert!(post.contains(r#""name":"slot-0""#));
    assert!(post.contains("Connect-Protocol-Version: 1"));
    assert!(
        !post.contains("READ-SCOPE-TOKEN"),
        "the read credential does not go to Register"
    );
}

#[test]
fn a_non_2xx_answer_is_an_error_and_a_dead_port_is_a_transport_error() {
    let (base, server) = fake_forge_server(vec![("GET /api/v1/user", 403, "{}".to_owned())], 1);
    let err = client(&base).scopes().unwrap_err();
    assert!(
        matches!(err, ForgeError::Status { status: 403, .. }),
        "{err}"
    );
    server.join().unwrap();
    let dead = CurlHttp::default().send(&HttpRequest {
        method: "GET".to_owned(),
        url: "http://127.0.0.1:1/".to_owned(),
        headers: vec![],
        body: None,
    });
    assert!(dead.is_err());
}

#[test]
fn a_request_never_shows_its_headers_in_debug() {
    let r = HttpRequest {
        method: "GET".to_owned(),
        url: "http://x/".to_owned(),
        headers: vec![("Authorization".to_owned(), "token SECRET".to_owned())],
        body: None,
    };
    let text = format!("{r:?}");
    assert!(!text.contains("SECRET"));
}

// ---------------------------------------------------------------------------------------------------------------------
// Admission by the conductor, and the configuration
// ---------------------------------------------------------------------------------------------------------------------

fn snapshot(available_mib: u64, psi_some: f32) -> std::io::Result<cdct::MemorySnapshot> {
    cdct::MemorySnapshot::parse(&cdct::RawFacts {
        meminfo: format!("MemTotal: {} kB\nMemAvailable: {} kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n", 128 * 1024 * 1024, available_mib * 1024),
        arcstats: None,
        pressure_memory: format!("some avg10={psi_some:.2} avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"),
    })
    .map_err(|e| std::io::Error::other(e.to_string()))
}

#[test]
fn the_conductors_memory_rules_decide_and_a_pending_claim_counts() {
    let adm = ConductorAdmission::new(cdct::Conductor::with_defaults(), || snapshot(4096, 0.0));
    assert_eq!(adm.admit("linux", 2048, 0), Admit::Granted);
    assert!(
        matches!(adm.admit("linux", 2048, 4096), Admit::Wait(_)),
        "granted-but-not-resident claims are subtracted"
    );
    assert!(matches!(
        adm.admit("linux", 1_000_000_000, 0),
        Admit::Decline(_) | Admit::Wait(_)
    ));
    let broken = ConductorAdmission::new(cdct::Conductor::with_defaults(), || {
        Err(std::io::Error::other("no /proc"))
    });
    assert!(
        matches!(broken.admit("linux", 1, 0), Admit::Wait(_)),
        "no facts, no grant"
    );
}

#[test]
fn the_configuration_is_validated_at_load() {
    let ok = r#"
state_dir = "/var/lib/cwld-ci"
[forge]
api = "https://forge.example/api/v1"
runner_url = "https://forge.example/"
runner_version = "v13.1.0"
[[pool]]
label = "linux"
kind = "host"
claim_mib = 2048
runner = "/bin/forgejo-runner"
template = "/etc/runner.json"
run_as = "forgejo-runner"
slots = [{ id = "0", name = "slot-0" }, { id = "1", name = "slot-1" }]
"#;
    let cfg = CiConfig::parse(ok).expect("valid");
    assert_eq!(cfg.pools[0].slots.len(), 2);
    assert!(
        CiConfig::parse(&ok.replace("\"1\"", "\"0\"")).is_err(),
        "a repeated slot"
    );
    assert!(
        CiConfig::parse(&ok.replace("https://forge.example/\"", "http://forge.example/\""))
            .is_err(),
        "plain http"
    );
    assert!(
        CiConfig::parse(&ok.replace("kind = \"host\"", "kind = \"jail\"")).is_err(),
        "an unknown kind"
    );
    assert!(
        CiConfig::parse(&ok.replace("runner = \"/bin/forgejo-runner\"\n", "")).is_err(),
        "a host pool without a runner"
    );
    assert!(
        CiConfig::parse(&format!("{ok}\nsurprise = 1")).is_err(),
        "unknown keys are refused"
    );
}

#[test]
fn a_line_break_in_a_header_is_refused_before_curl_runs() {
    let err = CurlHttp {
        curl: "/nonexistent/curl".to_owned(),
        timeout_s: 1,
    }
    .send(&HttpRequest {
        method: "GET".to_owned(),
        url: "http://127.0.0.1:1/".to_owned(),
        headers: vec![("X-A".to_owned(), "v\r\nX-Injected: 1".to_owned())],
        body: None,
    })
    .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

// --- review fixes ------------------------------------------------------------------------------------------------------------

#[test]
fn output_that_is_not_utf8_neither_ends_the_stream_nor_hides_the_cancel_line() {
    use cwld::dispatch::units::SystemdRun;
    use std::os::unix::fs::PermissionsExt;
    let dir = temp("fake-systemd-run");
    let script = dir.join("systemd-run");
    std::fs::write(
        &script,
        "#!/bin/sh\nprintf 'bad \\377\\376 bytes\\n'\nprintf 'unauthenticated: unregistered runner\\n'\n",
    )
    .expect("script");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let runner = SystemdRun {
        systemd_run: script.display().to_string(),
        ..SystemdRun::default()
    };
    let spec = UnitSpec {
        unit: "cwld-ci-0-x".to_owned(),
        argv: vec!["true".to_owned()],
        workdir: dir.clone(),
        properties: Vec::new(),
        environment: Vec::new(),
        stdin: Some(b"fed through\n".to_vec()),
    };
    let mut unit = runner.start(&spec).expect("start");
    let mut lines = Vec::new();
    while let UnitEvent::Line(l) = unit.next(Duration::from_secs(5)) {
        lines.push(l);
    }
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[0].starts_with("bad "));
    assert!(cwld::dispatch::cancel::is_cancel_line(&lines[1]));
}

#[test]
fn a_sweep_and_a_kill_go_through_systemctl_for_cwld_ci_units_only() {
    use cwld::dispatch::units::SystemdRun;
    use std::os::unix::fs::PermissionsExt;
    let root = temp("cgroups");
    let slice = root.join("system.slice");
    for name in [
        "cwld-ci-0-aaaa.service",
        "cwld-ci-1-bbbb.service",
        "sshd.service",
    ] {
        std::fs::create_dir_all(slice.join(name)).expect("cgroup");
    }
    let calls = root.join("calls");
    let script = root.join("systemctl");
    std::fs::write(
        &script,
        format!("#!/bin/sh\necho \"$@\" >> {}\n", calls.display()),
    )
    .expect("script");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let runner = SystemdRun {
        systemctl: script.display().to_string(),
        cgroup_root: root,
        ..SystemdRun::default()
    };
    let swept = runner.sweep("cwld-ci-").expect("sweep");
    assert_eq!(swept, ["cwld-ci-0-aaaa", "cwld-ci-1-bbbb"]);
    let log = std::fs::read_to_string(&calls).expect("calls");
    assert_eq!(log.lines().count(), 2, "{log}");
    assert!(
        log.contains("kill --kill-whom=all --signal=SIGKILL cwld-ci-0-aaaa.service"),
        "{log}"
    );
    assert!(!log.contains("sshd"));
    // a unit that is already gone costs no call
    runner.kill_all("cwld-ci-9-gone").expect("gone is fine");
    assert_eq!(std::fs::read_to_string(&calls).unwrap().lines().count(), 2);
}

#[test]
fn a_host_pool_names_its_account_and_the_state_directory_stays_relative() {
    let base = r#"
state_dir = "/var/lib/cwld-ci"
[forge]
api = "https://forge.example/api/v1"
runner_url = "https://forge.example/"
runner_version = "v13.1.0"
[[pool]]
label = "linux"
kind = "host"
claim_mib = 2048
runner = "/bin/forgejo-runner"
template = "/etc/runner.json"
run_as = "forgejo-runner"
environment = { HOME = "/var/lib/forgejo-runner" }
slots = [{ id = "0", name = "slot-0", environment = { SCCACHE_SERVER_PORT = "4230" } }]
"#;
    assert!(CiConfig::parse(base).is_ok());
    assert!(
        CiConfig::parse(&base.replace("run_as = \"forgejo-runner\"\n", "")).is_err(),
        "a host pool without an account would run its jobs as root"
    );
    assert!(CiConfig::parse(&format!("state_directory = \"../etc\"\n{base}")).is_err());
    assert!(
        CiConfig::parse(&base.replace(
            "run_as = \"forgejo-runner\"",
            "run_as = \"forgejo-runner\"\nproperties = [\"User=x\"]"
        ))
        .is_err()
    );
    assert!(CiConfig::parse(&base.replace("HOME =", "\"1BAD\" =")).is_err());
}

#[test]
fn the_scrub_command_removes_job_directories_and_nothing_else() {
    let dir = temp("scrub-cmd");
    let job = dir.join("job.abc");
    std::fs::create_dir_all(job.join("work/deep")).unwrap();
    std::fs::write(job.join("work/deep/f"), "x").unwrap();
    let bin = env!("CARGO_BIN_EXE_cwld");
    let run = |p: &std::path::Path| {
        std::process::Command::new(bin)
            .args(["ci", "scrub"])
            .arg(p)
            .status()
            .unwrap()
    };
    assert!(!run(&dir).success(), "not a job directory");
    assert!(dir.exists());
    assert!(run(&job).success());
    assert!(!job.exists());
}

// --- pre- and post-job commands (the Windows test clone) --------------------------------------------------------------------

fn clone_hooks(c: &mut HostSettings) {
    c.pre = vec![
        "/bin/systemctl".to_owned(),
        "start".to_owned(),
        "wake-clone".to_owned(),
    ];
    c.post = vec![
        "/bin/systemctl".to_owned(),
        "start".to_owned(),
        "roll-back-clone".to_owned(),
    ];
}

fn hooked(scripts: Vec<Script>) -> Rig {
    Rig::host_with(
        &["6"],
        scripts,
        0,
        vec![(org("o1"), job(1, HANDLE_A, "linux"))],
        FakeAdmission::granting(),
        100,
        clone_hooks,
    )
}

#[test]
fn the_clone_is_woken_before_the_job_and_rolled_back_after_it() {
    let rig = hooked(vec![
        plain(&[], 0),
        plain(&["test output"], 0),
        plain(&[], 0),
    ]);
    rig.run_cycle();
    let st = lock(&rig.units.state);
    let names: Vec<_> = st.started.iter().map(|s| s.unit.clone()).collect();
    assert_eq!(
        names,
        [
            format!("cwld-ci-6-{HANDLE_A}-pre"),
            format!("cwld-ci-6-{HANDLE_A}"),
            format!("cwld-ci-6-{HANDLE_A}-post"),
        ]
    );
    assert_eq!(st.started[0].argv[2], "wake-clone");
    assert_eq!(st.started[2].argv[2], "roll-back-clone");
    assert!(
        st.started[0].properties.contains(&"User=jobs".to_owned()),
        "same account and sandbox as the job"
    );
    drop(st);
    assert!(rig.dispatcher.busy_slots().is_empty());
}

#[test]
fn a_failed_wake_mints_no_registration_and_still_rolls_the_clone_back() {
    let rig = hooked(vec![plain(&[], 1), plain(&[], 0)]);
    rig.run_cycle();
    assert_eq!(rig.forge.registrations(), 0, "the job stays with the forge");
    let st = lock(&rig.units.state);
    assert_eq!(st.started.len(), 2);
    assert!(st.started[1].unit.ends_with("-post"));
    drop(st);
    assert!(rig.log_text().contains("prepare-failed"));
    assert!(rig.dispatcher.busy_slots().is_empty());
}

#[test]
fn the_slot_stays_busy_until_the_roll_back_works() {
    let rig = hooked(vec![
        plain(&[], 0),
        plain(&[], 0),
        plain(&[], 1),
        plain(&[], 0),
    ]);
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert_eq!(st.started.len(), 4, "the post command ran twice");
    drop(st);
    assert!(rig.log_text().contains("teardown-failed"));
    assert!(
        rig.dispatcher.busy_slots().is_empty(),
        "free only after it succeeded"
    );
}

// --- VM pools as commands of the pool's account -------------------------------------------------------------------------------

fn vm_assignment() -> Assignment {
    Assignment {
        job: JobRef {
            handle: HANDLE_A.to_owned(),
            label: "macos".to_owned(),
            slot: "mac-0".to_owned(),
        },
        scope: org("o1"),
        runner_url: "https://forge.example/".to_owned(),
        registration: Registration {
            uuid: "00000000-0000-0000-0000-000000000001".to_owned(),
            token: Secret::new("single-use-token"),
        },
    }
}

fn unit_vm(
    scripts: Vec<Script>,
    standby: bool,
) -> (cwld::dispatch::unitvm::UnitVmDriver, Arc<FakeUnits>) {
    use cwld::dispatch::unitvm::{UnitVmDriver, VmCommands};
    use cwld::dispatch::vmdriver::VmSettings;
    let units = FakeUnits::new(scripts, 0);
    let cmd = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let driver = UnitVmDriver::new(
        units.clone(),
        VmCommands {
            transport: cmd(&["/bin/vm-dev", "{slot}", "pipe", "--"]),
            ensure_clean: cmd(&["/bin/vm-dev", "{slot}", "ensure-clean"]),
            wake: cmd(&["/bin/vm-dev", "{slot}", "wake"]),
            sync: cmd(&["/bin/vm-dev", "{slot}", "sync"]),
            reset: cmd(&["/bin/vm-dev", "{slot}", "reset"]),
            standby: if standby {
                cmd(&["/bin/vm-dev", "{slot}", "standby"])
            } else {
                Vec::new()
            },
        },
        VmSettings::default(),
        vec!["User=jobs".to_owned()],
        Vec::new(),
        temp("vm-workdir"),
        |_| {},
    );
    (driver, units)
}

#[test]
fn a_vm_job_is_commands_of_the_pool_account_with_the_payload_on_stdin() {
    let (driver, units) = unit_vm(
        vec![
            plain(&[], 0),
            plain(&[], 0),
            plain(&[], 0),
            plain(&["job ran"], 0),
            plain(&[], 0),
        ],
        false,
    );
    driver.ensure_clean("mac-0").unwrap();
    driver.start("mac-0").unwrap();
    let mut lines = Vec::new();
    let exit = driver
        .run_job(
            "mac-0",
            &vm_assignment(),
            Duration::from_secs(30),
            &mut |l| {
                lines.push(l.to_owned());
                Flow::Continue
            },
        )
        .unwrap();
    driver.stop_and_reset("mac-0").unwrap();
    assert_eq!(exit, VmExit::Code(0));
    assert_eq!(lines, ["job ran"]);
    let st = lock(&units.state);
    let verbs: Vec<_> = st.started.iter().map(|s| s.argv[2..].join(" ")).collect();
    assert_eq!(verbs[0], "ensure-clean");
    assert_eq!(verbs[1], "wake");
    assert_eq!(verbs[2], "sync");
    assert_eq!(verbs[4], "reset");
    let session = &st.started[3];
    assert_eq!(
        session.argv,
        ["/bin/vm-dev", "mac-0", "pipe", "--", "bash", "-s"]
    );
    let stdin = String::from_utf8(session.stdin.clone().unwrap()).unwrap();
    assert!(stdin.contains("vm-guest-job"));
    assert!(
        !stdin.contains("single-use-token"),
        "only base64 in the payload"
    );
    assert!(
        st.started
            .iter()
            .all(|s| s.properties.contains(&"User=jobs".to_owned())
                && s.unit.starts_with("cwld-ci-vm-mac-0-")),
        "every operation is a unit of the pool's account, named for the polkit rule"
    );
    assert!(
        st.started
            .iter()
            .all(|s| !s.argv.join(" ").contains("single-use-token")
                && !s.properties.join(" ").contains("single-use-token"))
    );
}

#[test]
fn a_refused_wake_is_the_memory_guard_and_no_task_is_status_75() {
    use cwld::dispatch::exec::VmError;
    let (driver, _) = unit_vm(vec![plain(&[], 75)], false);
    assert!(matches!(driver.start("mac-0"), Err(VmError::NoMemory)));
    let (driver, _) = unit_vm(vec![plain(&[], 75)], false);
    let exit = driver
        .run_job(
            "mac-0",
            &vm_assignment(),
            Duration::from_secs(30),
            &mut |_| Flow::Continue,
        )
        .unwrap();
    assert_eq!(exit, VmExit::NoTask);
}

#[test]
fn the_cancel_line_kills_the_guest_session_unit() {
    let (driver, units) = unit_vm(
        vec![Script {
            lines: vec!["unauthenticated: unregistered runner".to_owned()],
            pick: false,
            behaviour: Behaviour::Hang,
        }],
        false,
    );
    let mut exec =
        cwld::dispatch::exec::VmExecutor::new(Arc::new(driver), "mac-0", Duration::from_secs(30));
    let outcome = exec.run(&vm_assignment(), &mut |_| {});
    assert_eq!(outcome, RunOutcome::Cancelled);
    assert_eq!(lock(&units.state).killed.len(), 1);
}

#[test]
fn a_failed_standby_rolls_the_clone_back_again() {
    let (driver, units) = unit_vm(vec![plain(&[], 0), plain(&[], 1), plain(&[], 0)], true);
    driver.stop_and_reset("mac-0").unwrap();
    let st = lock(&units.state);
    let verbs: Vec<_> = st.started.iter().map(|s| s.argv[2].clone()).collect();
    assert_eq!(verbs, ["reset", "standby", "reset"]);
}

#[test]
fn a_vm_pool_needs_its_account_template_and_commands() {
    let base = r#"
state_dir = "/var/lib/cwld-ci"
[forge]
api = "https://forge.example/api/v1"
runner_url = "https://forge.example/"
runner_version = "v13.1.0"
[[pool]]
label = "macos"
kind = "vm"
claim_mib = 24576
run_as = "forgejo-runner"
template = "/etc/runner.json"
slots = [{ id = "mac-0", name = "slot-mac-0" }]
[pool.vm]
transport = ["/bin/vm-dev", "{slot}", "pipe", "--"]
wake = ["/bin/vm-dev", "{slot}", "wake"]
reset = ["/bin/vm-dev", "{slot}", "reset"]
"#;
    assert!(CiConfig::parse(base).is_ok());
    assert!(CiConfig::parse(&base.replace("run_as = \"forgejo-runner\"\n", "")).is_err());
    assert!(
        CiConfig::parse(&base.replace("reset = [\"/bin/vm-dev\", \"{slot}\", \"reset\"]\n", ""))
            .is_err()
    );
}

#[test]
fn the_job_unit_starts_in_the_configured_slice() {
    use cwld::dispatch::units::SystemdRun;
    use std::os::unix::fs::PermissionsExt;
    let dir = temp("fake-systemd-run-slice");
    let args = dir.join("args");
    let script = dir.join("systemd-run");
    std::fs::write(
        &script,
        format!("#!/bin/sh\necho \"$@\" > {}\n", args.display()),
    )
    .expect("script");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let runner = SystemdRun {
        systemd_run: script.display().to_string(),
        slice: "ci.slice".to_owned(),
        ..SystemdRun::default()
    };
    let spec = UnitSpec {
        unit: "cwld-ci-0-x".to_owned(),
        argv: vec!["true".to_owned()],
        workdir: dir.clone(),
        properties: Vec::new(),
        environment: Vec::new(),
        stdin: None,
    };
    let mut unit = runner.start(&spec).expect("start");
    while let UnitEvent::Line(_) = unit.next(Duration::from_secs(5)) {}
    let recorded = std::fs::read_to_string(&args).expect("args");
    assert!(recorded.contains("--slice=ci.slice"), "{recorded}");
}

#[test]
fn a_job_asking_for_an_alias_of_the_pool_runs_in_it() {
    let rig = Rig::host(
        &["0"],
        vec![plain(&["job output"], 0)],
        0,
        vec![(org("o1"), job(1, HANDLE_A, "heavy"))],
        FakeAdmission::granting(),
        100,
    );
    rig.run_cycle();
    let st = lock(&rig.units.state);
    assert_eq!(
        st.started.len(),
        1,
        "the alias label reaches the pool's slot"
    );
    assert_eq!(st.started[0].unit, format!("cwld-ci-0-{HANDLE_A}"));
}
