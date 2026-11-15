//! The production VM driver against a fake host and a fake streaming guest
//! agent: no real host, no real guest.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cdrv::{
    CpuClaim, DeviceAddress, DeviceFacts, DeviceFilter, DiskReset, Event, EventId, EventLog,
    EventStream, ExecChunk, ExecOutcome, ExecResult, ExecStream, GuestAgent, GuestExec,
    GuestHealth, GuestOs, HostDevices, HostInstances, InstanceClass, InstanceId, InstanceKind,
    InstanceSpec, InstanceState, MemoryClaim, Severity, ShutdownMode, Source, StopMode, Subject,
    Timestamp,
};
use cwld::dispatch::exec::{
    Assignment, Executor, Flow, JobRef, RunOutcome, VmDriver, VmError, VmExecutor, VmExit,
};
use cwld::dispatch::forge::{Registration, Scope, ScopeKind};
use cwld::dispatch::secret::Secret;
use cwld::dispatch::vmdriver::{NO_TASK_EXIT, RuntimeVmDriver, SharedGuest, VmSettings};
use cwld::{ClassCatalog, Runtime};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cwld-vmdriver-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// --- the fake host ------------------------------------------------------------------------------------------------------------

#[derive(Default)]
struct FakeHost {
    states: Arc<Mutex<BTreeMap<String, InstanceState>>>,
    events: Mutex<Vec<Event>>,
    calls: Mutex<Vec<String>>,
    stop_fails: Mutex<bool>,
}

impl FakeHost {
    fn calls(&self) -> Vec<String> {
        lock(&self.calls).clone()
    }
    fn push(&self, id: &InstanceId, kind: &str) {
        let mut events = lock(&self.events);
        let seq = events.len() as u64;
        events.push(Event {
            id: EventId(format!("test-host/{seq}")),
            at: Timestamp::from_millis(now_ms()),
            host: "test-host".to_owned(),
            source: Source::Clnx,
            kind: kind.to_owned(),
            subject: Subject::Instance(id.clone()),
            severity: Severity::Info,
            seq,
            data: serde_json::json!({}),
            trace: None,
        });
    }
}

struct Delivered {
    events: std::vec::IntoIter<Event>,
    states: Arc<Mutex<BTreeMap<String, InstanceState>>>,
}

impl Iterator for Delivered {
    type Item = Event;
    fn next(&mut self) -> Option<Event> {
        let event = self.events.next()?;
        if let Subject::Instance(id) = &event.subject {
            let state = match event.kind.as_str() {
                "instance.running" => Some(InstanceState::Running),
                "instance.stopped" => Some(InstanceState::Stopped),
                _ => None,
            };
            if let Some(state) = state {
                lock(&self.states).insert(id.as_str().to_owned(), state);
            }
        }
        Some(event)
    }
}

impl HostInstances for FakeHost {
    fn list(&self) -> cdrv::Result<Vec<(InstanceId, InstanceState)>> {
        Ok(Vec::new())
    }
    fn define(&self, _spec: &InstanceSpec) -> cdrv::Result<()> {
        Ok(())
    }
    fn start(&self, id: &InstanceId) -> cdrv::Result<()> {
        lock(&self.calls).push(format!("start:{id}"));
        lock(&self.states).insert(id.as_str().to_owned(), InstanceState::Starting);
        self.push(id, "instance.running");
        Ok(())
    }
    fn stop(&self, id: &InstanceId, mode: StopMode) -> cdrv::Result<()> {
        lock(&self.calls).push(format!("stop:{id}:{mode:?}"));
        if *lock(&self.stop_fails) {
            return Err("injected stop failure".into());
        }
        lock(&self.states).insert(id.as_str().to_owned(), InstanceState::Stopping);
        self.push(id, "instance.stopped");
        Ok(())
    }
    fn destroy(&self, _id: &InstanceId) -> cdrv::Result<()> {
        Ok(())
    }
    fn state(&self, id: &InstanceId) -> cdrv::Result<InstanceState> {
        Ok(lock(&self.states)
            .get(id.as_str())
            .cloned()
            .unwrap_or(InstanceState::Defined))
    }
    fn events(&self) -> cdrv::Result<EventStream> {
        Ok(Box::new(Delivered {
            events: lock(&self.events).clone().into_iter(),
            states: Arc::clone(&self.states),
        }))
    }
}

impl HostDevices for FakeHost {
    fn enumerate(&self, _filter: DeviceFilter) -> cdrv::Result<Vec<DeviceFacts>> {
        Ok(Vec::new())
    }
    fn bind_passthrough(&self, _dev: &DeviceAddress) -> cdrv::Result<()> {
        Ok(())
    }
    fn unbind_passthrough(&self, _dev: &DeviceAddress) -> cdrv::Result<()> {
        Ok(())
    }
    fn reset(&self, _dev: &DeviceAddress) -> cdrv::Result<()> {
        Ok(())
    }
}

impl DiskReset for FakeHost {
    fn reset_to_clean(&self, instance: &InstanceId) -> cdrv::Result<()> {
        lock(&self.calls).push(format!("disk-reset:{instance}"));
        Ok(())
    }
}

// --- the fake streaming guest agent -------------------------------------------------------------------------------------------

struct Streamed {
    chunks: Vec<(ExecStream, &'static [u8])>,
    exit: i32,
}

#[derive(Default)]
struct FakeGuest {
    script: Mutex<Option<Streamed>>,
    requests: Mutex<Vec<GuestExec>>,
    delivered: Mutex<usize>,
}

impl FakeGuest {
    fn with(chunks: Vec<(ExecStream, &'static [u8])>, exit: i32) -> Arc<Self> {
        let guest = Self::default();
        *lock(&guest.script) = Some(Streamed { chunks, exit });
        Arc::new(guest)
    }
}

impl GuestAgent for FakeGuest {
    fn health(&self) -> cdrv::Result<GuestHealth> {
        Err("not used".into())
    }
    fn shutdown(&self, _mode: ShutdownMode) -> cdrv::Result<()> {
        Ok(())
    }
    fn exec(&self, _req: &GuestExec) -> cdrv::Result<ExecResult> {
        Err("the driver must stream".into())
    }
    fn exec_stream(
        &self,
        req: &GuestExec,
        sink: &mut dyn FnMut(ExecChunk) -> ControlFlow<()>,
    ) -> cdrv::Result<ExecOutcome> {
        lock(&self.requests).push(req.clone());
        let script = lock(&self.script).take().ok_or("no script")?;
        for (stream, bytes) in script.chunks {
            *lock(&self.delivered) += 1;
            if sink(ExecChunk {
                stream,
                bytes: bytes.to_vec(),
            })
            .is_break()
            {
                return Ok(ExecOutcome::Cancelled);
            }
        }
        Ok(ExecOutcome::Exited(script.exit))
    }
    fn free_page_reporting(&self, _enable: bool) -> cdrv::Result<()> {
        Ok(())
    }
}

// --- the rig ------------------------------------------------------------------------------------------------------------------

fn rig(guest: &Arc<FakeGuest>) -> (RuntimeVmDriver<FakeHost>, Arc<FakeHost>) {
    let dir = temp();
    let mut runtime = Runtime::new(&dir, ClassCatalog::defaults()).unwrap();
    runtime.register(InstanceSpec {
        id: InstanceId::from("ci-win-0"),
        kind: InstanceKind::Vm,
        guest_os: GuestOs::Windows,
        memory: MemoryClaim {
            min_mib: 1024,
            max_mib: 8 * 1024,
            balloon: true,
        },
        cpu: CpuClaim { quota_cores: 4.0 },
        class: InstanceClass::Ci,
        devices: Vec::new(),
        disks: Vec::new(),
        vcpus: None,
        nic: None,
        console: false,
        identity: None,
    });
    let host = Arc::new(FakeHost::default());
    lock(&host.states).insert("ci-win-0".to_owned(), InstanceState::Defined);
    let agent: SharedGuest = Arc::clone(guest) as SharedGuest;
    let driver = RuntimeVmDriver::new(
        runtime,
        EventLog::new("test-host", Source::Cwld, 1),
        Arc::clone(&host),
        move |_| Arc::clone(&agent),
        VmSettings::default(),
    );
    (driver, host)
}

fn assignment() -> Assignment {
    Assignment {
        job: JobRef {
            handle: "aaaaaaaa-0000-0000-0000-000000000001".to_owned(),
            label: "windows".to_owned(),
            slot: "ci-win-0".to_owned(),
        },
        scope: Scope {
            kind: ScopeKind::Org,
            name: "o1".to_owned(),
        },
        runner_url: "https://forge.example/".to_owned(),
        registration: Registration {
            uuid: "00000000-0000-0000-0000-000000000001".to_owned(),
            token: Secret::new("single-use-token"),
        },
    }
}

const CANCEL: &[u8] = b"unauthenticated: unregistered runner\n";

// --- the specification --------------------------------------------------------------------------------------------------------

#[test]
fn chunks_become_lines_and_the_token_travels_on_stdin_only() {
    let guest = FakeGuest::with(
        vec![
            (ExecStream::Stdout, b"first li"),
            (ExecStream::Stdout, b"ne\nsecond\nthird, unfinished"),
            (ExecStream::Stderr, b"oops\n"),
        ],
        0,
    );
    let (driver, _) = rig(&guest);
    let mut lines = Vec::new();
    let exit = driver
        .run_job(
            "ci-win-0",
            &assignment(),
            Duration::from_secs(20),
            &mut |l| {
                lines.push(l.to_owned());
                Flow::Continue
            },
        )
        .unwrap();
    assert_eq!(exit, VmExit::Code(0));
    assert_eq!(lines, ["first line", "second", "oops", "third, unfinished"]);
    let request = lock(&guest.requests)[0].clone();
    assert_eq!(
        request.stdin.as_deref(),
        Some(b"single-use-token".as_slice())
    );
    assert!(
        !request.args.join(" ").contains("single-use-token"),
        "the token is in no argument"
    );
    assert!(request.args.join(" ").contains("--pickup-s 20"));
    assert!(!format!("{request:?}").contains("single-use-token"));
}

#[test]
fn a_cancel_line_ends_the_guest_session_at_once() {
    let guest = FakeGuest::with(
        vec![
            (ExecStream::Stdout, b"working\n"),
            (ExecStream::Stderr, CANCEL),
            (ExecStream::Stdout, b"never delivered\n"),
        ],
        0,
    );
    let (driver, _) = rig(&guest);
    let mut exec = VmExecutor::new(Arc::new(driver), "ci-win-0", Duration::from_secs(20));
    // the executor owns the cancel rule; the driver only obeys Flow::Abort
    let mut seen = Vec::new();
    let outcome = exec.run(&assignment(), &mut |l| seen.push(l.to_owned()));
    assert_eq!(outcome, RunOutcome::Cancelled);
    assert_eq!(*lock(&guest.delivered), 2, "the third chunk was never sent");
    assert_eq!(seen.len(), 2);
}

#[test]
fn the_no_task_exit_status_keeps_the_registration() {
    let guest = FakeGuest::with(Vec::new(), NO_TASK_EXIT);
    let (driver, _) = rig(&guest);
    let mut exec = VmExecutor::new(Arc::new(driver), "ci-win-0", Duration::from_secs(20));
    assert_eq!(exec.run(&assignment(), &mut |_| {}), RunOutcome::NoTask);
}

#[test]
fn a_runner_status_is_the_jobs_status() {
    let guest = FakeGuest::with(Vec::new(), 3);
    let (driver, _) = rig(&guest);
    let mut exec = VmExecutor::new(Arc::new(driver), "ci-win-0", Duration::from_secs(20));
    assert_eq!(
        exec.run(&assignment(), &mut |_| {}),
        RunOutcome::Finished { exit: 3 }
    );
}

#[test]
fn a_job_is_clean_start_run_stop_reset() {
    let guest = FakeGuest::with(vec![(ExecStream::Stdout, b"done\n")], 0);
    let (driver, host) = rig(&guest);
    driver.ensure_clean("ci-win-0").unwrap();
    driver.start("ci-win-0").unwrap();
    driver
        .run_job(
            "ci-win-0",
            &assignment(),
            Duration::from_secs(20),
            &mut |_| Flow::Continue,
        )
        .unwrap();
    driver.stop_and_reset("ci-win-0").unwrap();
    assert_eq!(
        host.calls(),
        [
            "start:ci-win-0",
            "stop:ci-win-0:Clean",
            "disk-reset:ci-win-0"
        ]
    );
}

#[test]
fn a_failed_stop_skips_the_reset_and_reports() {
    let guest = FakeGuest::with(Vec::new(), 0);
    let (driver, host) = rig(&guest);
    driver.start("ci-win-0").unwrap();
    *lock(&host.stop_fails) = true;
    let error = driver.stop_and_reset("ci-win-0").unwrap_err();
    assert!(matches!(error, VmError::Failed(_)));
    assert!(
        !host.calls().iter().any(|c| c.starts_with("disk-reset")),
        "a guest that may still run is never reset"
    );
}
