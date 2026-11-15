//! The runtime's specification: one test binary, read as sentences.
//!
//! Every scenario uses a fake `cdrv` driver, synthetic names and fixture text.
//! No test touches a real host, opens a device or runs a host tool.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cdct::holders::read_table;
use cdct::{
    DisplacedTenant, GpuLadder, GpuState, GpuTenant, Holder, HolderState, HolderStore, HolderTable,
    InstanceObservation, KernelOwner, MemorySnapshot, PlanStep, Policy, RawFacts, SwitchOutcome,
    SwitchPlan, TenantState, plan_switch,
};
use cdrv::{
    CpuClaim, DeviceAddress, DeviceClass, DeviceFacts, DeviceFilter, Event, EventId, EventLog,
    EventStream, ExecResult, GuestAgent, GuestExec, GuestHealth, GuestOs, HolderId, HostDevices,
    HostGpu, HostInstances, InstanceClass, InstanceId, InstanceKind, InstanceSpec, InstanceState,
    LeaseId, MemoryClaim, Severity, ShutdownMode, Source, StopMode, Subject, Timestamp,
};
use cwld::{
    CiJob, ClassCatalog, CleanOutcome, Clock, DirtyReason, DiscardHandoff, DiskReset,
    DisplacedHandoff, HandoffError, HandoffStatus, Hibernation, HolderHandoff, Hosts, Lifecycle,
    MemoryRule, ParseError, Runtime, RuntimeError, StepError, TransitionError, Trigger,
    ValidationError,
};
use serde_json::json;

const DESKTOP_MIB: u64 = 24 * 1024;

/// An event later than every test deadline: the fake clock is at `at()`
/// (1 s) and the longest class timeout is 600 s.
const LATE_AT: u64 = 601_001;

// ---------------------------------------------------------------------------
// Fixtures: memory facts and the observed GPU state
// ---------------------------------------------------------------------------

fn memory_snapshot(available_mib: u64) -> MemorySnapshot {
    MemorySnapshot::parse(&RawFacts {
        meminfo: format!(
            "MemTotal: {} kB\nMemAvailable: {} kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
            128 * 1024 * 1024,
            available_mib * 1024
        ),
        arcstats: None,
        pressure_memory: "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n\
                          full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
            .to_owned(),
    })
    .expect("the memory fixture parses")
}

fn tenant(id: &str, ladder: GpuLadder) -> GpuTenant {
    GpuTenant {
        id: id.to_owned(),
        ladder,
        state: TenantState::Running,
    }
}

fn observation(
    id: &str,
    class: InstanceClass,
    guest_os: GuestOs,
    state: InstanceState,
    install: &str,
) -> InstanceObservation {
    InstanceObservation {
        id: InstanceId::from(id),
        class,
        guest_os,
        state,
        memory_mib: None,
        resident_mib: Some(6 * 1024),
        install: Some(install.to_owned()),
    }
}

fn gpu_state(owner: KernelOwner) -> GpuState {
    GpuState {
        device: "gpu".to_owned(),
        owner,
        bound_to_vfio: false,
        vram: None,
        tenants: vec![
            tenant("desktop-session", GpuLadder::Desktop),
            tenant("gpu-serving", GpuLadder::Interactive),
            tenant("gpu-batch", GpuLadder::BestEffort),
        ],
        instances: vec![
            InstanceObservation {
                memory_mib: Some(DESKTOP_MIB),
                ..observation(
                    "mac-desktop",
                    InstanceClass::Desktop,
                    GuestOs::MacOs,
                    InstanceState::Defined,
                    "mac-install",
                )
            },
            InstanceObservation {
                memory_mib: Some(DESKTOP_MIB),
                ..observation(
                    "win-desktop",
                    InstanceClass::Desktop,
                    GuestOs::Windows,
                    InstanceState::Defined,
                    "win-install",
                )
            },
            InstanceObservation {
                resident_mib: Some(6 * 1024),
                ..observation(
                    "office",
                    InstanceClass::Office,
                    GuestOs::Windows,
                    InstanceState::Running,
                    "win-install",
                )
            },
        ],
    }
}

fn mac_plan() -> SwitchPlan {
    let outcome = plan_switch(
        &KernelOwner::Guest(InstanceId::from("mac-desktop")),
        &gpu_state(KernelOwner::Host),
        &memory_snapshot(64 * 1024),
        &[],
        &Policy::default(),
    );
    match outcome {
        SwitchOutcome::Plan(plan) => plan,
        other => panic!("the Mac switch must be planned, got {other:?}"),
    }
}

fn windows_plan() -> SwitchPlan {
    let outcome = plan_switch(
        &KernelOwner::Guest(InstanceId::from("win-desktop")),
        &gpu_state(KernelOwner::Host),
        &memory_snapshot(64 * 1024),
        &[],
        &Policy::default(),
    );
    match outcome {
        SwitchOutcome::Plan(plan) => plan,
        other => panic!("the Windows switch must be planned, got {other:?}"),
    }
}

fn back_plan(displaced: &[DisplacedTenant]) -> SwitchPlan {
    let outcome = plan_switch(
        &KernelOwner::Host,
        &gpu_state(KernelOwner::Guest(InstanceId::from("mac-desktop"))),
        &memory_snapshot(64 * 1024),
        displaced,
        &Policy::default(),
    );
    match outcome {
        SwitchOutcome::Plan(plan) => plan,
        other => panic!("the switch back must be planned, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The fake `cdrv` driver
// ---------------------------------------------------------------------------

/// A fake host driver that records every call, pushes lifecycle events and can
/// be told to fail one.
///
/// `start` and `stop` return while the transition is still in flight, as the
/// `cdrv` contract says, and push the event that completes it; the runtime must
/// follow the events, not the state.
#[derive(Default)]
struct FakeHost {
    states: Arc<Mutex<BTreeMap<String, InstanceState>>>,
    events: RefCell<Vec<Event>>,
    calls: RefCell<Vec<String>>,
    card: RefCell<Vec<DeviceFacts>>,
    fail_prefix: RefCell<Option<String>>,
    destroys: Cell<u32>,
    state_reads: Cell<u32>,
    stop_times_out: Cell<bool>,
    never_ready: Cell<bool>,
    start_fails: Cell<bool>,
    silent: Cell<bool>,
}

impl FakeHost {
    /// A host whose hardware inventory names the fixture card `gpu` with one
    /// GPU function.
    fn new() -> Self {
        Self::default().with_card(vec![function("gpu", gpu_address(), DeviceClass::Gpu)])
    }

    fn with_card(self, facts: Vec<DeviceFacts>) -> Self {
        *self.card.borrow_mut() = facts;
        self
    }

    fn with_state(self, id: &str, state: InstanceState) -> Self {
        self.set(&InstanceId::from(id), state);
        self
    }

    fn with_event(self, event: Event) -> Self {
        self.events.borrow_mut().push(event);
        self
    }

    fn failing(self, prefix: &str) -> Self {
        *self.fail_prefix.borrow_mut() = Some(prefix.to_owned());
        self
    }

    fn stop_times_out(self) -> Self {
        self.stop_times_out.set(true);
        self
    }

    fn never_ready(self) -> Self {
        self.never_ready.set(true);
        self
    }

    fn start_fails(self) -> Self {
        self.start_fails.set(true);
        self
    }

    /// Push nothing: the stream ends before the instance is ready.
    fn silent(self) -> Self {
        self.silent.set(true);
        self
    }

    fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }

    fn set(&self, id: &InstanceId, state: InstanceState) {
        self.states
            .lock()
            .expect("the fake state lock")
            .insert(id.as_str().to_owned(), state);
    }

    fn record(&self, call: String) -> cdrv::Result<()> {
        let fails = {
            self.calls.borrow_mut().push(call.clone());
            self.fail_prefix
                .borrow()
                .as_ref()
                .is_some_and(|prefix| call.starts_with(prefix))
        };
        if fails {
            return Err(format!("injected failure: {call}").into());
        }
        Ok(())
    }

    fn push(&self, id: &InstanceId, kind: &str, at_ms: u64) {
        self.push_with(id, kind, at_ms, json!({}));
    }

    fn push_with(&self, id: &InstanceId, kind: &str, at_ms: u64, data: serde_json::Value) {
        let seq = u64::try_from(self.events.borrow().len()).unwrap_or(u64::MAX);
        self.events.borrow_mut().push(Event {
            id: EventId(format!("test-host/{seq}")),
            at: Timestamp::from_millis(at_ms),
            host: "test-host".to_owned(),
            source: Source::Clnx,
            kind: kind.to_owned(),
            subject: Subject::Instance(id.clone()),
            severity: Severity::Info,
            seq,
            data,
            trace: None,
        });
    }
}

impl HostInstances for FakeHost {
    fn list(&self) -> cdrv::Result<Vec<(InstanceId, InstanceState)>> {
        Ok(self
            .states
            .lock()
            .expect("the fake state lock")
            .iter()
            .map(|(id, state)| (InstanceId::from(id.as_str()), state.clone()))
            .collect())
    }

    fn define(&self, spec: &InstanceSpec) -> cdrv::Result<()> {
        self.record(format!("define:{}", spec.id))?;
        self.set(&spec.id, InstanceState::Defined);
        Ok(())
    }

    fn start(&self, id: &InstanceId) -> cdrv::Result<()> {
        self.record(format!("start:{id}"))?;
        self.set(id, InstanceState::Starting);
        if self.silent.get() {
            return Ok(());
        }
        if self.start_fails.get() {
            self.push_with(
                id,
                "instance.failed",
                at().as_millis(),
                json!({"detail": "the guest did not boot"}),
            );
        } else if self.never_ready.get() {
            self.push(id, "instance.starting", LATE_AT);
        } else {
            self.push(id, "instance.running", at().as_millis());
        }
        Ok(())
    }

    fn stop(&self, id: &InstanceId, mode: StopMode) -> cdrv::Result<()> {
        self.record(format!("stop:{id}:{mode:?}"))?;
        self.set(id, InstanceState::Stopping);
        if self.stop_times_out.get() {
            self.push(id, "instance.stopping", LATE_AT);
        } else {
            self.push(id, "instance.stopped", at().as_millis());
        }
        Ok(())
    }

    fn destroy(&self, id: &InstanceId) -> cdrv::Result<()> {
        self.destroys.set(self.destroys.get() + 1);
        self.record(format!("destroy:{id}"))?;
        self.set(id, InstanceState::Stopped);
        Ok(())
    }

    fn state(&self, id: &InstanceId) -> cdrv::Result<InstanceState> {
        self.state_reads.set(self.state_reads.get() + 1);
        Ok(self
            .states
            .lock()
            .expect("the fake state lock")
            .get(id.as_str())
            .cloned()
            .unwrap_or(InstanceState::Defined))
    }

    fn events(&self) -> cdrv::Result<EventStream> {
        Ok(Box::new(DeliveredEvents {
            events: self.events.borrow().clone().into_iter(),
            states: Arc::clone(&self.states),
        }))
    }
}

/// The fake driver's event stream: as it delivers an event, the state it
/// reports becomes the state the driver answers with.
struct DeliveredEvents {
    events: std::vec::IntoIter<Event>,
    states: Arc<Mutex<BTreeMap<String, InstanceState>>>,
}

impl Iterator for DeliveredEvents {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let event = self.events.next()?;
        if let (Subject::Instance(id), Some(state)) = (&event.subject, reported_state(&event)) {
            self.states
                .lock()
                .expect("the fake state lock")
                .insert(id.as_str().to_owned(), state);
        }
        Some(event)
    }
}

/// The state a pushed lifecycle event reports, as the fake driver understands
/// it; the runtime is specified against the same vocabulary.
fn reported_state(event: &Event) -> Option<InstanceState> {
    match event.kind.as_str() {
        "instance.defined" => Some(InstanceState::Defined),
        "instance.starting" => Some(InstanceState::Starting),
        "instance.running" => Some(InstanceState::Running),
        "instance.stopping" => Some(InstanceState::Stopping),
        "instance.stopped" => Some(InstanceState::Stopped),
        "instance.failed" => Some(InstanceState::Failed(
            event
                .data
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("the driver reported a failure")
                .to_owned(),
        )),
        _ => None,
    }
}

impl HostDevices for FakeHost {
    fn enumerate(&self, filter: DeviceFilter) -> cdrv::Result<Vec<DeviceFacts>> {
        Ok(self
            .card
            .borrow()
            .iter()
            .filter(|facts| filter.class.is_none_or(|class| facts.class == class))
            .filter(|facts| filter.name.as_ref().is_none_or(|name| &facts.name == name))
            .cloned()
            .collect())
    }

    fn bind_passthrough(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.record(format!("bind:{}", address(*dev)))
    }

    fn unbind_passthrough(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.record(format!("unbind:{}", address(*dev)))
    }

    fn reset(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.record(format!("reset:{}", address(*dev)))
    }
}

impl HostGpu for FakeHost {
    fn stop_tenant(&self, tenant: &str, ladder: GpuLadder) -> cdrv::Result<()> {
        self.record(format!("tenant-stop:{tenant}:{ladder}"))
    }

    fn start_tenant(&self, tenant: &str, ladder: GpuLadder) -> cdrv::Result<()> {
        self.record(format!("tenant-start:{tenant}:{ladder}"))
    }

    fn sunshine_follows(&self, kernel: &KernelOwner) -> cdrv::Result<()> {
        self.record(format!("sunshine:{kernel}"))
    }
}

impl DiskReset for FakeHost {
    fn reset_to_clean(&self, instance: &InstanceId) -> cdrv::Result<()> {
        self.record(format!("disk-reset:{instance}"))
    }
}

/// A guest agent that answers `exec` with a fixed result.
struct FakeGuest {
    status: i32,
}

impl FakeGuest {
    fn new(status: i32) -> Self {
        Self { status }
    }
}

impl GuestAgent for FakeGuest {
    fn health(&self) -> cdrv::Result<GuestHealth> {
        Ok(GuestHealth {
            ready: true,
            clock_synchronised: true,
            booted_at: None,
            agent_version: None,
        })
    }

    fn shutdown(&self, _mode: ShutdownMode) -> cdrv::Result<()> {
        Ok(())
    }

    fn exec(&self, _request: &GuestExec) -> cdrv::Result<ExecResult> {
        Ok(ExecResult {
            status: self.status,
            stdout: b"ok\n".to_vec(),
            stderr: Vec::new(),
        })
    }

    fn free_page_reporting(&self, _enable: bool) -> cdrv::Result<()> {
        Ok(())
    }
}

/// A hand-off sink that keeps what it was asked to record.
#[derive(Default)]
struct RecordingHandoff {
    recorded: Vec<Vec<DisplacedTenant>>,
}

impl DisplacedHandoff for RecordingHandoff {
    fn record(&mut self, plan: &SwitchPlan) -> Result<(), HandoffError> {
        self.recorded.push(plan.displaced.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn gpu_address() -> DeviceAddress {
    DeviceAddress {
        domain: 0,
        bus: 1,
        slot: 0,
        function: 0,
    }
}

/// One PCI function of a card, as the hardware inventory reports it.
fn function(name: &str, address: DeviceAddress, class: DeviceClass) -> DeviceFacts {
    DeviceFacts {
        name: name.to_owned(),
        address: Some(address),
        vendor: None,
        class,
        iommu_group: Some(14),
        driver: None,
    }
}

fn address(dev: DeviceAddress) -> String {
    format!(
        "{:04x}:{:02x}:{:02x}.{}",
        dev.domain, dev.bus, dev.slot, dev.function
    )
}

fn event_log() -> EventLog {
    EventLog::new("test-host", Source::Cwld, 1)
}

/// One lifecycle event a driver would push.
fn event_for(id: &str, kind: &str, at_ms: u64) -> Event {
    Event {
        id: EventId(format!("test-host/{at_ms}")),
        at: Timestamp::from_millis(at_ms),
        host: "test-host".to_owned(),
        source: Source::Clnx,
        kind: kind.to_owned(),
        subject: Subject::Instance(InstanceId::from(id)),
        severity: Severity::Info,
        seq: at_ms,
        data: json!({}),
        trace: None,
    }
}

fn at() -> Timestamp {
    Timestamp::from_millis(1_000)
}

/// A clock frozen at one instant, so a test places the deadline exactly.
struct FakeClock {
    now: Timestamp,
}

impl Clock for FakeClock {
    fn now(&mut self) -> Timestamp {
        self.now
    }
}

fn runtime(tree: &TempTree) -> Runtime {
    runtime_at(tree, at())
}

fn runtime_at(tree: &TempTree, now: Timestamp) -> Runtime {
    Runtime::new(tree.path(), ClassCatalog::defaults())
        .expect("the state directory opens")
        .with_clock(Box::new(FakeClock { now }))
}

fn host_set(host: &FakeHost) -> Hosts<'_> {
    Hosts::new(host, host).with_gpu(host).with_disk_reset(host)
}

fn spec(id: &str, class: InstanceClass, guest_os: GuestOs) -> InstanceSpec {
    let memory = if class == InstanceClass::Desktop {
        MemoryClaim::fixed(DESKTOP_MIB)
    } else {
        MemoryClaim {
            min_mib: 1024,
            max_mib: 16 * 1024,
            balloon: true,
        }
    };
    InstanceSpec {
        id: InstanceId::from(id),
        kind: InstanceKind::Vm,
        guest_os,
        memory,
        cpu: CpuClaim { quota_cores: 4.0 },
        class,
        devices: if class == InstanceClass::Desktop {
            vec![gpu_address()]
        } else {
            Vec::new()
        },
        disks: Vec::new(),
    }
}

fn write_holder_table(path: &Path, target: &InstanceId) {
    let mut table = HolderTable::new();
    let mut log = EventLog::new("test-host", Source::Cdct, 1);
    let holder = Holder {
        id: HolderId::from("holder-1"),
        lease: LeaseId::from("lease-1"),
        instance: Some(target.clone()),
        tenant: None,
        class: InstanceClass::Desktop,
        priority: GpuLadder::Desktop.priority(),
        memory_mib: DESKTOP_MIB,
        cpu_cores: 4.0,
        devices: vec!["gpu".to_owned()],
        displaced: Vec::new(),
        state: HolderState::Running,
        since: Timestamp::from_millis(0),
        last_event_seq: 0,
    };
    table.admit(holder, &mut log, Timestamp::from_millis(0));
    let mut store = HolderStore::open(path).expect("the holder store opens");
    store.save(&table).expect("the holder table saves");
}

fn event_kinds(events: &[Event]) -> Vec<&str> {
    events.iter().map(|event| event.kind.as_str()).collect()
}

// ---------------------------------------------------------------------------
// Classes as validated data
// ---------------------------------------------------------------------------

const DEV_RULES: &str = r#"memory = { kind = "balloon", min_mib = 1024, max_mib = 24576, size_classes_mib = [8192, 12288, 16384, 24576] }
persistence = "persistent"
reset = "never"
gpu = "none"
hibernation = "never"
handover_stop = "clean"
exec = false
free_page_reporting = false
readiness_timeout_secs = 300
stop_timeout_secs = 120"#;

const DESKTOP_RULES: &str = r#"memory = { kind = "fixed", mib = 24576 }
persistence = "persistent"
reset = "never"
gpu = "gpu"
hibernation = "never"
handover_stop = "clean"
exec = false
free_page_reporting = false
readiness_timeout_secs = 300
stop_timeout_secs = 180"#;

const CI_RULES: &str = r#"memory = { kind = "balloon", min_mib = 2048, max_mib = 16384 }
persistence = "ephemeral"
reset = "after_each_job"
gpu = "none"
hibernation = "never"
handover_stop = "clean"
exec = true
free_page_reporting = true
readiness_timeout_secs = 600
stop_timeout_secs = 120"#;

const OFFICE_RULES: &str = r#"memory = { kind = "balloon", min_mib = 4096, max_mib = 24576 }
persistence = "persistent"
reset = "never"
gpu = "none"
hibernation = "when_idle"
handover_stop = "clean"
exec = false
free_page_reporting = true
readiness_timeout_secs = 300
stop_timeout_secs = 180"#;

fn toml_doc(dev: &str, desktop: &str, ci: &str, office: &str) -> String {
    format!("[dev]\n{dev}\n\n[desktop]\n{desktop}\n\n[ci]\n{ci}\n\n[office]\n{office}\n")
}

fn house_toml() -> String {
    toml_doc(DEV_RULES, DESKTOP_RULES, CI_RULES, OFFICE_RULES)
}

fn invalid_validation(text: &str) -> ValidationError {
    match ClassCatalog::from_toml(text) {
        Err(ParseError::Invalid(error)) => error,
        other => panic!("expected an invalid class document, got {other:?}"),
    }
}

#[test]
fn the_house_classes_are_exactly_status() {
    let classes = ClassCatalog::defaults();
    let dev = classes.rules(InstanceClass::Dev);
    assert!(dev.memory.balloons());
    assert_eq!(
        dev.memory.size_classes_mib(),
        &[8 * 1024, 12 * 1024, 16 * 1024, 24 * 1024]
    );
    assert!(!dev.is_ephemeral());

    let desktop = classes.rules(InstanceClass::Desktop);
    assert_eq!(desktop.memory, MemoryRule::Fixed { mib: DESKTOP_MIB });
    assert!(desktop.needs_gpu());
    assert!(!desktop.memory.balloons());

    let ci = classes.rules(InstanceClass::Ci);
    assert!(ci.is_ephemeral());
    assert!(ci.reset_after_each_job());
    assert!(ci.exec);
    assert!(!ci.needs_gpu());

    let office = classes.rules(InstanceClass::Office);
    assert!(office.memory.balloons());
    assert_eq!(office.hibernation, Hibernation::WhenIdle);
    assert_eq!(office.handover_stop, StopMode::Clean);
    assert!(!office.needs_gpu());
}

#[test]
fn class_data_loads_from_toml_and_json() {
    let from_toml = ClassCatalog::from_toml(&house_toml()).expect("the fixture is valid TOML");
    let defaults = ClassCatalog::defaults();
    for class in [
        InstanceClass::Dev,
        InstanceClass::Desktop,
        InstanceClass::Ci,
        InstanceClass::Office,
    ] {
        assert_eq!(
            from_toml.rules(class),
            defaults.rules(class),
            "the house defaults are valid class data"
        );
    }
    assert!(from_toml.is_ephemeral(InstanceClass::Ci));

    let json = r#"{
        "dev": {"memory":{"kind":"balloon","min_mib":1024,"max_mib":24576,"size_classes_mib":[8192,12288,16384,24576]},"persistence":"persistent","reset":"never","gpu":"none","hibernation":"never","handover_stop":"clean","exec":false,"free_page_reporting":false,"readiness_timeout_secs":300,"stop_timeout_secs":120},
        "desktop": {"memory":{"kind":"fixed","mib":24576},"persistence":"persistent","reset":"never","gpu":"gpu","hibernation":"never","handover_stop":"clean","exec":false,"free_page_reporting":false,"readiness_timeout_secs":300,"stop_timeout_secs":180},
        "ci": {"memory":{"kind":"balloon","min_mib":2048,"max_mib":16384,"size_classes_mib":[]},"persistence":"ephemeral","reset":"after_each_job","gpu":"none","hibernation":"never","handover_stop":"clean","exec":true,"free_page_reporting":true,"readiness_timeout_secs":600,"stop_timeout_secs":120},
        "office": {"memory":{"kind":"balloon","min_mib":4096,"max_mib":24576,"size_classes_mib":[]},"persistence":"persistent","reset":"never","gpu":"none","hibernation":"when_idle","handover_stop":"clean","exec":false,"free_page_reporting":true,"readiness_timeout_secs":300,"stop_timeout_secs":180}
    }"#;
    let from_json = ClassCatalog::from_json(json).expect("the fixture is valid JSON");
    assert_eq!(
        from_json.rules(InstanceClass::Ci),
        ClassCatalog::defaults().rules(InstanceClass::Ci)
    );
}

#[test]
fn invalid_class_data_is_an_error_at_load() {
    let cases: Vec<(&str, String)> = vec![
        (
            "a desktop that is not 24 GiB",
            toml_doc(
                DEV_RULES,
                &DESKTOP_RULES.replace("mib = 24576", "mib = 16384"),
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a desktop that balloons",
            toml_doc(
                DEV_RULES,
                &DESKTOP_RULES.replace(
                    r#"memory = { kind = "fixed", mib = 24576 }"#,
                    r#"memory = { kind = "balloon", min_mib = 8192, max_mib = 24576 }"#,
                ),
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a dev VM with fixed memory",
            toml_doc(
                &DEV_RULES.replace(
                    r#"memory = { kind = "balloon", min_mib = 1024, max_mib = 24576, size_classes_mib = [8192, 12288, 16384, 24576] }"#,
                    r#"memory = { kind = "fixed", mib = 8192 }"#,
                ),
                DESKTOP_RULES,
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a dev VM with an unknown size class",
            toml_doc(
                &DEV_RULES.replace(
                    "size_classes_mib = [8192, 12288, 16384, 24576]",
                    "size_classes_mib = [8192, 12288, 16384, 20000]",
                ),
                DESKTOP_RULES,
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a dev ceiling that is not its largest size class",
            toml_doc(
                &DEV_RULES.replace("max_mib = 24576", "max_mib = 16384"),
                DESKTOP_RULES,
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a balloon floor above its ceiling",
            toml_doc(
                &DEV_RULES.replace("min_mib = 1024, max_mib = 24576", "min_mib = 24576, max_mib = 1024"),
                DESKTOP_RULES,
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "a persistent CI instance",
            toml_doc(
                DEV_RULES,
                DESKTOP_RULES,
                &CI_RULES.replace(r#"persistence = "ephemeral""#, r#"persistence = "persistent""#),
                OFFICE_RULES,
            ),
        ),
        (
            "a CI instance without exec",
            toml_doc(
                DEV_RULES,
                DESKTOP_RULES,
                &CI_RULES.replace("exec = true", "exec = false"),
                OFFICE_RULES,
            ),
        ),
        (
            "an office profile that never hibernates when idle",
            toml_doc(
                DEV_RULES,
                DESKTOP_RULES,
                CI_RULES,
                &OFFICE_RULES.replace(r#"hibernation = "when_idle""#, r#"hibernation = "never""#),
            ),
        ),
        (
            "an office profile that hibernates for a handover",
            toml_doc(
                DEV_RULES,
                DESKTOP_RULES,
                CI_RULES,
                &OFFICE_RULES.replace(
                    r#"handover_stop = "clean""#,
                    r#"handover_stop = "hibernate""#,
                ),
            ),
        ),
        (
            "a zero readiness timeout",
            toml_doc(
                DEV_RULES,
                &DESKTOP_RULES.replace("readiness_timeout_secs = 300", "readiness_timeout_secs = 0"),
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
        (
            "an unknown field",
            toml_doc(
                &format!("{DEV_RULES}\ncolour = \"blue\""),
                DESKTOP_RULES,
                CI_RULES,
                OFFICE_RULES,
            ),
        ),
    ];
    for (label, text) in cases {
        assert!(
            ClassCatalog::from_toml(&text).is_err(),
            "{label} must be rejected"
        );
    }

    // The precise reasons, for the rules that carry one.
    let wrong_fixed = invalid_validation(&toml_doc(
        DEV_RULES,
        &DESKTOP_RULES.replace("mib = 24576", "mib = 16384"),
        CI_RULES,
        OFFICE_RULES,
    ));
    assert!(matches!(
        wrong_fixed,
        ValidationError::WrongFixed { got_mib: 16384, .. }
    ));
    let must_balloon = invalid_validation(&toml_doc(
        &DEV_RULES.replace(
            r#"memory = { kind = "balloon", min_mib = 1024, max_mib = 24576, size_classes_mib = [8192, 12288, 16384, 24576] }"#,
            r#"memory = { kind = "fixed", mib = 8192 }"#,
        ),
        DESKTOP_RULES,
        CI_RULES,
        OFFICE_RULES,
    ));
    assert!(matches!(must_balloon, ValidationError::MustBalloon { .. }));
    let handover = invalid_validation(&toml_doc(
        DEV_RULES,
        DESKTOP_RULES,
        CI_RULES,
        &OFFICE_RULES.replace(
            r#"handover_stop = "clean""#,
            r#"handover_stop = "hibernate""#,
        ),
    ));
    assert!(matches!(
        handover,
        ValidationError::HandoverMustBeClean { .. }
    ));

    // A missing class is an error, not a default.
    let missing = format!("[dev]\n{DEV_RULES}\n[desktop]\n{DESKTOP_RULES}\n[ci]\n{CI_RULES}\n");
    assert!(matches!(
        ClassCatalog::from_toml(&missing),
        Err(ParseError::Toml { .. })
    ));
}

#[test]
fn class_data_loads_by_extension() {
    let tree = TempTree::new("classes");
    let toml_path = tree.path().join("classes.toml");
    std::fs::write(&toml_path, house_toml()).expect("write the TOML fixture");
    let catalog = ClassCatalog::load(&toml_path).expect("the TOML file loads");
    assert!(catalog.is_ephemeral(InstanceClass::Ci));

    let unknown = tree.path().join("classes.yaml");
    std::fs::write(&unknown, house_toml()).expect("write the unknown-format fixture");
    assert!(matches!(
        ClassCatalog::load(&unknown),
        Err(cwld::LoadError::UnknownFormat { .. })
    ));
}

// ---------------------------------------------------------------------------
// One lifecycle state machine
// ---------------------------------------------------------------------------

#[test]
fn the_state_machine_follows_the_contracts_table() {
    let instance = InstanceId::from("example-vm");
    let mut log = event_log();
    let mut lifecycle = Lifecycle::unobserved();

    fn advance(
        lifecycle: &mut Lifecycle,
        trigger: Trigger,
        log: &mut EventLog,
        instance: &InstanceId,
    ) -> Event {
        lifecycle
            .advance(trigger, log, at(), instance)
            .expect("the transition is legal")
            .expect("the transition emits an event")
    }

    assert_eq!(
        advance(&mut lifecycle, Trigger::Define, &mut log, &instance).kind,
        "instance.defined"
    );
    assert_eq!(lifecycle.state(), Some(&InstanceState::Defined));
    assert_eq!(
        advance(&mut lifecycle, Trigger::Start, &mut log, &instance).kind,
        "instance.starting"
    );
    assert_eq!(
        advance(&mut lifecycle, Trigger::Ready, &mut log, &instance).kind,
        "instance.running"
    );
    assert_eq!(lifecycle.state(), Some(&InstanceState::Running));
    assert_eq!(
        advance(&mut lifecycle, Trigger::Stop, &mut log, &instance).kind,
        "instance.stopping"
    );
    assert_eq!(
        advance(&mut lifecycle, Trigger::GuestOff, &mut log, &instance).kind,
        "instance.stopped"
    );
    assert_eq!(
        advance(&mut lifecycle, Trigger::Reset, &mut log, &instance).kind,
        "instance.reset"
    );
    assert_eq!(lifecycle.state(), Some(&InstanceState::Defined));

    // An observation adopts what the driver says and reports drift.
    let drift = lifecycle
        .advance(
            Trigger::Observe(InstanceState::Failed("crash".to_owned())),
            &mut log,
            at(),
            &instance,
        )
        .expect("observing is always legal");
    let drift = drift.expect("a changed state is drift");
    assert_eq!(drift.kind, "instance.drift");
    assert_eq!(
        lifecycle.state(),
        Some(&InstanceState::Failed("crash".to_owned()))
    );
    let quiet = lifecycle
        .advance(
            Trigger::Observe(InstanceState::Failed("crash".to_owned())),
            &mut log,
            at(),
            &instance,
        )
        .expect("observing is always legal");
    assert!(quiet.is_none(), "the same state is not drift");
}

#[test]
fn illegal_transitions_are_typed_errors() {
    let instance = InstanceId::from("example-vm");
    let mut log = event_log();
    let cases = [
        (Lifecycle::unobserved(), Trigger::Start),
        (Lifecycle::observed(InstanceState::Defined), Trigger::Ready),
        (Lifecycle::observed(InstanceState::Running), Trigger::Start),
        (Lifecycle::observed(InstanceState::Starting), Trigger::Stop),
        (Lifecycle::observed(InstanceState::Stopping), Trigger::Reset),
        (
            Lifecycle::observed(InstanceState::Failed("crash".to_owned())),
            Trigger::Start,
        ),
    ];
    for (mut lifecycle, trigger) in cases {
        let error = lifecycle
            .advance(trigger, &mut log, at(), &instance)
            .expect_err("the transition is illegal");
        assert!(matches!(error, TransitionError::Illegal { .. }));
    }
}

#[test]
fn a_stopped_persistent_instance_starts_again() {
    let instance = InstanceId::from("example-vm");
    let mut log = event_log();

    // The state machine itself knows no classes: a stopped instance starts.
    let mut lifecycle = Lifecycle::observed(InstanceState::Stopped);
    let event = lifecycle
        .advance(Trigger::Start, &mut log, at(), &instance)
        .expect("a stopped instance may start")
        .expect("the transition emits an event");
    assert_eq!(event.kind, "instance.starting");
    assert_eq!(lifecycle.state(), Some(&InstanceState::Starting));

    // A stopped ephemeral instance reaches the same trigger through the
    // runtime's reset to `Defined`, which this test covers at the plan level.
}

#[test]
fn a_stop_timeout_ends_failed_and_says_a_human_is_needed() {
    let instance = InstanceId::from("example-vm");
    let mut log = event_log();
    let mut lifecycle = Lifecycle::observed(InstanceState::Running);
    lifecycle
        .advance(Trigger::Stop, &mut log, at(), &instance)
        .expect("stopping is legal");
    let event = lifecycle
        .advance(Trigger::StopTimeout, &mut log, at(), &instance)
        .expect("the timeout is legal from Stopping")
        .expect("the failure emits an event");
    assert_eq!(event.kind, "instance.failed");
    assert_eq!(event.severity, cdrv::Severity::Error);
    assert_eq!(event.data["reason"], "stop_timeout");
    assert_eq!(event.data["needs_human"], true);
    assert!(matches!(lifecycle.state(), Some(InstanceState::Failed(_))));
}

// ---------------------------------------------------------------------------
// The plan executor
// ---------------------------------------------------------------------------

#[test]
fn a_full_mac_switch_hands_over_the_card_and_records_what_it_displaced() {
    let tree = TempTree::new("mac-switch");
    let holders = tree.path().join("holders.json");
    let target = InstanceId::from("mac-desktop");
    write_holder_table(&holders, &target);

    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Defined);
    let hosts = host_set(&host);
    let plan = mac_plan();
    let mut log = event_log();
    let mut handoff = HolderHandoff::new(&holders);

    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut handoff)
        .expect("the journal is writable");

    assert!(report.is_complete());
    assert_eq!(report.executed.len(), plan.steps.len());
    assert_eq!(report.handoff, HandoffStatus::Recorded);
    assert!(report.not_run.is_empty());
    assert_eq!(
        host.calls(),
        vec![
            "tenant-stop:gpu-batch:gpu-besteffort".to_owned(),
            "tenant-stop:gpu-serving:gpu-interactive".to_owned(),
            "tenant-stop:desktop-session:gpu-desktop".to_owned(),
            format!("bind:{}", address(gpu_address())),
            "start:mac-desktop".to_owned(),
            "sunshine:guest:mac-desktop".to_owned(),
        ]
    );
    assert_eq!(runtime.state(&target), Some(&InstanceState::Running));
    assert!(
        runtime
            .journals()
            .scan()
            .expect("the journals read")
            .is_empty()
    );

    // The hand-off recorded exactly what the plan displaced.
    let table = read_table(&holders)
        .expect("the holder table reads")
        .expect("the holder table exists");
    assert_eq!(table.gpu_owner, KernelOwner::Guest(target.clone()));
    assert_eq!(
        table.displaced_by_card_custodian(),
        plan.displaced.as_slice()
    );

    // The reverse plan restores exactly that set, in ladder order.
    let reverse = back_plan(table.displaced_by_card_custodian());
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Running);
    let hosts = host_set(&host);
    let report = runtime
        .execute_plan(&reverse, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(report.is_complete());
    assert_eq!(report.handoff, HandoffStatus::NotRequired);
    assert_eq!(
        host.calls(),
        vec![
            "stop:mac-desktop:Clean".to_owned(),
            format!("unbind:{}", address(gpu_address())),
            format!("reset:{}", address(gpu_address())),
            "tenant-start:desktop-session:gpu-desktop".to_owned(),
            "tenant-start:gpu-serving:gpu-interactive".to_owned(),
            "tenant-start:gpu-batch:gpu-besteffort".to_owned(),
            "sunshine:host".to_owned(),
        ]
    );
}

#[test]
fn a_windows_switch_stops_the_office_profile_first() {
    let tree = TempTree::new("windows-switch");
    let mut runtime = runtime(&tree);
    runtime.register(spec(
        "win-desktop",
        InstanceClass::Desktop,
        GuestOs::Windows,
    ));
    runtime.register(spec("office", InstanceClass::Office, GuestOs::Windows));
    let host = FakeHost::new()
        .with_state("win-desktop", InstanceState::Defined)
        .with_state("office", InstanceState::Running);
    let hosts = host_set(&host);
    let plan = windows_plan();

    // The plan itself stops the office profile cleanly, never hibernating it.
    let office_step = plan
        .steps
        .iter()
        .position(|step| {
            matches!(
                step,
                PlanStep::StopInstance { instance, mode }
                    if instance.as_str() == "office" && *mode == StopMode::Clean
            )
        })
        .expect("the plan stops the office profile");
    let handover_step = plan
        .steps
        .iter()
        .position(|step| matches!(step, PlanStep::HandOverCard { .. }))
        .expect("the plan hands the card over");
    assert!(office_step < handover_step);

    let mut log = event_log();
    let mut handoff = RecordingHandoff::default();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut handoff)
        .expect("the journal is writable");
    assert!(report.is_complete());
    assert_eq!(report.handoff, HandoffStatus::Recorded);
    assert_eq!(handoff.recorded, vec![plan.displaced.clone()]);

    let calls = host.calls();
    let office_stop = calls
        .iter()
        .position(|call| call == "stop:office:Clean")
        .expect("the office profile is stopped");
    let bind = calls
        .iter()
        .position(|call| call.starts_with("bind:"))
        .expect("the card is bound");
    let start = calls
        .iter()
        .position(|call| call == "start:win-desktop")
        .expect("the Windows desktop starts");
    assert!(office_stop < bind);
    assert!(office_stop < start);
    assert!(!calls.iter().any(|call| call.contains("Hibernate")));
}

#[test]
fn a_handover_that_hibernates_is_refused() {
    let tree = TempTree::new("hibernate");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Running);
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Host,
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StopInstance {
            instance: InstanceId::from("mac-desktop"),
            mode: StopMode::Hibernate,
        }],
    };
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    let failed = report.failed.expect("the step is refused");
    assert!(matches!(failed.error, StepError::HibernateForbidden { .. }));
    assert!(host.calls().is_empty());
    assert_eq!(host.destroys.get(), 0);
}

#[test]
fn a_failing_step_stops_the_plan_and_reports_what_did_not_run() {
    let tree = TempTree::new("failing-step");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .failing("bind:");
    let hosts = host_set(&host);
    let plan = mac_plan();
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");

    let failed = report.failed.as_ref().expect("the handover fails");
    assert_eq!(failed.index, 3);
    assert!(matches!(&failed.step, PlanStep::HandOverCard { .. }));
    assert!(matches!(failed.error, StepError::Driver { .. }));
    assert_eq!(report.executed.len(), 3);
    assert_eq!(report.not_run.len(), 2);
    assert_eq!(
        report.not_run,
        vec![
            PlanStep::StartInstance {
                instance: InstanceId::from("mac-desktop")
            },
            PlanStep::SunshineFollows {
                kernel: KernelOwner::Guest(InstanceId::from("mac-desktop"))
            },
        ]
    );
    assert!(
        event_kinds(&report.events).contains(&"plan.step_failed"),
        "the failure is an event"
    );
    assert!(!host.calls().iter().any(|call| call.starts_with("start:")));
    assert!(
        runtime
            .journals()
            .scan()
            .expect("the journals read")
            .is_empty()
    );
}

#[test]
fn a_readiness_timeout_fails_the_start() {
    let tree = TempTree::new("readiness-timeout");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .never_ready();
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Guest(InstanceId::from("mac-desktop")),
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance {
            instance: InstanceId::from("mac-desktop"),
        }],
    };
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    let failed = report.failed.as_ref().expect("the start times out");
    assert!(matches!(
        failed.error,
        StepError::ReadinessTimeout {
            timeout_secs: 300,
            ..
        }
    ));
    assert!(matches!(
        runtime.state(&InstanceId::from("mac-desktop")),
        Some(InstanceState::Failed(_))
    ));
    assert!(event_kinds(&report.events).contains(&"instance.failed"));
    assert_eq!(host.destroys.get(), 0);
    assert_eq!(
        host.state_reads.get(),
        2,
        "one observation and one read before the stream; the wait never polls"
    );
}

#[test]
fn a_start_follows_the_pushed_events_and_reads_the_state_once() {
    let tree = TempTree::new("pushed-events");
    let mut runtime = runtime(&tree);
    let mac = InstanceId::from("mac-desktop");
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    // Events for another instance and an intermediate state are ignored; only
    // the pushed `instance.running` completes the start.
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .with_event(event_for("other-vm", "instance.running", 1_050))
        .with_event(event_for("mac-desktop", "instance.starting", 1_100));
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Guest(mac.clone()),
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance {
            instance: mac.clone(),
        }],
    };
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");

    assert!(report.is_complete());
    assert_eq!(runtime.state(&mac), Some(&InstanceState::Running));
    assert_eq!(host.calls(), vec!["start:mac-desktop".to_owned()]);
    assert_eq!(
        host.state_reads.get(),
        2,
        "the state is read once for the observation and once before the stream"
    );
}

#[test]
fn a_driver_failure_event_fails_the_start() {
    let tree = TempTree::new("failed-event");
    let mut runtime = runtime(&tree);
    let mac = InstanceId::from("mac-desktop");
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .start_fails();
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Guest(mac.clone()),
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance {
            instance: mac.clone(),
        }],
    };
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");

    let failed = report.failed.expect("the driver's failure ends the start");
    assert!(matches!(
        &failed.error,
        StepError::ObservedFailed { during: "starting", reason, .. }
            if reason == "the guest did not boot"
    ));
    assert_eq!(
        runtime.state(&mac),
        Some(&InstanceState::Failed("the guest did not boot".to_owned()))
    );
    assert_eq!(host.state_reads.get(), 2);
}

#[test]
fn an_ended_event_stream_is_a_driver_error() {
    let tree = TempTree::new("ended-stream");
    let mut runtime = runtime(&tree);
    let mac = InstanceId::from("mac-desktop");
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .silent();
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Guest(mac.clone()),
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance {
            instance: mac.clone(),
        }],
    };
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");

    let failed = report.failed.expect("an exhausted stream fails the start");
    assert!(matches!(failed.error, StepError::Driver { .. }));
}

#[test]
fn the_deadline_is_measured_on_the_clock_the_events_share() {
    // `never_ready` pushes `instance.starting` at `LATE_AT` (601 s).
    let late = TempTree::new("late-clock");
    let mut late_runtime = runtime(&late);
    let mac = InstanceId::from("mac-desktop");
    late_runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .never_ready();
    let hosts = host_set(&host);
    let plan = SwitchPlan {
        target: KernelOwner::Guest(mac.clone()),
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance {
            instance: mac.clone(),
        }],
    };
    let mut log = event_log();
    let report = late_runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(matches!(
        report.failed.expect("late event").error,
        StepError::ReadinessTimeout { .. }
    ));

    // The same stream is not late for a clock whose deadline is beyond it, so
    // the wait ends with the driver's stream, not a timeout.
    let early = TempTree::new("early-clock");
    let mut early_runtime = runtime_at(&early, Timestamp::from_millis(400_000));
    early_runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .never_ready();
    let hosts = host_set(&host);
    let report = early_runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(matches!(
        report.failed.expect("the stream ends").error,
        StepError::Driver { .. }
    ));
}

#[test]
fn a_stop_timeout_ends_failed_and_never_destroys() {
    let tree = TempTree::new("stop-timeout");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Running)
        .stop_times_out();
    let hosts = host_set(&host);
    let plan = back_plan(&[]);
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");

    let failed = report.failed.as_ref().expect("the stop fails");
    assert!(matches!(failed.error, StepError::StopTimeout { .. }));
    assert_eq!(host.destroys.get(), 0, "cwld never destroys on its own");
    assert_eq!(
        runtime.state(&InstanceId::from("mac-desktop")),
        Some(&InstanceState::Failed(
            "the stop timed out; a human or a reboot is needed".to_owned()
        ))
    );
    let human = report
        .events
        .iter()
        .find(|event| event.kind == "instance.failed")
        .expect("the failure is an event");
    assert_eq!(human.data["needs_human"], true);
    assert_eq!(human.severity, cdrv::Severity::Error);
    assert!(!host.calls().iter().any(|call| call.starts_with("unbind:")));
    assert_eq!(
        host.state_reads.get(),
        2,
        "the stop wait follows events, it does not poll the state"
    );
}

#[test]
fn a_plan_without_its_ports_fails_with_a_typed_error() {
    let tree = TempTree::new("ports");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Defined);
    let cdrv_only = Hosts::new(&host, &host);
    let plan = mac_plan();
    let mut log = event_log();
    let report = runtime
        .execute_plan(&plan, &cdrv_only, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    let failed = report.failed.as_ref().expect("the first step fails");
    assert!(matches!(
        failed.error,
        StepError::NoPort {
            port: "HostGpu",
            ..
        }
    ));

    // A card the hardware inventory does not name is refused, not invented.
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .with_card(Vec::new());
    let hosts = host_set(&host);
    let report = runtime
        .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(matches!(
        report.failed.expect("the handover fails").error,
        StepError::UnknownDevice { ref device } if device == "gpu"
    ));
    assert!(!host.calls().iter().any(|call| call.starts_with("bind:")));
}

#[test]
fn a_handover_resolves_the_card_name_to_every_function() {
    let tree = TempTree::new("card-functions");
    let mut runtime = runtime(&tree);
    let mac = InstanceId::from("mac-desktop");
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let audio = DeviceAddress {
        domain: 0,
        bus: 1,
        slot: 0,
        function: 1,
    };
    let usb = DeviceAddress {
        domain: 0,
        bus: 1,
        slot: 0,
        function: 2,
    };
    let card = || {
        vec![
            function("gpu", usb, DeviceClass::Usb),
            function("gpu", gpu_address(), DeviceClass::Gpu),
            function("gpu", audio, DeviceClass::Audio),
        ]
    };
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Defined)
        .with_card(card());
    let hosts = host_set(&host);
    let mut log = event_log();
    let report = runtime
        .execute_plan(&mac_plan(), &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(report.is_complete());
    assert_eq!(
        host.calls(),
        vec![
            "tenant-stop:gpu-batch:gpu-besteffort".to_owned(),
            "tenant-stop:gpu-serving:gpu-interactive".to_owned(),
            "tenant-stop:desktop-session:gpu-desktop".to_owned(),
            format!("bind:{}", address(gpu_address())),
            format!("bind:{}", address(audio)),
            format!("bind:{}", address(usb)),
            "start:mac-desktop".to_owned(),
            "sunshine:guest:mac-desktop".to_owned(),
        ]
    );

    // The reverse hands back every function it took, in address order.
    let host = FakeHost::new()
        .with_state("mac-desktop", InstanceState::Running)
        .with_card(card());
    let hosts = host_set(&host);
    let report = runtime
        .execute_plan(&back_plan(&[]), &hosts, &mut log, at(), &mut DiscardHandoff)
        .expect("the journal is writable");
    assert!(report.is_complete());
    assert_eq!(runtime.state(&mac), Some(&InstanceState::Stopped));
    assert_eq!(
        host.calls(),
        vec![
            "stop:mac-desktop:Clean".to_owned(),
            format!("unbind:{}", address(gpu_address())),
            format!("reset:{}", address(gpu_address())),
            format!("unbind:{}", address(audio)),
            format!("reset:{}", address(audio)),
            format!("unbind:{}", address(usb)),
            format!("reset:{}", address(usb)),
            "sunshine:host".to_owned(),
        ]
    );
}

// ---------------------------------------------------------------------------
// The CI class
// ---------------------------------------------------------------------------

#[test]
fn a_ci_job_runs_clean_start_exec_stop_and_reset() {
    let tree = TempTree::new("ci-job");
    let mut runtime = runtime(&tree);
    let ci = InstanceId::from("ci-1");
    runtime.register(spec("ci-1", InstanceClass::Ci, GuestOs::Linux));
    let host = FakeHost::new().with_state("ci-1", InstanceState::Defined);
    let hosts = host_set(&host);
    let guest = FakeGuest::new(0);
    let job = CiJob {
        instance: ci.clone(),
        command: GuestExec {
            program: "build.sh".to_owned(),
            args: vec!["--check".to_owned()],
            timeout: std::time::Duration::from_secs(60),
        },
    };
    let mut log = event_log();
    let report = runtime
        .run_ci_job(&job, &hosts, &guest, &mut log, at())
        .expect("the CI class rules allow the job");

    assert!(report.started);
    assert!(report.stopped, "the stop failed: {:?}", report.failure);
    assert!(report.reset);
    assert!(report.failure.is_none());
    assert_eq!(report.exec.expect("the command ran").status, 0);
    assert_eq!(
        host.calls(),
        vec![
            "start:ci-1".to_owned(),
            "stop:ci-1:Clean".to_owned(),
            "disk-reset:ci-1".to_owned(),
        ]
    );
    assert_eq!(runtime.state(&ci), Some(&InstanceState::Defined));
    let kinds = event_kinds(&report.events);
    assert!(kinds.contains(&"job.started"));
    assert!(kinds.contains(&"job.finished"));
    assert!(kinds.contains(&"instance.reset"));
}

#[test]
fn a_stopped_ephemeral_instance_is_reset_before_it_starts() {
    let tree = TempTree::new("ci-stopped");
    let mut runtime = runtime(&tree);
    runtime.register(spec("ci-1", InstanceClass::Ci, GuestOs::Linux));
    let host = FakeHost::new().with_state("ci-1", InstanceState::Stopped);
    let hosts = host_set(&host);
    let guest = FakeGuest::new(0);
    let job = CiJob {
        instance: InstanceId::from("ci-1"),
        command: GuestExec {
            program: "build.sh".to_owned(),
            args: Vec::new(),
            timeout: std::time::Duration::from_secs(60),
        },
    };
    let mut log = event_log();
    let report = runtime
        .run_ci_job(&job, &hosts, &guest, &mut log, at())
        .expect("the CI job runs");
    assert!(report.started);
    assert!(report.reset);
    assert_eq!(
        host.calls(),
        vec![
            "disk-reset:ci-1".to_owned(),
            "start:ci-1".to_owned(),
            "stop:ci-1:Clean".to_owned(),
            "disk-reset:ci-1".to_owned(),
        ]
    );
}

#[test]
fn a_stopped_persistent_instance_starts_without_a_reset() {
    let tree = TempTree::new("stopped-persistent");
    let mut runtime = runtime(&tree);
    let cases = [
        ("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs),
        ("dev-1", InstanceClass::Dev, GuestOs::Linux),
        ("office", InstanceClass::Office, GuestOs::Windows),
    ];
    for (id, class, guest_os) in cases {
        runtime.register(spec(id, class, guest_os));
        let instance = InstanceId::from(id);
        let host = FakeHost::new().with_state(id, InstanceState::Stopped);
        let hosts = host_set(&host);
        let plan = SwitchPlan {
            target: KernelOwner::Host,
            memory: None,
            displaced: Vec::new(),
            steps: vec![PlanStep::StartInstance {
                instance: instance.clone(),
            }],
        };
        let mut log = event_log();
        let report = runtime
            .execute_plan(&plan, &hosts, &mut log, at(), &mut DiscardHandoff)
            .expect("the journal is writable");
        assert!(report.is_complete(), "{id} starts from Stopped: {report:?}");
        assert_eq!(runtime.state(&instance), Some(&InstanceState::Running));
        assert_eq!(
            host.calls(),
            vec![format!("start:{id}")],
            "{id} starts again without a disk reset"
        );
    }
}

#[test]
fn a_ci_job_requires_a_ci_instance() {
    let tree = TempTree::new("ci-class");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Defined);
    let hosts = host_set(&host);
    let guest = FakeGuest::new(0);
    let job = CiJob {
        instance: InstanceId::from("mac-desktop"),
        command: GuestExec {
            program: "true".to_owned(),
            args: Vec::new(),
            timeout: std::time::Duration::from_secs(1),
        },
    };
    let mut log = event_log();
    let error = runtime
        .run_ci_job(&job, &hosts, &guest, &mut log, at())
        .expect_err("a desktop is not a CI instance");
    assert!(matches!(error, RuntimeError::NotACiInstance { .. }));
    assert!(host.calls().is_empty());

    let unknown = CiJob {
        instance: InstanceId::from("nobody"),
        ..job
    };
    assert!(matches!(
        runtime
            .run_ci_job(&unknown, &hosts, &guest, &mut log, at())
            .expect_err("an unregistered instance has no class"),
        RuntimeError::UnknownInstance { .. }
    ));
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

#[test]
fn recovery_reports_a_half_run_plan_without_guessing() {
    let tree = TempTree::new("half-run");
    let plan = mac_plan();
    {
        let runtime = runtime(&tree);
        let mut journal = runtime
            .journals()
            .begin(&plan, at())
            .expect("the journal begins");
        runtime
            .journals()
            .before_step(&mut journal, 0)
            .expect("step 0 is recorded");
        runtime
            .journals()
            .after_step(&mut journal, 0)
            .expect("step 0 completed");
        runtime
            .journals()
            .before_step(&mut journal, 1)
            .expect("step 1 is in flight");
        // The runtime dies here: the journal stays on disk.
    }
    let mut runtime = runtime(&tree);
    let mut log = event_log();
    let report = runtime.recover(&mut log, at()).expect("the state reads");

    assert_eq!(report.half_run.len(), 1);
    let half = &report.half_run[0];
    assert_eq!(half.target, plan.target);
    assert_eq!(half.executed, vec![plan.steps[0].clone()]);
    assert_eq!(half.uncertain, Some(plan.steps[1].clone()));
    assert_eq!(half.not_started, plan.steps[2..].to_vec());
    assert!(event_kinds(&report.events).contains(&"plan.incomplete"));
    assert!(
        runtime
            .journals()
            .scan()
            .expect("the journals read")
            .is_empty(),
        "a reported crash is not reported twice"
    );
}

#[test]
fn a_dirty_ephemeral_instance_is_reset_before_the_next_start() {
    let tree = TempTree::new("dirty-ephemeral");
    let ci = InstanceId::from("ci-1");
    {
        let runtime = runtime(&tree);
        runtime
            .marks()
            .mark(
                &ci,
                DirtyReason::Starting,
                at(),
                Some("simulated crash during start".to_owned()),
            )
            .expect("the mark is written");
    }
    let mut runtime = runtime(&tree);
    runtime.register(spec("ci-1", InstanceClass::Ci, GuestOs::Linux));
    let mut log = event_log();
    let recovery = runtime.recover(&mut log, at()).expect("the state reads");
    assert_eq!(recovery.dirty.len(), 1);
    assert!(recovery.dirty[0].ephemeral);
    assert!(event_kinds(&recovery.events).contains(&"instance.dirty"));

    let host = FakeHost::new().with_state("ci-1", InstanceState::Defined);
    let hosts = host_set(&host);
    let guest = FakeGuest::new(0);
    let job = CiJob {
        instance: ci,
        command: GuestExec {
            program: "build.sh".to_owned(),
            args: Vec::new(),
            timeout: std::time::Duration::from_secs(60),
        },
    };
    let report = runtime
        .run_ci_job(&job, &hosts, &guest, &mut log, at())
        .expect("the CI job runs");
    assert!(report.reset);
    let calls = host.calls();
    assert_eq!(calls[0], "disk-reset:ci-1");
    assert_eq!(calls[1], "start:ci-1");
    assert_eq!(
        calls
            .iter()
            .filter(|call| *call == "disk-reset:ci-1")
            .count(),
        2,
        "ensure_clean resets once, the job's last step resets once"
    );
}

#[test]
fn a_dirty_persistent_instance_is_reported_not_reset() {
    let tree = TempTree::new("dirty-persistent");
    let mac = InstanceId::from("mac-desktop");
    let mut runtime = runtime(&tree);
    runtime.register(spec("mac-desktop", InstanceClass::Desktop, GuestOs::MacOs));
    runtime
        .marks()
        .mark(
            &mac,
            DirtyReason::Stopping,
            at(),
            Some("simulated crash during a stop".to_owned()),
        )
        .expect("the mark is written");
    let host = FakeHost::new().with_state("mac-desktop", InstanceState::Stopped);
    let hosts = host_set(&host);
    let mut log = event_log();
    let outcome = runtime
        .ensure_clean(&mac, &hosts, &mut log, at())
        .expect("the mark reads");
    assert!(matches!(outcome, CleanOutcome::Reported { .. }));
    assert!(!outcome.is_clean());
    assert!(
        host.calls().is_empty(),
        "a persistent disk is never reset automatically"
    );
    assert!(runtime.marks().is_dirty(&mac).expect("the mark reads"));
}

// ---------------------------------------------------------------------------
// Temporary fixture directories
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempTree {
    root: PathBuf,
}

impl TempTree {
    fn new(label: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("cwld-it-{}-{label}-{id}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create the fixture directory");
        Self { root }
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
