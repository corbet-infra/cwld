//! The Linux host: `cwld follow` executes the plans the conductor grants,
//! through the `clnx` drivers.
//!
//! This module is the composition root of one host and nothing else: it reads
//! the host file (deployment data), builds the `cdrv` host traits from `clnx`
//! for every plan, subscribes to the conductor and runs each `granted` plan it
//! is pushed. It decides nothing (the plan is `cdct`'s), polls nothing (the
//! conductor pushes the grant), and destroys nothing (a failed step stops the
//! plan and is reported).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use cdct::{Client, Endpoint, Reply, Request, SwitchPlan};
use cdrv::traits::{HostDevices, Result as CdrvResult};
use cdrv::{DeviceAddress, DeviceFacts, DeviceFilter, EventLog, InstanceSpec, Source, Timestamp};
use clnx::{
    KubeClient, LinuxDevices, LinuxGpu, LinuxInstances, LinuxKernel, LinuxSysfs, RealLibvirt,
    RenderOptions, TenantConfig, ZbusSystemd, parse_bdf,
};
use serde::Deserialize;

use crate::classes::ClassCatalog;
use crate::guest_stop::{GuestStopReaction, GuestStops, restart_plan};
use crate::handoff::{DaemonHandoff, DiscardHandoff};
use crate::plan::{Hosts, PlanReport};
use crate::runtime::Runtime;
use crate::wait::{Clock, SystemClock};

/// One host's deployment data (`/etc/cwld/host.json`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// The host name events carry.
    pub host: String,
    /// The conductor's local control socket.
    pub conductor: PathBuf,
    /// The runtime's state directory (dirty marks, plan journals).
    pub state_dir: PathBuf,
    /// The class data file; the house classes when absent.
    #[serde(default)]
    pub classes: Option<PathBuf>,
    /// The libvirt connection.
    #[serde(default = "default_libvirt")]
    pub libvirt_uri: String,
    /// The GPU inventory nixgpu renders (the card's name and its functions).
    pub inventory: PathBuf,
    /// The driver the card returns to.
    pub host_driver: String,
    /// How long the hand-over waits for the card's holders to leave.
    #[serde(default = "default_holder_wait")]
    pub holder_wait_secs: u64,
    /// How long a systemd start or stop may take.
    #[serde(default = "default_job_timeout")]
    pub job_timeout_secs: u64,
    /// The desktop session that holds the card while the host kernel does.
    pub desktop: DesktopConfig,
    /// The Argo CD namespace whose Applications own the GPU pods.
    #[serde(default = "default_argocd")]
    pub argocd_namespace: String,
    /// Deployment data the instance specs do not carry (firmware, OpenCore image, CPU model...);
    /// the defaults of `RenderOptions` when absent.
    #[serde(default)]
    pub render: Option<RenderConfig>,
    /// The instances this host runs.
    #[serde(default)]
    pub instances: Vec<InstanceEntry>,
    /// The live ladder's observer; absent, the conductor only switches.
    #[serde(default)]
    pub ladder: Option<LadderConfig>,
}

/// Where the live ladder's facts and events come from.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LadderConfig {
    /// This host's Kubernetes node name.
    pub node: String,
    /// The shared model broker, when one runs on the card.
    #[serde(default)]
    pub broker: Option<BrokerEntry>,
    /// The GPU device plugin's registration guard.
    #[serde(default)]
    pub guard: Option<GuardEntry>,
    /// The card sampler: the desktop's GTT spill, read on a clock.
    #[serde(default)]
    pub sampler: Option<SamplerEntry>,
}

/// The card sampler's clock and thresholds (the conductor's policy values).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamplerEntry {
    /// Seconds between two readings (at least 1).
    pub interval_secs: u64,
    /// GTT growth between two readings below which a spill is noise, in bytes.
    pub gtt_delta_bytes: u64,
    /// The VRAM-used fraction above which the card counts as full.
    pub vram_full_fraction: f64,
}

/// The device plugin and the resources it must advertise while it runs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardEntry {
    /// The extended resources the plugin advertises.
    pub resources: Vec<String>,
    /// Its pods' namespace.
    pub namespace: String,
    /// Its pods' label selector.
    pub labels: String,
}

/// The render options of this host (`RenderOptions`, as deployment data).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderConfig {
    /// The QEMU machine type.
    pub machine: Option<String>,
    /// The emulator binary (absolute path).
    pub emulator: Option<String>,
    /// The pflash code file.
    pub loader: Option<PathBuf>,
    /// The NVRAM template.
    pub nvram_template: Option<PathBuf>,
    /// The per-domain NVRAM file.
    pub nvram: Option<PathBuf>,
    /// The OpenCore boot image of a macOS guest.
    pub opencore_iso: Option<PathBuf>,
    /// A file holding the AppleSMC OSK string (read at render time, never kept in the config).
    pub apple_smc_osk_file: Option<PathBuf>,
    /// A custom CPU model string.
    pub cpu_model: Option<String>,
    /// The 64-bit MMIO window in MiB.
    pub mmio64_mib: Option<u64>,
    /// The VNC socket for a spec that asks for a console.
    pub console_socket: Option<PathBuf>,
    /// Whether to keep an emulated display beside a passed-through card.
    pub emulated_display: Option<bool>,
    /// Extra MiB above the RAM claim for the domain's hard limit.
    pub hard_limit_extra_mib: Option<u64>,
}

impl HostConfig {
    /// The render options: the library defaults with this host's data on top.
    ///
    /// # Errors
    ///
    /// The OSK file cannot be read.
    pub fn render_options(&self) -> Result<RenderOptions, String> {
        let mut o = RenderOptions::default();
        let Some(r) = &self.render else {
            return Ok(o);
        };
        if let Some(v) = &r.machine {
            o.machine.clone_from(v);
        }
        o.emulator = r.emulator.clone().or(o.emulator);
        o.loader = r.loader.clone().or(o.loader);
        o.nvram_template = r.nvram_template.clone().or(o.nvram_template);
        o.nvram = r.nvram.clone().or(o.nvram);
        o.opencore_iso = r.opencore_iso.clone().or(o.opencore_iso);
        if let Some(file) = &r.apple_smc_osk_file {
            let text = fs::read_to_string(file)
                .map_err(|error| format!("cannot read {}: {error}", file.display()))?;
            o.apple_smc_osk = Some(text.trim().to_owned());
        }
        o.cpu_model = r.cpu_model.clone().or(o.cpu_model);
        if let Some(v) = r.mmio64_mib {
            o.mmio64_mib = v;
        }
        o.console_socket = r.console_socket.clone().or(o.console_socket);
        if let Some(v) = r.emulated_display {
            o.emulated_display = v;
        }
        o.hard_limit_extra_mib = r.hard_limit_extra_mib.or(o.hard_limit_extra_mib);
        Ok(o)
    }
}

/// The model broker as a Kubernetes Service.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerEntry {
    /// `namespace/name` of the Service.
    pub service: String,
    /// The Service port of its HTTP API.
    pub port: u16,
}

/// Where the desktop session's unit lives.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopConfig {
    /// The system unit that owns the session (stopping it stops the session).
    pub unit: String,
    /// The `cgroup.procs` file of a container's init scope, when the session
    /// runs in a container: its first pid's root holds the container's system
    /// bus. Absent: the host's own system bus.
    #[serde(default)]
    pub container_init_procs: Option<PathBuf>,
    /// The system bus socket inside that root.
    #[serde(default = "default_system_bus")]
    pub system_bus: PathBuf,
    /// Sunshine's unit when it is separate from the session; absent when it
    /// leaves and returns with the session unit.
    #[serde(default)]
    pub sunshine_unit: Option<String>,
    /// Whether that Sunshine unit is a user unit.
    #[serde(default)]
    pub sunshine_in_user_manager: bool,
}

/// One instance: its spec and, for a guest that ignores ACPI, the command
/// that makes it halt itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceEntry {
    /// The instance spec (its class decides the wait timeouts).
    pub spec: InstanceSpec,
    /// The clean-stop command, program first, no shell.
    #[serde(default)]
    pub clean_stop: Option<Vec<String>>,
    /// BARs of the card's GPU function this guest gets shrunk (a guest that
    /// cannot use the full ReBAR window, such as macOS); empty: as they are.
    #[serde(default)]
    pub bar_limits: Vec<BarLimitEntry>,
}

/// One BAR size limit for a guest.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarLimitEntry {
    /// The BAR index.
    pub bar: u8,
    /// The largest size the guest gets, in MiB.
    pub mib: u64,
}

fn default_libvirt() -> String {
    "qemu:///system".to_owned()
}
const fn default_holder_wait() -> u64 {
    60
}
const fn default_job_timeout() -> u64 {
    120
}
fn default_argocd() -> String {
    "argocd".to_owned()
}
fn default_system_bus() -> PathBuf {
    PathBuf::from("/run/dbus/system_bus_socket")
}

impl HostConfig {
    /// Read and parse a host file.
    ///
    /// # Errors
    ///
    /// The file cannot be read or is not a valid host file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
    }
}

/// The GPU inventory nixgpu renders.
#[derive(Debug, Clone, Deserialize)]
struct Inventory {
    name: String,
    functions: Vec<InventoryFunction>,
}

#[derive(Debug, Clone, Deserialize)]
struct InventoryFunction {
    address: String,
}

impl Inventory {
    fn load(path: &Path) -> Result<(String, Vec<DeviceAddress>), String> {
        let text = fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        let inventory: Self =
            serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        let mut functions = Vec::new();
        for function in &inventory.functions {
            functions.push(parse_bdf(&function.address).ok_or_else(|| {
                format!(
                    "{}: {} is not a PCI address",
                    path.display(),
                    function.address
                )
            })?);
        }
        if functions.is_empty() {
            return Err(format!("{}: the card has no functions", path.display()));
        }
        Ok((inventory.name, functions))
    }
}

/// The device driver, with the card resolvable by its inventory name.
///
/// A plan names the card as the inventory does; `clnx` enumerates by PCI
/// name. This adapter answers a filter on the inventory name with the facts
/// of exactly the declared functions and passes everything else through.
struct InventoryDevices<D: HostDevices> {
    inner: D,
    name: String,
    functions: Vec<DeviceAddress>,
}

impl<D: HostDevices> HostDevices for InventoryDevices<D> {
    fn enumerate(&self, filter: DeviceFilter) -> CdrvResult<Vec<DeviceFacts>> {
        if filter.name.as_deref() != Some(self.name.as_str()) {
            return self.inner.enumerate(filter);
        }
        let mut facts = Vec::new();
        for function in &self.functions {
            let by_address = DeviceFilter {
                class: filter.class,
                name: Some(clnx::bdf(function)),
            };
            facts.extend(self.inner.enumerate(by_address)?);
        }
        Ok(facts)
    }

    fn bind_passthrough(&self, dev: &DeviceAddress) -> CdrvResult<()> {
        self.inner.bind_passthrough(dev)
    }

    fn unbind_passthrough(&self, dev: &DeviceAddress) -> CdrvResult<()> {
        self.inner.unbind_passthrough(dev)
    }

    fn reset(&self, dev: &DeviceAddress) -> CdrvResult<()> {
        self.inner.reset(dev)
    }
}

/// The address of the desktop's system bus, resolved now (a container's init
/// pid changes when the container restarts).
fn desktop_bus(desktop: &DesktopConfig) -> Result<Option<String>, String> {
    let Some(procs) = &desktop.container_init_procs else {
        return Ok(None);
    };
    let text = fs::read_to_string(procs)
        .map_err(|error| format!("cannot read {}: {error}", procs.display()))?;
    let pid = text
        .lines()
        .next()
        .map(str::trim)
        .filter(|pid| !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| {
            format!(
                "{} names no pid: is the container running?",
                procs.display()
            )
        })?;
    let socket = desktop
        .system_bus
        .strip_prefix("/")
        .unwrap_or(&desktop.system_bus);
    Ok(Some(format!(
        "unix:path=/proc/{pid}/root/{}",
        socket.display()
    )))
}

/// Execute one plan against this host's drivers, connected for this plan.
fn execute(
    runtime: &mut Runtime,
    config: &HostConfig,
    plan: &SwitchPlan,
    record: bool,
) -> Result<PlanReport, String> {
    let (name, functions) = Inventory::load(&config.inventory)?;
    let libvirt = RealLibvirt::connect(&config.libvirt_uri).map_err(|error| error.to_string())?;
    let mut instances = LinuxInstances::new(config.host.clone(), libvirt, config.render_options()?);
    for entry in &config.instances {
        if let Some(argv) = &entry.clean_stop {
            instances = instances.with_clean_stop(entry.spec.id.as_str(), argv.clone());
        }
    }
    // The guest the card goes to may take it only with some BARs shrunk.
    let bar_limits: Vec<clnx::BarLimit> = plan
        .target
        .guest()
        .and_then(|guest| {
            config
                .instances
                .iter()
                .find(|entry| &entry.spec.id == guest)
        })
        .map(|entry| {
            entry
                .bar_limits
                .iter()
                .map(|limit| clnx::BarLimit {
                    bar: limit.bar,
                    mib: limit.mib,
                })
                .collect()
        })
        .unwrap_or_default();
    let devices = InventoryDevices {
        inner: LinuxDevices::new(
            LinuxSysfs::system(),
            LinuxKernel::system(),
            functions.clone(),
            config.host_driver.clone(),
        )
        .with_holder_wait(Duration::from_secs(config.holder_wait_secs))
        .with_bar_limits(bar_limits),
        name,
        functions,
    };
    let systemd = match desktop_bus(&config.desktop)? {
        Some(address) => ZbusSystemd::system_at(&address),
        None => ZbusSystemd::system(),
    }
    .map_err(|error| error.to_string())?
    .with_job_timeout(Duration::from_secs(config.job_timeout_secs));
    let kube = KubeClient::with_argocd_namespace(config.argocd_namespace.clone())
        .map_err(|error| error.to_string())?;
    let gpu = LinuxGpu::new(
        systemd,
        kube,
        TenantConfig {
            desktop_unit: config.desktop.unit.clone(),
            sunshine_unit: config.desktop.sunshine_unit.clone(),
            sunshine_in_user_manager: config.desktop.sunshine_in_user_manager,
            device_plugin: config.ladder.as_ref().and_then(|ladder| {
                ladder.guard.as_ref().map(|guard| clnx::DevicePlugin {
                    namespace: guard.namespace.clone(),
                    labels: guard.labels.clone(),
                    node: ladder.node.clone(),
                })
            }),
        },
    );
    let hosts = Hosts::new(&instances, &devices).with_gpu(&gpu);
    let mut log = EventLog::new(config.host.clone(), Source::Cwld, 1);
    let at: Timestamp = SystemClock.now();
    let outcome = if record {
        let mut handoff = DaemonHandoff::new(&config.conductor);
        runtime.execute_plan(plan, &hosts, &mut log, at, &mut handoff)
    } else {
        // A rebooted guest started again: the conductor's holder (and the
        // tenants it displaced) stay exactly as they are.
        runtime.execute_plan(plan, &hosts, &mut log, at, &mut DiscardHandoff)
    };
    outcome.map_err(|error| format!("the plan journal could not be written: {error}"))
}

fn report(report: &PlanReport) {
    for event in &report.events {
        if let Ok(line) = serde_json::to_string(event) {
            println!("{line}");
        }
    }
    match &report.failed {
        None => println!(
            "cwld: plan to {} complete: {} step(s)",
            report.target,
            report.executed.len()
        ),
        Some(failure) => {
            eprintln!(
                "cwld: plan to {} STOPPED at step {} ({:?}): {}; {} step(s) ran, {} not run; nothing is retried",
                report.target,
                failure.index + 1,
                failure.step,
                failure.error,
                report.executed.len(),
                report.not_run.len()
            );
        }
    }
}

/// Tell the conductor a plan ran to its end. A return to the host releases the
/// guest's holder only now; an incomplete plan is not reported, so the next
/// `cdct switch` plans it again.
fn complete(conductor: &Path, plan: &SwitchPlan) {
    let outcome = Client::connect(&Endpoint::LocalSocket(conductor.to_path_buf()))
        .and_then(|mut client| client.completed(&plan.target));
    match outcome {
        Ok(owner) => println!(
            "cwld: reported the plan to {} complete; the card's owner is {owner}",
            plan.target
        ),
        Err(error) => eprintln!(
            "cwld: the plan to {} completed, but the conductor did not take the report: {error}",
            plan.target
        ),
    }
}

/// Follow the conductor: run every plan it grants, until its stream ends.
///
/// # Errors
///
/// The host file, the class data or the state directory cannot be used, or
/// the conductor cannot be reached; the stream ending is an error too, so the
/// service manager restarts the follower (the conductor is socket-activated).
pub fn follow(path: &Path) -> Result<(), String> {
    let config = HostConfig::load(path)?;
    let classes = match &config.classes {
        Some(file) => ClassCatalog::load(file).map_err(|error| error.to_string())?,
        None => ClassCatalog::defaults(),
    };
    let mut runtime =
        Runtime::new(&config.state_dir, classes).map_err(|error| error.to_string())?;
    for entry in &config.instances {
        runtime.register(entry.spec.clone());
    }
    for journal in runtime
        .journals()
        .scan()
        .map_err(|error| error.to_string())?
    {
        eprintln!(
            "cwld: a plan was left half run by an earlier process: {} step(s) done, uncertain {:?}, {} not started; reconcile before switching",
            journal.executed().len(),
            journal.uncertain(),
            journal.not_started().len()
        );
    }
    // SIGTERM never cuts a plan: while one runs, the stop waits for it to end
    // (a half-bound card or a half-stopped guest is the worst state to leave);
    // when idle, the process exits at once.
    let busy = Arc::new(AtomicBool::new(false));
    let stopping = Arc::new(AtomicBool::new(false));
    {
        let busy = Arc::clone(&busy);
        let stopping = Arc::clone(&stopping);
        ctrlc::set_handler(move || {
            stopping.store(true, Ordering::SeqCst);
            if !busy.load(Ordering::SeqCst) {
                println!("cwld: stopping (no plan in flight)");
                std::process::exit(0);
            }
            println!("cwld: stopping after the plan in flight");
        })
        .map_err(|error| format!("cannot install the stop handler: {error}"))?;
    }
    if let Some(ladder) = &config.ladder {
        let (_, functions) = Inventory::load(&config.inventory)?;
        let card = functions
            .iter()
            .copied()
            .find(|address| address.function == 0)
            .unwrap_or(functions[0]);
        let broker = match &ladder.broker {
            Some(entry) => {
                let (namespace, name) = entry.service.split_once('/').ok_or_else(|| {
                    format!(
                        "{}: the broker service {} is not namespace/name",
                        path.display(),
                        entry.service
                    )
                })?;
                Some(clnx::pressure::BrokerConfig {
                    namespace: namespace.to_owned(),
                    name: name.to_owned(),
                    port: entry.port,
                })
            }
            None => None,
        };
        let sampler = match ladder.sampler {
            Some(entry) if entry.interval_secs == 0 => {
                return Err(format!(
                    "{}: ladder.sampler.interval_secs must be at least 1",
                    path.display()
                ));
            }
            Some(entry) => Some(clnx::pressure::SamplerConfig {
                interval: Duration::from_secs(entry.interval_secs),
                gtt_delta_bytes: entry.gtt_delta_bytes,
                vram_full_fraction: entry.vram_full_fraction,
            }),
            None => None,
        };
        crate::ladder::start(
            clnx::pressure::PressureConfig {
                node: ladder.node.clone(),
                card,
                broker,
                guard: ladder
                    .guard
                    .as_ref()
                    .map(|guard| clnx::pressure::GuardConfig {
                        resources: guard.resources.clone(),
                        namespace: guard.namespace.clone(),
                        labels: guard.labels.clone(),
                    }),
                sampler,
            },
            config.conductor.clone(),
        )?;
        println!("cwld: the live ladder observes node {}", ladder.node);
    }
    let mut client = Client::connect(&Endpoint::LocalSocket(config.conductor.clone()))
        .map_err(|error| format!("cannot reach the conductor: {error}"))?;
    client
        .send(&Request::Subscribe)
        .map_err(|error| format!("cannot subscribe: {error}"))?;
    println!(
        "cwld: following the conductor at {}",
        config.conductor.display()
    );
    // One input queue, one consumer: the conductor's pushes, the guests'
    // lifecycle stops and QEMU's shutdown reasons. A plan runs to its end before
    // the next input is read, so the stops a plan makes are known when their
    // events are handled.
    let (sender, inputs) = mpsc::channel::<Input>();
    {
        let sender = sender.clone();
        std::thread::Builder::new()
            .name("cwld-conductor".to_owned())
            .spawn(move || {
                loop {
                    let reply = client.receive();
                    let end = !matches!(reply, Ok(Some(_)));
                    if sender
                        .send(Input::Conductor(reply.map_err(|e| e.to_string())))
                        .is_err()
                        || end
                    {
                        return;
                    }
                }
            })
            .map_err(|error| format!("cannot start the conductor reader: {error}"))?;
    }
    let guests: Vec<String> = config
        .instances
        .iter()
        .map(|entry| entry.spec.id.as_str().to_owned())
        .collect();
    if !guests.is_empty() {
        watch_guests(&config.libvirt_uri, &guests, &sender)?;
    }
    drop(sender);
    let mut stops = GuestStops::default();
    // Inputs read while waiting for a stop's reason, handled next.
    let mut pending: std::collections::VecDeque<Input> = std::collections::VecDeque::new();
    loop {
        let input = match pending.pop_front() {
            Some(input) => input,
            None => match inputs.recv() {
                Ok(input) => input,
                Err(_) => break,
            },
        };
        match input {
            Input::Conductor(Ok(Some(Reply::Granted { plan }))) => {
                println!(
                    "cwld: granted: switch the card to {} ({} step(s))",
                    plan.target,
                    plan.steps.len()
                );
                busy.store(true, Ordering::SeqCst);
                match execute(&mut runtime, &config, &plan, true) {
                    Ok(done) => {
                        report(&done);
                        stops.plan_stopped(&done.executed);
                        if done.is_complete() {
                            complete(&config.conductor, &plan);
                        }
                    }
                    Err(message) => eprintln!(
                        "cwld: the plan to {} was NOT started: {message}",
                        plan.target
                    ),
                }
                busy.store(false, Ordering::SeqCst);
                if stopping.load(Ordering::SeqCst) {
                    return Ok(());
                }
            }
            Input::Conductor(Ok(Some(_))) => {}
            Input::Conductor(Ok(None)) => break,
            Input::Conductor(Err(error)) => {
                return Err(format!("the conductor stream failed: {error}"));
            }
            Input::Shutdown { domain, reset } => stops.shutdown(&domain, reset),
            Input::Stopped(domain) => {
                let owner = conductor_owner(&config.conductor);
                let owns = owner
                    .as_ref()
                    .and_then(|owner| owner.guest())
                    .is_some_and(|guest| guest.as_str() == domain);
                // QEMU's reason and the stop come from two event clients: give
                // the reason a moment to arrive (bounded; it precedes the stop).
                if owns && !stops.knows_reason(&domain) {
                    let deadline = std::time::Instant::now() + REASON_WAIT;
                    while let Some(left) =
                        deadline.checked_duration_since(std::time::Instant::now())
                    {
                        match inputs.recv_timeout(left) {
                            Ok(Input::Shutdown {
                                domain: about,
                                reset,
                            }) if about == domain => {
                                stops.shutdown(&about, reset);
                                break;
                            }
                            Ok(other) => pending.push_back(other),
                            Err(_) => break,
                        }
                    }
                }
                match stops.stopped(&domain, owns) {
                    GuestStopReaction::Ignore => {}
                    GuestStopReaction::Restart => {
                        println!(
                            "cwld: {domain} rebooted from inside: starting it again, the card stays with it"
                        );
                        let Some(owner) = owner else { continue };
                        let plan = restart_plan(owner, cdrv::InstanceId::from(domain.as_str()));
                        busy.store(true, Ordering::SeqCst);
                        match execute(&mut runtime, &config, &plan, false) {
                            Ok(done) => report(&done),
                            Err(message) => {
                                eprintln!("cwld: {domain} was NOT started again: {message}")
                            }
                        }
                        busy.store(false, Ordering::SeqCst);
                        if stopping.load(Ordering::SeqCst) {
                            return Ok(());
                        }
                    }
                    GuestStopReaction::ReturnToHost => {
                        println!(
                            "cwld: {domain} was shut down from inside: asking the conductor to return the card to the host"
                        );
                        match Client::connect(&Endpoint::LocalSocket(config.conductor.clone()))
                            .and_then(|mut asker| asker.switch(&cdrv::KernelOwner::Host))
                        {
                            Ok(reply) => {
                                println!("cwld: the conductor answered the return: {reply:?}")
                            }
                            Err(error) => {
                                eprintln!("cwld: the return to the host was not granted: {error}")
                            }
                        }
                    }
                }
            }
        }
    }
    Err("the conductor closed the stream".to_owned())
}

/// How long a stopped guest's QEMU shutdown reason may lag behind its stop.
const REASON_WAIT: Duration = Duration::from_secs(3);

/// What the follow loop reads.
enum Input {
    /// A push from the conductor (`None`: the stream ended).
    Conductor(Result<Option<Reply>, String>),
    /// A configured guest's domain stopped.
    Stopped(String),
    /// QEMU's shutdown reason for a domain (`reset`: a reboot from inside).
    Shutdown { domain: String, reset: bool },
}

/// The card's owner as the conductor names it now; `None` when it cannot be asked.
fn conductor_owner(conductor: &Path) -> Option<cdrv::KernelOwner> {
    Client::connect(&Endpoint::LocalSocket(conductor.to_path_buf()))
        .and_then(|mut client| client.status())
        .map(|status| status.owner)
        .ok()
}

/// Stream the configured guests' stops and QEMU's shutdown reasons into the
/// follow loop (libvirt's own event clients; nothing polls).
fn watch_guests(uri: &str, guests: &[String], sender: &mpsc::Sender<Input>) -> Result<(), String> {
    use clnx::DomainEventSource;
    let lifecycle = clnx::VirshEvents::new(uri)
        .subscribe()
        .map_err(|error| error.to_string())?;
    let reasons = clnx::QemuShutdownEvents::new(uri)
        .subscribe()
        .map_err(|error| error.to_string())?;
    let known = guests.to_vec();
    let to_loop = sender.clone();
    std::thread::Builder::new()
        .name("cwld-guest-stops".to_owned())
        .spawn(move || {
            for event in lifecycle {
                if known.contains(&event.domain)
                    && matches!(event.kind, clnx::DomainEventKind::Stopped(_))
                    && to_loop.send(Input::Stopped(event.domain)).is_err()
                {
                    return;
                }
            }
        })
        .map_err(|error| format!("cannot start the guest stop reader: {error}"))?;
    let known = guests.to_vec();
    let to_loop = sender.clone();
    std::thread::Builder::new()
        .name("cwld-guest-reasons".to_owned())
        .spawn(move || {
            for event in reasons {
                if known.contains(&event.domain)
                    && to_loop
                        .send(Input::Shutdown {
                            domain: event.domain,
                            reset: event.reset,
                        })
                        .is_err()
                {
                    return;
                }
            }
        })
        .map_err(|error| format!("cannot start the guest reason reader: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cwld-linux-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_host_file_parses_with_its_defaults() {
        let config: HostConfig = serde_json::from_str(
            r#"{"host":"example-host","conductor":"/run/cdct/control.sock",
                "state_dir":"/var/lib/cwld","inventory":"/etc/example/gpu.json",
                "host_driver":"amdgpu","desktop":{"unit":"desktop-session.service"},
                "instances":[{"spec":{"id":"guest-a","kind":"vm","guest_os":"macos",
                  "memory":{"min_mib":1024,"max_mib":1024,"balloon":false},
                  "cpu":{"quota_cores":2.0},"class":"desktop","devices":[],"disks":[]},
                  "clean_stop":["ssh","guest-a","halt"]}]}"#,
        )
        .unwrap();
        assert_eq!(config.libvirt_uri, "qemu:///system");
        assert_eq!(config.holder_wait_secs, 60);
        assert_eq!(config.argocd_namespace, "argocd");
        assert!(config.desktop.sunshine_unit.is_none());
        assert_eq!(config.instances[0].clean_stop.as_ref().unwrap()[0], "ssh");
    }

    #[test]
    fn a_host_file_carries_bar_limits_and_the_ladder() {
        let config: HostConfig = serde_json::from_str(
            r#"{"host":"example-host","conductor":"/run/cdct/control.sock",
                "state_dir":"/var/lib/cwld","inventory":"/etc/example/gpu.json",
                "host_driver":"amdgpu","desktop":{"unit":"desktop-session.service"},
                "instances":[{"spec":{"id":"guest-a","kind":"vm","guest_os":"macos",
                  "memory":{"min_mib":1024,"max_mib":1024,"balloon":false},
                  "cpu":{"quota_cores":2.0},"class":"desktop","devices":[],"disks":[]},
                  "bar_limits":[{"bar":0,"mib":1024}]}],
                "ladder":{"node":"example-node",
                  "broker":{"service":"llm/example-broker","port":8080},
                  "guard":{"resources":["example.com/slot"],"namespace":"kube-system",
                    "labels":"app=example-plugin"},
                  "sampler":{"interval_secs":6,"gtt_delta_bytes":67108864,
                    "vram_full_fraction":0.85}}}"#,
        )
        .unwrap();
        assert_eq!(config.instances[0].bar_limits[0].mib, 1024);
        let ladder = config.ladder.unwrap();
        let sampler = ladder.sampler.unwrap();
        assert_eq!(sampler.interval_secs, 6);
        assert_eq!(sampler.gtt_delta_bytes, 64 * 1024 * 1024);
        assert!((sampler.vram_full_fraction - 0.85).abs() < f64::EPSILON);
        assert_eq!(ladder.broker.unwrap().port, 8080);
        assert_eq!(
            ladder.guard.unwrap().resources,
            vec!["example.com/slot".to_owned()]
        );
    }

    #[test]
    fn the_container_bus_follows_the_init_pid() {
        let dir = temp("bus");
        let procs = dir.join("cgroup.procs");
        fs::write(&procs, "4242\n4300\n").unwrap();
        let desktop = DesktopConfig {
            unit: "desktop-session.service".to_owned(),
            container_init_procs: Some(procs.clone()),
            system_bus: default_system_bus(),
            sunshine_unit: None,
            sunshine_in_user_manager: false,
        };
        assert_eq!(
            desktop_bus(&desktop).unwrap().unwrap(),
            "unix:path=/proc/4242/root/run/dbus/system_bus_socket"
        );
        fs::write(&procs, "").unwrap();
        assert!(
            desktop_bus(&desktop).is_err(),
            "a stopped container names no pid"
        );
        let host = DesktopConfig {
            container_init_procs: None,
            ..desktop
        };
        assert_eq!(desktop_bus(&host).unwrap(), None);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_inventory_names_the_card_and_its_functions() {
        let dir = temp("inventory");
        let file = dir.join("gpu.json");
        fs::write(
            &file,
            r#"{"schema":1,"name":"example-gpu","functions":[
                {"address":"0000:01:00.0","role":"video"},{"address":"0000:01:00.1","role":"audio"}]}"#,
        )
        .unwrap();
        let (name, functions) = Inventory::load(&file).unwrap();
        assert_eq!(name, "example-gpu");
        assert_eq!(functions.len(), 2);
        assert_eq!(functions[1].function, 1);
        fs::write(&file, r#"{"schema":1,"name":"example-gpu","functions":[]}"#).unwrap();
        assert!(Inventory::load(&file).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}

/// The host side of the instance reconcile: clnx renders, libvirt defines.
struct HostDomains {
    instances: LinuxInstances<RealLibvirt, clnx::ProcessRunner>,
    print: bool,
}

impl crate::reconcile::Domains for HostDomains {
    fn render(&self, spec: &InstanceSpec) -> Result<String, String> {
        let xml = self
            .instances
            .render(spec)
            .map_err(|error| error.to_string())?;
        if self.print {
            println!("<!-- rendered {} -->\n{xml}", spec.id);
        }
        Ok(xml)
    }

    fn state(&self, id: &cdrv::InstanceId) -> Result<Option<cdrv::InstanceState>, String> {
        use cdrv::traits::HostInstances;
        let known = self.instances.list().map_err(|error| error.to_string())?;
        Ok(known
            .into_iter()
            .find(|(name, _)| name == id)
            .map(|(_, state)| state))
    }

    fn define(&self, spec: &InstanceSpec) -> Result<(), String> {
        use cdrv::traits::HostInstances;
        self.instances
            .define(spec)
            .map_err(|error| error.to_string())
    }
}

/// `cwld reconcile`: define every declared instance whose domain is missing or shut off with a
/// different render; a live domain is reported `pending` and not touched. Prints one line each.
///
/// # Errors
///
/// The host file or libvirt cannot be used, or an instance could not be reconciled.
/// `dry_run` defines nothing and records nothing; `print` writes each rendered definition to stdout.
pub fn reconcile_instances(path: &Path, dry_run: bool, print: bool) -> Result<(), String> {
    let config = HostConfig::load(path)?;
    let libvirt = RealLibvirt::connect(&config.libvirt_uri).map_err(|error| error.to_string())?;
    let instances = LinuxInstances::new(config.host.clone(), libvirt, config.render_options()?);
    let specs: Vec<InstanceSpec> = config.instances.iter().map(|e| e.spec.clone()).collect();
    let mut failed = 0;
    for (id, outcome) in crate::reconcile::reconcile(
        &HostDomains { instances, print },
        &specs,
        &config.state_dir,
        !dry_run,
    ) {
        println!("{id}: {}", outcome.describe());
        if matches!(outcome, crate::reconcile::Outcome::Failed(_)) {
            failed += 1;
        }
    }
    if failed == 0 {
        Ok(())
    } else {
        Err(format!("{failed} instance(s) could not be reconciled"))
    }
}

#[cfg(test)]
mod render_config_tests {
    use super::HostConfig;

    #[test]
    fn the_render_section_overrides_the_defaults_and_the_osk_comes_from_a_file() {
        let dir = std::env::temp_dir().join(format!("cwld-render-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a temp dir");
        std::fs::write(dir.join("osk"), "synthetic-osk\n").expect("an osk file");
        let text = format!(
            r#"{{"host":"h","conductor":"/c","state_dir":"/s","inventory":"/i","host_driver":"amdgpu",
               "desktop":{{"unit":"u"}},
               "render":{{"opencore_iso":"/oc.iso","apple_smc_osk_file":"{}/osk","mmio64_mib":1024,"cpu_model":"m"}}}}"#,
            dir.display()
        );
        let config: HostConfig = serde_json::from_str(&text).expect("a host file");
        let o = config.render_options().expect("options");
        assert_eq!(o.apple_smc_osk.as_deref(), Some("synthetic-osk"));
        assert_eq!(o.mmio64_mib, 1024);
        assert_eq!(o.cpu_model.as_deref(), Some("m"));
        assert_eq!(
            o.machine, "pc-q35-11.1",
            "unset fields keep the library default"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
