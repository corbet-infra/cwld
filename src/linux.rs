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
use crate::handoff::DaemonHandoff;
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
    /// The instances this host runs.
    #[serde(default)]
    pub instances: Vec<InstanceEntry>,
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
) -> Result<PlanReport, String> {
    let (name, functions) = Inventory::load(&config.inventory)?;
    let libvirt = RealLibvirt::connect(&config.libvirt_uri).map_err(|error| error.to_string())?;
    let mut instances = LinuxInstances::new(config.host.clone(), libvirt, RenderOptions::default());
    for entry in &config.instances {
        if let Some(argv) = &entry.clean_stop {
            instances = instances.with_clean_stop(entry.spec.id.as_str(), argv.clone());
        }
    }
    let devices = InventoryDevices {
        inner: LinuxDevices::new(
            LinuxSysfs::system(),
            LinuxKernel::system(),
            functions.clone(),
            config.host_driver.clone(),
        )
        .with_holder_wait(Duration::from_secs(config.holder_wait_secs)),
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
        },
    );
    let hosts = Hosts::new(&instances, &devices).with_gpu(&gpu);
    let mut log = EventLog::new(config.host.clone(), Source::Cwld, 1);
    let mut handoff = DaemonHandoff::new(&config.conductor);
    let at: Timestamp = SystemClock.now();
    runtime
        .execute_plan(plan, &hosts, &mut log, at, &mut handoff)
        .map_err(|error| format!("the plan journal could not be written: {error}"))
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
    let mut client = Client::connect(&Endpoint::LocalSocket(config.conductor.clone()))
        .map_err(|error| format!("cannot reach the conductor: {error}"))?;
    client
        .send(&Request::Subscribe)
        .map_err(|error| format!("cannot subscribe: {error}"))?;
    println!(
        "cwld: following the conductor at {}",
        config.conductor.display()
    );
    while let Some(reply) = client
        .receive()
        .map_err(|error| format!("the conductor stream failed: {error}"))?
    {
        if let Reply::Granted { plan } = reply {
            println!(
                "cwld: granted: switch the card to {} ({} step(s))",
                plan.target,
                plan.steps.len()
            );
            match execute(&mut runtime, &config, &plan) {
                Ok(done) => report(&done),
                Err(message) => eprintln!(
                    "cwld: the plan to {} was NOT started: {message}",
                    plan.target
                ),
            }
        }
    }
    Err("the conductor closed the stream".to_owned())
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
