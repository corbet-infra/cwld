//! `cwld` — the workload runtime's command line.
//!
//! One command, no daemon: `run-plan` takes a switch plan from `cdct` and
//! executes it against a logging fake driver, printing every call it would
//! make and changing nothing. No OS driver crate is linked in this iteration,
//! so only `--dry-run` is accepted.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use cdct::{GpuLadder, KernelOwner, PlanStep, SwitchOutcome, SwitchPlan};
use cdrv::{
    DeviceAddress, DeviceClass, DeviceFacts, DeviceFilter, EventLog, EventStream, HostDevices,
    HostGpu, HostInstances, InstanceId, InstanceSpec, InstanceState, Source, StopMode, Timestamp,
};
use clap::{Parser, Subcommand};
use cwld::{ClassCatalog, DiscardHandoff, DiskReset, Hosts, Runtime};

/// The workload runtime: executes what the conductor decided.
#[derive(Debug, Parser)]
#[command(name = "cwld", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute a switch plan against a logging fake driver.
    RunPlan {
        /// A JSON switch plan, or a `cdct plan switch --json` outcome.
        #[arg(long)]
        plan: PathBuf,
        /// Print what would be called and change nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// The CI dispatcher: Forgejo jobs onto clean one-job instances.
    #[command(subcommand)]
    Ci(CiCommand),
    /// Follow the conductor on this Linux host: execute every plan it grants
    /// through the `clnx` drivers, until its stream ends.
    #[cfg(feature = "clnx")]
    Follow {
        /// The host file (JSON).
        #[arg(long)]
        config: PathBuf,
    },
    /// Make the defined domains follow the declared instances: define a missing domain, redefine a
    /// shut-off one whose render changed, report a live one as pending and leave it. Run it when the
    /// configuration changes; it does nothing else.
    #[cfg(feature = "clnx")]
    Reconcile {
        /// The host file (JSON).
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CiCommand {
    /// Serve until SIGTERM; a drain never cuts a job in progress.
    Serve {
        /// The dispatcher configuration (TOML).
        #[arg(long)]
        config: PathBuf,
    },
    /// Remove one finished job's runner-owned directory (run by the job
    /// unit's `ExecStopPost=` as the runner account).
    Scrub {
        /// A `job.<handle>` directory.
        path: PathBuf,
    },
    /// Validate a configuration file and print what it would serve.
    Check {
        /// The dispatcher configuration (TOML).
        #[arg(long)]
        config: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::RunPlan { plan, dry_run } => run_plan(&plan, dry_run),
        #[cfg(feature = "clnx")]
        Command::Follow { config } => match cwld::linux::follow(&config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(message) => {
                eprintln!("cwld: {message}");
                ExitCode::FAILURE
            }
        },
        #[cfg(feature = "clnx")]
        Command::Reconcile { config } => match cwld::linux::reconcile_instances(&config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(message) => {
                eprintln!("cwld: {message}");
                ExitCode::FAILURE
            }
        },
        Command::Ci(CiCommand::Check { config }) => ci_check(&config),
        Command::Ci(CiCommand::Serve { config }) => ci_serve(&config),
        Command::Ci(CiCommand::Scrub { path }) => ci_scrub(&path),
    }
}

fn ci_scrub(path: &Path) -> ExitCode {
    // only a job directory, never anything else a bad argument could name
    let ok = path.is_absolute()
        && path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("job."));
    if !ok {
        eprintln!("cwld: {} is not a job directory", path.display());
        return ExitCode::FAILURE;
    }
    match cwld::dispatch::exec::scrub(path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cwld: {error}");
            ExitCode::FAILURE
        }
    }
}

fn ci_check(path: &Path) -> ExitCode {
    match cwld::dispatch::config::CiConfig::load(path) {
        Ok(cfg) => {
            for pool in &cfg.pools {
                println!(
                    "pool {} ({}): {} slot(s), {} MiB per job",
                    pool.label,
                    pool.kind,
                    pool.slots.len(),
                    pool.claim_mib
                );
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("cwld: {error}");
            ExitCode::FAILURE
        }
    }
}

fn ci_serve(path: &Path) -> ExitCode {
    use cwld::dispatch::admission::{ConductorAdmission, system_snapshot};
    use cwld::dispatch::config::{CiConfig, build, read_credential};
    let run = || -> Result<(), String> {
        let cfg = CiConfig::load(path).map_err(|e| e.to_string())?;
        let creds = std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from);
        let token =
            read_credential(creds.as_deref(), &cfg.forge.credential).map_err(|e| e.to_string())?;
        let admission = std::sync::Arc::new(ConductorAdmission::new(
            cdct::Conductor::with_defaults(),
            system_snapshot,
        ));
        let dispatcher = std::sync::Arc::new(
            build(&cfg, token, admission, |line| println!("{line}")).map_err(|e| e.to_string())?,
        );
        let stopper = std::sync::Arc::clone(&dispatcher);
        ctrlc::set_handler(move || stopper.stop()).map_err(|e| e.to_string())?;
        dispatcher.run();
        Ok(())
    };
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("cwld: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run_plan(path: &Path, dry_run: bool) -> ExitCode {
    if !dry_run {
        eprintln!("cwld: no OS driver is linked in this iteration; only --dry-run is supported");
        return ExitCode::FAILURE;
    }
    let plan = match read_plan(path) {
        Ok(plan) => plan,
        Err(message) => {
            eprintln!("cwld: {message}");
            return ExitCode::FAILURE;
        }
    };
    let state = std::env::temp_dir().join(format!("cwld-dry-run-{}", std::process::id()));
    let mut runtime = match Runtime::new(&state, ClassCatalog::defaults()) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cwld: cannot open the dry-run state directory: {error}");
            return ExitCode::FAILURE;
        }
    };
    let host = LoggingHost::new();
    let hosts = Hosts::new(&host, &host)
        .with_gpu(&host)
        .with_disk_reset(&host);
    let mut log = EventLog::new("dry-run", Source::Cwld, 1);
    println!("plan: switch the card to {}", plan.target);
    for (index, step) in plan.steps.iter().enumerate() {
        println!("  {}. {}", index + 1, step_line(step));
    }
    let report = runtime.execute_plan(
        &plan,
        &hosts,
        &mut log,
        Timestamp::from_millis(0),
        &mut DiscardHandoff,
    );
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            eprintln!("cwld: cannot write the plan journal: {error}");
            let _ = std::fs::remove_dir_all(&state);
            return ExitCode::FAILURE;
        }
    };
    for step in &report.not_run {
        println!("not run: {}", step_line(step));
    }
    match &report.failed {
        Some(failed) => println!(
            "failed at step {}: {} ({})",
            failed.index + 1,
            step_line(&failed.step),
            failed.error
        ),
        None => println!("result: complete ({} steps)", report.executed.len()),
    }
    let _ = std::fs::remove_dir_all(&state);
    if report.is_complete() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

fn read_plan(path: &Path) -> Result<SwitchPlan, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if let Ok(plan) = serde_json::from_str::<SwitchPlan>(&text) {
        return Ok(plan);
    }
    match serde_json::from_str::<SwitchOutcome>(&text) {
        Ok(SwitchOutcome::Plan(plan)) => Ok(plan),
        Ok(SwitchOutcome::AlreadyHeld { kernel }) => Err(format!(
            "the card already belongs to {kernel}; there is no plan to run"
        )),
        Ok(SwitchOutcome::Wait { reason }) => {
            Err(format!("the outcome is a wait, not a plan: {reason:?}"))
        }
        Ok(SwitchOutcome::Decline { reason }) => {
            Err(format!("the outcome is a decline, not a plan: {reason:?}"))
        }
        Err(error) => Err(format!(
            "{} is neither a switch plan nor a switch outcome: {error}",
            path.display()
        )),
    }
}

fn step_line(step: &PlanStep) -> String {
    match step {
        PlanStep::StopHostTenant { tenant, ladder } => {
            format!("stop host tenant {tenant} ({ladder})")
        }
        PlanStep::StopInstance { instance, mode } => {
            format!("stop instance {instance} ({mode:?})")
        }
        PlanStep::HandOverCard { device, from, to } => {
            format!("hand the card {device} over: {from} -> {to}")
        }
        PlanStep::StartInstance { instance } => format!("start instance {instance}"),
        PlanStep::StartHostTenant {
            tenant,
            ladder,
            replicas,
        } => {
            format!("start host tenant {tenant} ({ladder}), {replicas} replica(s)")
        }
        PlanStep::SunshineFollows { kernel } => format!("Sunshine follows {kernel}"),
        PlanStep::RestartDevicePlugin => "restart the GPU device plugin".to_owned(),
    }
}

/// A synthetic PCI function the dry run resolves the card's name to.
///
/// A dry run never reads the host's hardware inventory, so it answers with one
/// placeholder function; it is printed, never used.
const SYNTHETIC_GPU: DeviceAddress = DeviceAddress {
    domain: 0,
    bus: 1,
    slot: 0,
    function: 0,
};

/// A fake host that prints every call it would make.
struct LoggingHost {
    states: RefCell<BTreeMap<String, InstanceState>>,
}

impl LoggingHost {
    fn new() -> Self {
        Self {
            states: RefCell::new(BTreeMap::new()),
        }
    }

    fn log(&self, call: &str) {
        println!("  would call {call}");
    }

    fn state_of(&self, id: &InstanceId) -> InstanceState {
        self.states
            .borrow()
            .get(id.as_str())
            .cloned()
            .unwrap_or(InstanceState::Defined)
    }

    fn set(&self, id: &InstanceId, state: InstanceState) {
        self.states
            .borrow_mut()
            .insert(id.as_str().to_owned(), state);
    }
}

impl HostInstances for LoggingHost {
    fn list(&self) -> cdrv::Result<Vec<(InstanceId, InstanceState)>> {
        self.log("list()");
        Ok(self
            .states
            .borrow()
            .iter()
            .map(|(id, state)| (InstanceId::from(id.as_str()), state.clone()))
            .collect())
    }

    fn define(&self, spec: &InstanceSpec) -> cdrv::Result<()> {
        self.log(&format!("define({})", spec.id));
        self.set(&spec.id, InstanceState::Defined);
        Ok(())
    }

    fn start(&self, id: &InstanceId) -> cdrv::Result<()> {
        self.log(&format!("start({id})"));
        self.set(id, InstanceState::Running);
        Ok(())
    }

    fn stop(&self, id: &InstanceId, mode: StopMode) -> cdrv::Result<()> {
        self.log(&format!("stop({id}, {mode:?})"));
        self.set(id, InstanceState::Stopped);
        Ok(())
    }

    fn destroy(&self, id: &InstanceId) -> cdrv::Result<()> {
        self.log(&format!("destroy({id})"));
        self.set(id, InstanceState::Stopped);
        Ok(())
    }

    fn state(&self, id: &InstanceId) -> cdrv::Result<InstanceState> {
        Ok(self.state_of(id))
    }

    fn events(&self) -> cdrv::Result<EventStream> {
        Ok(Box::new(std::iter::empty()))
    }
}

impl HostDevices for LoggingHost {
    fn enumerate(&self, filter: DeviceFilter) -> cdrv::Result<Vec<DeviceFacts>> {
        self.log(&format!("enumerate({filter:?})"));
        let Some(name) = filter.name else {
            return Ok(Vec::new());
        };
        Ok(vec![DeviceFacts {
            name,
            address: Some(SYNTHETIC_GPU),
            vendor: None,
            class: DeviceClass::Gpu,
            iommu_group: None,
            driver: None,
            bar0_bytes: None,
        }])
    }

    fn bind_passthrough(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.log(&format!("bind_passthrough({})", address(*dev)));
        Ok(())
    }

    fn unbind_passthrough(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.log(&format!("unbind_passthrough({})", address(*dev)));
        Ok(())
    }

    fn reset(&self, dev: &DeviceAddress) -> cdrv::Result<()> {
        self.log(&format!("reset({})", address(*dev)));
        Ok(())
    }
}

impl HostGpu for LoggingHost {
    fn stop_tenant(&self, tenant: &str, ladder: GpuLadder) -> cdrv::Result<()> {
        self.log(&format!("stop_tenant({tenant}, {ladder})"));
        Ok(())
    }

    fn tenant_replicas(&self, tenant: &str, ladder: GpuLadder) -> cdrv::Result<u32> {
        self.log(&format!("tenant_replicas({tenant}, {ladder})"));
        Ok(1)
    }

    fn start_tenant(&self, tenant: &str, ladder: GpuLadder, replicas: u32) -> cdrv::Result<()> {
        self.log(&format!("start_tenant({tenant}, {ladder}, {replicas})"));
        Ok(())
    }

    fn sunshine_follows(&self, kernel: &KernelOwner) -> cdrv::Result<()> {
        self.log(&format!("sunshine_follows({kernel})"));
        Ok(())
    }

    fn restart_device_plugin(&self) -> cdrv::Result<()> {
        self.log("restart_device_plugin()");
        Ok(())
    }
}

impl DiskReset for LoggingHost {
    fn reset_to_clean(&self, instance: &InstanceId) -> cdrv::Result<()> {
        self.log(&format!("reset_to_clean({instance})"));
        Ok(())
    }
}

fn address(dev: DeviceAddress) -> String {
    format!(
        "{:04x}:{:02x}:{:02x}.{}",
        dev.domain, dev.bus, dev.slot, dev.function
    )
}
