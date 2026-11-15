//! The dispatcher's configuration (TOML), validated at load.
//!
//! Nothing secret lives here: the forge credential is read from the
//! credentials directory the deployment (a separate repository) provides.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::admission::Admission;
use super::exec::{Executor, HostExecutor, HostSettings, VmDriver, VmExecutor};
use super::flow::{Dispatcher, ExecutorFactory, Pool, SlotCfg, Tuning};
use super::forge::{ForgeClient, is_name};
use super::http::CurlHttp;
use super::secret::Secret;
use super::units::{SystemdRun, UnitRunner};
use super::unitvm::{UnitVmDriver, VmCommands};
use super::vmdriver::VmSettings;

/// A configuration problem.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read or parsed.
    #[error("{0}")]
    Load(String),
    /// A value is invalid.
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// The forge to serve.
pub struct ForgeSection {
    /// REST base, e.g. `https://forge.example/api/v1`.
    pub api: String,
    /// Forge root, e.g. `https://forge.example/`.
    pub runner_url: String,
    /// What the runner protocol's `Register` reports as version.
    pub runner_version: String,
    /// The file name of the read-scope credential in the credentials directory.
    #[serde(default = "default_credential")]
    pub credential: String,
    /// Whether the token owner's own namespace is served.
    #[serde(default = "yes")]
    pub include_user: bool,
}

fn default_credential() -> String {
    "mint".to_owned()
}
fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// Listing cadence, in milliseconds or seconds as named.
pub struct CadenceSection {
    /// Listing interval while work was seen lately (ms).
    #[serde(default = "d_poll")]
    pub poll_ms: u64,
    /// Listing interval while idle (ms).
    #[serde(default = "d_idle")]
    pub idle_ms: u64,
    /// Scope derivation reuse (s).
    #[serde(default = "d_scope")]
    pub scope_ttl_s: u64,
    /// Claim lifetime (s).
    #[serde(default = "d_claim")]
    pub claim_ttl_s: u64,
}

fn d_poll() -> u64 {
    1500
}
fn d_idle() -> u64 {
    4000
}
fn d_scope() -> u64 {
    60
}
fn d_claim() -> u64 {
    120
}

impl Default for CadenceSection {
    fn default() -> Self {
        Self {
            poll_ms: d_poll(),
            idle_ms: d_idle(),
            scope_ttl_s: d_scope(),
            claim_ttl_s: d_claim(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// One slot of a pool.
pub struct SlotSection {
    /// The slot id.
    pub id: String,
    /// The runner name it registers under.
    pub name: String,
    /// Job environment values of this slot only (the sccache port, ...).
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// A credential file shipped into a VM job.
pub struct VmCredential {
    /// The name in the job's credentials directory (`sccache-aws`).
    pub name: String,
    /// The host path (a credential of the dispatcher's own unit).
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// How a `vm` pool reaches its clones: commands run as units of `run_as`.
/// `{slot}` in an argument is the instance.
pub struct VmSection {
    /// The guest transport; the guest command is appended.
    pub transport: Vec<String>,
    /// Roll a dirty clone back first (optional).
    #[serde(default)]
    pub ensure_clean: Vec<String>,
    /// Start the clone; status 75 means the memory guard refused.
    pub wake: Vec<String>,
    /// Install the guest tools (optional).
    #[serde(default)]
    pub sync: Vec<String>,
    /// Power off and roll back to the clean snapshot.
    pub reset: Vec<String>,
    /// Boot clean and hibernate (optional).
    #[serde(default)]
    pub standby: Vec<String>,
    /// The guest path of the job script.
    #[serde(default = "d_guest_job")]
    pub guest_job: String,
    /// The guest directory below which a job directory is made.
    #[serde(default = "d_guest_tmp")]
    pub guest_tmp: String,
    /// Credentials shipped with each job.
    #[serde(default)]
    pub credentials: Vec<VmCredential>,
    /// The placement view ccid reads (optional).
    pub placement: Option<PathBuf>,
    /// Seconds a guest job may run.
    #[serde(default = "d_job_timeout")]
    pub job_timeout_s: u64,
}

fn d_guest_job() -> String {
    "/Volumes/cache/bin/vm-guest-job".to_owned()
}
fn d_guest_tmp() -> String {
    "/tmp".to_owned()
}
fn d_job_timeout() -> u64 {
    3 * 3600 + 600
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// One pool.
pub struct PoolSection {
    /// The label.
    pub label: String,
    /// Further labels the pool serves (`heavy` beside `linux`).
    #[serde(default)]
    pub aliases: Vec<String>,
    /// `host` (a transient unit) or `vm` (a ci-class instance).
    pub kind: String,
    /// RAM claim per job, MiB.
    pub claim_mib: u64,
    /// Seconds without a task before the registration is given back.
    #[serde(default = "d_pickup")]
    pub pickup_s: u64,
    /// The slots.
    pub slots: Vec<SlotSection>,
    /// Host pools: the forgejo-runner binary.
    pub runner: Option<PathBuf>,
    /// Host pools: the runner configuration template (JSON, no connection).
    pub template: Option<PathBuf>,
    /// Host pools: unit properties from the deployment.
    #[serde(default)]
    pub properties: Vec<String>,
    /// VM pools: how the clones are reached.
    pub vm: Option<VmSection>,
    /// Host pools: the account the runner process runs as. The dispatcher
    /// starts the unit with `User=` this name; a host pool must name one (a
    /// transient unit would otherwise run as root).
    pub run_as: Option<String>,
    /// Host pools: a command (argv) run as the job's account before the runner
    /// starts, to take the resource the job owns (the Windows test clone is woken).
    #[serde(default)]
    pub pre: Vec<String>,
    /// Host pools: a command run after the job ended, whatever happened (the
    /// clone is rolled back); the slot is free again only when it succeeded.
    #[serde(default)]
    pub post: Vec<String>,
    /// Host pools: the job environment. A value may hold `{unit}`, `{slot}`,
    /// `{job_dir}` and `{credentials}`.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

fn d_pickup() -> u64 {
    20
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// The whole file.
pub struct CiConfig {
    /// The forge.
    pub forge: ForgeSection,
    /// The cadence.
    #[serde(default)]
    pub cadence: CadenceSection,
    /// The dispatcher's own directory (per job: configuration and credential file).
    pub state_dir: PathBuf,
    /// Where systemd keeps state directories.
    #[serde(default = "d_state_root")]
    pub state_root: PathBuf,
    /// The runner account's state directory below `state_root`: job
    /// directories (`slots/<slot>/job.<handle>`) and the runner cache
    /// (`cache/<slot>`) live there.
    #[serde(default = "d_state_directory")]
    pub state_directory: String,
    /// Where systemd exposes a unit's credentials.
    #[serde(default = "d_cred_root")]
    pub credentials_root: PathBuf,
    /// The slice the job units run in (`systemd-run --slice`); the cgroup
    /// checks look below it.
    #[serde(default = "d_slice")]
    pub slice: String,
    /// The pools.
    #[serde(rename = "pool")]
    pub pools: Vec<PoolSection>,
}

fn d_state_root() -> PathBuf {
    PathBuf::from("/var/lib")
}
fn d_slice() -> String {
    "system.slice".to_owned()
}
fn d_state_directory() -> String {
    "forgejo-runner".to_owned()
}

fn d_cred_root() -> PathBuf {
    PathBuf::from("/run/credentials")
}

impl CiConfig {
    /// Parse and validate.
    ///
    /// # Errors
    /// Unreadable, unparsable or invalid configuration.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Load(format!("cannot read {}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// Parse and validate text.
    ///
    /// # Errors
    /// Unparsable or invalid configuration.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(text).map_err(|e| ConfigError::Load(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if !self.forge.runner_url.starts_with("https://")
            && !self.forge.runner_url.starts_with("http://127.0.0.1:")
        {
            return bad(format!(
                "runner_url {:?} must be https://host/",
                self.forge.runner_url
            ));
        }
        if self.state_directory.is_empty()
            || self.state_directory.starts_with('/')
            || self
                .state_directory
                .split('/')
                .any(|c| c.is_empty() || c == "..")
        {
            return bad(format!(
                "state_directory {:?} must be a relative path below state_root",
                self.state_directory
            ));
        }
        let (mut labels, mut slots) = (
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
        );
        if self.pools.is_empty() {
            return bad("no pool is configured".to_owned());
        }
        for pool in &self.pools {
            if !is_name(&pool.label) || !labels.insert(pool.label.clone()) {
                return bad(format!(
                    "pool label {:?} is invalid or repeated",
                    pool.label
                ));
            }
            for alias in &pool.aliases {
                if !is_name(alias) || !labels.insert(alias.clone()) {
                    return bad(format!(
                        "pool {:?}: alias {alias:?} is invalid or repeated",
                        pool.label
                    ));
                }
            }
            if pool.slots.is_empty() {
                return bad(format!("pool {} has no slot", pool.label));
            }
            for slot in &pool.slots {
                if !is_name(&slot.id) || !is_name(&slot.name) || !slots.insert(slot.id.clone()) {
                    return bad(format!(
                        "pool {}: slot {:?} / name {:?} is invalid or repeated",
                        pool.label, slot.id, slot.name
                    ));
                }
            }
            if let Some(account) = &pool.run_as {
                if !is_name(account) {
                    return bad(format!(
                        "pool {}: run_as {account:?} is invalid",
                        pool.label
                    ));
                }
                if pool.properties.iter().any(|p| p.starts_with("User=")) {
                    return bad(format!(
                        "pool {}: `run_as` and a `User=` property say the same twice",
                        pool.label
                    ));
                }
            }
            for name in pool
                .environment
                .keys()
                .chain(pool.slots.iter().flat_map(|s| s.environment.keys()))
            {
                if !is_env_name(name) {
                    return bad(format!(
                        "pool {}: environment name {name:?} is invalid",
                        pool.label
                    ));
                }
            }
            match pool.kind.as_str() {
                "host" => {
                    if pool.run_as.is_none() {
                        return bad(format!("host pool {} needs `run_as`", pool.label));
                    }
                    if pool.runner.is_none() || pool.template.is_none() {
                        return bad(format!(
                            "host pool {} needs `runner` and `template`",
                            pool.label
                        ));
                    }
                }
                "vm" => {
                    if pool.run_as.is_none() || pool.template.is_none() {
                        return bad(format!(
                            "vm pool {} needs `run_as` and `template`",
                            pool.label
                        ));
                    }
                    match &pool.vm {
                        Some(vm)
                            if !vm.transport.is_empty()
                                && !vm.wake.is_empty()
                                && !vm.reset.is_empty() => {}
                        _ => {
                            return bad(format!(
                                "vm pool {} needs a [pool.vm] section with `transport`, `wake` and `reset`",
                                pool.label
                            ));
                        }
                    }
                }
                other => return bad(format!("pool {}: unknown kind {other:?}", pool.label)),
            }
        }
        Ok(())
    }

    /// The tuning knobs.
    #[must_use]
    pub fn tuning(&self) -> Tuning {
        Tuning {
            poll: Duration::from_millis(self.cadence.poll_ms),
            poll_idle: Duration::from_millis(self.cadence.idle_ms),
            scope_ttl: Duration::from_secs(self.cadence.scope_ttl_s),
            claim_ttl: Duration::from_secs(self.cadence.claim_ttl_s),
            runner_url: self.forge.runner_url.clone(),
            ..Tuning::default()
        }
    }
}

fn is_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Read the forge credential from the credentials directory.
///
/// # Errors
/// The directory is not set or the file is unreadable.
pub fn read_credential(dir: Option<&Path>, name: &str) -> Result<Secret, ConfigError> {
    let dir =
        dir.ok_or_else(|| ConfigError::Load("CREDENTIALS_DIRECTORY is not set".to_owned()))?;
    let path = dir.join(name);
    let text = std::fs::read_to_string(&path).map_err(|e| {
        ConfigError::Load(format!(
            "cannot read the credential {}: {e}",
            path.display()
        ))
    })?;
    Ok(Secret::new(text.trim()))
}

/// Build the production dispatcher: curl for the forge, systemd units for host
/// pools. VM pools need an OS driver behind [`super::vmdriver::RuntimeVmDriver`]
/// and are refused here until one is linked.
///
/// # Errors
/// A pool the build cannot serve, or an unreadable file.
pub fn build(
    cfg: &CiConfig,
    token: Secret,
    admission: Arc<dyn Admission>,
    log: impl Fn(&str) + Send + Sync + 'static,
) -> Result<Dispatcher, ConfigError> {
    let log: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(log);
    let forge = Arc::new(ForgeClient::new(
        Arc::new(CurlHttp::default()),
        &cfg.forge.api,
        &cfg.forge.runner_url,
        token,
        &cfg.forge.runner_version,
        cfg.forge.include_user,
    ));
    let units = Arc::new(SystemdRun {
        slice: cfg.slice.clone(),
        ..SystemdRun::default()
    });
    // what a crashed predecessor left running holds CPUs and caches: end it
    if let Ok(left) = units.sweep("cwld-ci-") {
        for unit in left {
            log(&format!("swept unit={unit}"));
        }
    }
    let mut pools = Vec::new();
    for p in &cfg.pools {
        let template_path = p.template.clone().unwrap_or_default();
        let template: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&template_path).map_err(|e| {
                ConfigError::Load(format!("cannot read {}: {e}", template_path.display()))
            })?)
            .map_err(|e| ConfigError::Load(format!("{}: {e}", template_path.display())))?;
        let mut properties = p.properties.clone();
        if let Some(name) = &p.run_as {
            properties.push(format!("User={name}"));
        }
        let settings = HostSettings {
            runner: p.runner.clone().unwrap_or_default(),
            template,
            state_dir: cfg.state_dir.clone(),
            state_root: cfg.state_root.clone(),
            state_directory: cfg.state_directory.clone(),
            scrub_bin: std::env::current_exe()
                .map_err(|e| ConfigError::Load(format!("cannot find myself: {e}")))?,
            pickup: Duration::from_secs(p.pickup_s),
            properties,
            pre: p.pre.clone(),
            post: p.post.clone(),
            environment: p.environment.clone().into_iter().collect(),
            slot_environment: Vec::new(),
            credentials_root: cfg.credentials_root.clone(),
            kill_rounds: 50,
            kill_pause: Duration::from_millis(200),
        };
        let units = Arc::clone(&units);
        if p.kind == "vm" {
            // validated: a vm pool has its section
            let vm = p.vm.clone().unwrap_or_else(unreachable_vm);
            let driver: Arc<dyn VmDriver> = Arc::new(UnitVmDriver::new(
                units.clone(),
                VmCommands {
                    transport: vm.transport,
                    ensure_clean: vm.ensure_clean,
                    wake: vm.wake,
                    sync: vm.sync,
                    reset: vm.reset,
                    standby: vm.standby,
                },
                VmSettings {
                    template: settings.template.clone(),
                    guest_job: vm.guest_job,
                    guest_tmp: vm.guest_tmp,
                    credentials: vm
                        .credentials
                        .into_iter()
                        .map(|c| (c.name, c.path))
                        .collect(),
                    placement: vm.placement,
                    job_timeout: Duration::from_secs(vm.job_timeout_s),
                },
                settings.properties.clone(),
                settings.environment.clone(),
                settings.state_root.join(&settings.state_directory),
                {
                    let log = Arc::clone(&log);
                    move |line| log(line)
                },
            ));
            let pickup = settings.pickup;
            let factory: ExecutorFactory = Arc::new(move |slot: &str| -> Box<dyn Executor> {
                Box::new(VmExecutor::new(driver.clone(), slot, pickup))
            });
            pools.push(pool_of(p, factory));
            continue;
        }
        let slot_env: HashMap<String, Vec<(String, String)>> = p
            .slots
            .iter()
            .map(|s| (s.id.clone(), s.environment.clone().into_iter().collect()))
            .collect();
        let factory: ExecutorFactory = Arc::new(move |slot: &str| -> Box<dyn Executor> {
            let mut settings = settings.clone();
            settings.slot_environment = slot_env.get(slot).cloned().unwrap_or_default();
            Box::new(HostExecutor::new(units.clone(), settings, slot))
        });
        pools.push(pool_of(p, factory));
    }
    Ok(Dispatcher::new(
        forge,
        admission,
        pools,
        cfg.tuning(),
        move |line| log(line),
    ))
}

fn unreachable_vm() -> VmSection {
    // the loader refuses a vm pool without its section
    VmSection {
        transport: Vec::new(),
        ensure_clean: Vec::new(),
        wake: Vec::new(),
        sync: Vec::new(),
        reset: Vec::new(),
        standby: Vec::new(),
        guest_job: d_guest_job(),
        guest_tmp: d_guest_tmp(),
        credentials: Vec::new(),
        placement: None,
        job_timeout_s: d_job_timeout(),
    }
}

fn pool_of(p: &PoolSection, factory: ExecutorFactory) -> Pool {
    Pool {
        label: p.label.clone(),
        aliases: p.aliases.clone(),
        claim_mib: p.claim_mib,
        slots: p
            .slots
            .iter()
            .map(|s| SlotCfg {
                id: s.id.clone(),
                name: s.name.clone(),
            })
            .collect(),
        factory,
    }
}
