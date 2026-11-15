//! The dispatcher's configuration (TOML), validated at load.
//!
//! Nothing secret lives here: the forge credential is read from the
//! credentials directory the deployment (`nixci`) provides.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::admission::Admission;
use super::exec::{Executor, HostExecutor, HostSettings};
use super::flow::{Dispatcher, ExecutorFactory, Pool, SlotCfg, Tuning};
use super::forge::{ForgeClient, is_name};
use super::http::CurlHttp;
use super::secret::Secret;
use super::units::SystemdRun;

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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
/// One pool.
pub struct PoolSection {
    /// The label.
    pub label: String,
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
    /// State directory (job directories, caches).
    pub state_dir: PathBuf,
    /// Where systemd exposes a unit's credentials.
    #[serde(default = "d_cred_root")]
    pub credentials_root: PathBuf,
    /// The pools.
    #[serde(rename = "pool")]
    pub pools: Vec<PoolSection>,
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
            match pool.kind.as_str() {
                "host" => {
                    if pool.runner.is_none() || pool.template.is_none() {
                        return bad(format!(
                            "host pool {} needs `runner` and `template`",
                            pool.label
                        ));
                    }
                }
                "vm" => {}
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
/// pools. VM pools need a [`super::exec::VmDriver`] and are refused here until
/// the OS driver can stream a guest job's output.
///
/// # Errors
/// A pool the build cannot serve, or an unreadable file.
pub fn build(
    cfg: &CiConfig,
    token: Secret,
    admission: Arc<dyn Admission>,
    log: impl Fn(&str) + Send + Sync + 'static,
) -> Result<Dispatcher, ConfigError> {
    let forge = Arc::new(ForgeClient::new(
        Arc::new(CurlHttp::default()),
        &cfg.forge.api,
        &cfg.forge.runner_url,
        token,
        &cfg.forge.runner_version,
        cfg.forge.include_user,
    ));
    let units = Arc::new(SystemdRun::default());
    let mut pools = Vec::new();
    for p in &cfg.pools {
        if p.kind != "host" {
            return Err(ConfigError::Invalid(format!(
                "pool {}: no VM driver is linked yet (needs a streaming guest exec); only host pools are served",
                p.label
            )));
        }
        let template_path = p.template.clone().unwrap_or_default();
        let template: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&template_path).map_err(|e| {
                ConfigError::Load(format!("cannot read {}: {e}", template_path.display()))
            })?)
            .map_err(|e| ConfigError::Load(format!("{}: {e}", template_path.display())))?;
        let settings = HostSettings {
            runner: p.runner.clone().unwrap_or_default(),
            template,
            state_dir: cfg.state_dir.clone(),
            cache_dir: cfg.state_dir.join("cache"),
            pickup: Duration::from_secs(p.pickup_s),
            properties: p.properties.clone(),
            credentials_root: cfg.credentials_root.clone(),
            kill_rounds: 50,
            kill_pause: Duration::from_millis(200),
        };
        let units = Arc::clone(&units);
        let factory: ExecutorFactory = Arc::new(move |slot: &str| -> Box<dyn Executor> {
            Box::new(HostExecutor::new(units.clone(), settings.clone(), slot))
        });
        pools.push(Pool {
            label: p.label.clone(),
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
        });
    }
    Ok(Dispatcher::new(forge, admission, pools, cfg.tuning(), log))
}
