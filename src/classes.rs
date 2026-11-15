//! Classes as validated data.
//!
//! The four classes are exactly the ones `STATUS.md` decides: `dev`,
//! `desktop`, `ci` and `office`. Their rules are loaded from a TOML or JSON
//! file and validated at load, so invalid class data is an error before
//! anything runs. `cwld` derives no class policy of its own: it executes the
//! data it was given.

use std::path::Path;
use std::time::Duration;

use cdrv::{InstanceClass, StopMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The fixed claim of a desktop class instance: 24 GiB, pinned by VFIO.
pub const DESKTOP_MIB: u64 = 24 * 1024;

/// The size classes a `dev` instance may take as its ceiling, in MiB.
pub const DEV_SIZE_CLASSES_MIB: [u64; 4] = [8 * 1024, 12 * 1024, 16 * 1024, 24 * 1024];

/// How a class gets its memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MemoryRule {
    /// A fixed claim that never balloons (a GPU VM pinned by VFIO).
    Fixed {
        /// The claim, in MiB.
        mib: u64,
    },
    /// A claim the runtime may move between the floor and the ceiling.
    Balloon {
        /// The floor, in MiB.
        min_mib: u64,
        /// The ceiling, in MiB.
        max_mib: u64,
        /// The size classes an instance may take as its ceiling, in MiB.
        #[serde(default)]
        size_classes_mib: Vec<u64>,
    },
}

impl MemoryRule {
    /// Whether the runtime may move this claim while the instance runs.
    #[must_use]
    pub const fn balloons(&self) -> bool {
        matches!(self, Self::Balloon { .. })
    }

    /// The fixed claim, when the class has one.
    #[must_use]
    pub const fn fixed_mib(&self) -> Option<u64> {
        match self {
            Self::Fixed { mib } => Some(*mib),
            Self::Balloon { .. } => None,
        }
    }

    /// The largest claim an instance of this class may take, in MiB.
    #[must_use]
    pub const fn max_mib(&self) -> u64 {
        match self {
            Self::Fixed { mib } => *mib,
            Self::Balloon { max_mib, .. } => *max_mib,
        }
    }

    /// The size classes an instance may take as its ceiling, in MiB.
    #[must_use]
    pub fn size_classes_mib(&self) -> &[u64] {
        match self {
            Self::Fixed { .. } => &[],
            Self::Balloon {
                size_classes_mib, ..
            } => size_classes_mib,
        }
    }
}

/// Whether an instance of a class keeps its disk between jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Persistence {
    /// The instance keeps its disk; nobody resets it per job.
    Persistent,
    /// The instance is a fresh clone for one job.
    Ephemeral,
}

impl Persistence {
    const fn name(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Ephemeral => "ephemeral",
        }
    }
}

/// When the runtime resets an instance's disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetPolicy {
    /// Never per job.
    Never,
    /// After every job.
    AfterEachJob,
}

impl ResetPolicy {
    const fn name(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::AfterEachJob => "after_each_job",
        }
    }
}

/// Which exclusive devices a class needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceRule {
    /// No exclusive device.
    None,
    /// The GPU, passed through with fixed memory.
    Gpu,
}

impl DeviceRule {
    const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gpu => "gpu",
        }
    }
}

/// When an instance of a class may hibernate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hibernation {
    /// Never.
    Never,
    /// When idle; never for a GPU handover, which the handover stop mode
    /// carries separately.
    WhenIdle,
}

impl Hibernation {
    const fn name(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::WhenIdle => "when_idle",
        }
    }
}

/// The validated rules of one class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassRules {
    /// How the class gets its memory.
    pub memory: MemoryRule,
    /// Whether the disk survives a job.
    pub persistence: Persistence,
    /// When the disk is reset.
    pub reset: ResetPolicy,
    /// Which exclusive devices the class needs.
    pub gpu: DeviceRule,
    /// When the instance may hibernate.
    pub hibernation: Hibernation,
    /// How the instance is stopped for a GPU handover. Always `clean`.
    pub handover_stop: StopMode,
    /// Whether the guest exposes command execution (the `ci` class only).
    pub exec: bool,
    /// Whether the guest reports free pages (the balloon's other half).
    pub free_page_reporting: bool,
    /// How long a start may take before it counts as failed.
    pub readiness_timeout_secs: u64,
    /// How long a clean stop may take before a human is needed.
    pub stop_timeout_secs: u64,
}

impl ClassRules {
    /// Whether the disk is a fresh clone for one job.
    #[must_use]
    pub const fn is_ephemeral(&self) -> bool {
        matches!(self.persistence, Persistence::Ephemeral)
    }

    /// Whether the disk is reset after every job.
    #[must_use]
    pub const fn reset_after_each_job(&self) -> bool {
        matches!(self.reset, ResetPolicy::AfterEachJob)
    }

    /// Whether the class needs the GPU passed through.
    #[must_use]
    pub const fn needs_gpu(&self) -> bool {
        matches!(self.gpu, DeviceRule::Gpu)
    }

    /// The readiness timeout as a duration.
    #[must_use]
    pub const fn readiness_timeout(&self) -> Duration {
        Duration::from_secs(self.readiness_timeout_secs)
    }

    /// The stop timeout as a duration.
    #[must_use]
    pub const fn stop_timeout(&self) -> Duration {
        Duration::from_secs(self.stop_timeout_secs)
    }
}

/// The on-disk class document: all four classes, each required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassFile {
    dev: ClassRules,
    desktop: ClassRules,
    ci: ClassRules,
    office: ClassRules,
}

/// The four classes, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassCatalog {
    dev: ClassRules,
    desktop: ClassRules,
    ci: ClassRules,
    office: ClassRules,
}

impl ClassCatalog {
    /// The house class data, exactly as `STATUS.md` decides it.
    #[must_use]
    pub fn defaults() -> Self {
        Self::from_file(house())
    }

    /// Parse and validate TOML class data.
    pub fn from_toml(text: &str) -> Result<Self, ParseError> {
        let file: ClassFile = toml::from_str(text).map_err(|source| ParseError::Toml { source })?;
        Self::validated(file)
    }

    /// Parse and validate JSON class data.
    pub fn from_json(text: &str) -> Result<Self, ParseError> {
        let file: ClassFile =
            serde_json::from_str(text).map_err(|source| ParseError::Json { source })?;
        Self::validated(file)
    }

    /// Read, parse and validate a class file, by its `.toml` or `.json`
    /// extension.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| LoadError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let parsed = match path.extension().and_then(std::ffi::OsStr::to_str) {
            Some("toml") => Self::from_toml(&text),
            Some("json") => Self::from_json(&text),
            _ => {
                return Err(LoadError::UnknownFormat {
                    path: path.display().to_string(),
                });
            }
        };
        parsed.map_err(|source| LoadError::Parse {
            path: path.display().to_string(),
            source,
        })
    }

    /// The rules of one class.
    #[must_use]
    pub const fn rules(&self, class: InstanceClass) -> &ClassRules {
        match class {
            InstanceClass::Dev => &self.dev,
            InstanceClass::Desktop => &self.desktop,
            InstanceClass::Ci => &self.ci,
            InstanceClass::Office => &self.office,
        }
    }

    /// Whether an instance of this class is a fresh clone for one job.
    #[must_use]
    pub const fn is_ephemeral(&self, class: InstanceClass) -> bool {
        self.rules(class).is_ephemeral()
    }

    fn validated(file: ClassFile) -> Result<Self, ParseError> {
        validate(InstanceClass::Dev, &file.dev)?;
        validate(InstanceClass::Desktop, &file.desktop)?;
        validate(InstanceClass::Ci, &file.ci)?;
        validate(InstanceClass::Office, &file.office)?;
        Ok(Self::from_file(file))
    }

    fn from_file(file: ClassFile) -> Self {
        Self {
            dev: file.dev,
            desktop: file.desktop,
            ci: file.ci,
            office: file.office,
        }
    }
}

/// Class data that does not parse.
#[derive(Debug, Error)]
pub enum ParseError {
    /// The document is not valid TOML.
    #[error("not valid TOML: {source}")]
    Toml {
        /// The underlying TOML error.
        #[source]
        source: toml::de::Error,
    },
    /// The document is not valid JSON.
    #[error("not valid JSON: {source}")]
    Json {
        /// The underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// The document parses but breaks a class rule.
    #[error("invalid class data: {0}")]
    Invalid(#[from] ValidationError),
}

/// A class file that could not be loaded.
#[derive(Debug, Error)]
pub enum LoadError {
    /// The file could not be read.
    #[error("cannot read class data {path}: {source}")]
    Read {
        /// The file that failed.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The file parsed or validated badly.
    #[error("class data {path}: {source}")]
    Parse {
        /// The file that failed.
        path: String,
        /// The parse or validation error.
        #[source]
        source: ParseError,
    },
    /// The file's extension names no supported format.
    #[error("class data {path} must end in .toml or .json")]
    UnknownFormat {
        /// The file that failed.
        path: String,
    },
}

/// One broken class rule.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// A balloon needs a floor below its ceiling.
    #[error("{class}: balloon min_mib ({min_mib}) must be below max_mib ({max_mib})")]
    BalloonOrder {
        /// The class.
        class: &'static str,
        /// The floor.
        min_mib: u64,
        /// The ceiling.
        max_mib: u64,
    },
    /// A fixed claim must be non-zero.
    #[error("{class}: fixed memory must be greater than zero")]
    EmptyFixed {
        /// The class.
        class: &'static str,
    },
    /// A class whose memory is fixed was given another fixed size.
    #[error("{class}: memory must be {expected_mib} MiB fixed, not {got_mib} MiB")]
    WrongFixed {
        /// The class.
        class: &'static str,
        /// The size `STATUS.md` decides.
        expected_mib: u64,
        /// The size in the file.
        got_mib: u64,
    },
    /// A class whose memory is fixed was given a balloon.
    #[error("{class}: memory must be {expected_mib} MiB fixed")]
    MustBeFixed {
        /// The class.
        class: &'static str,
        /// The size `STATUS.md` decides.
        expected_mib: u64,
    },
    /// A class that must balloon was given a fixed claim.
    #[error("{class}: memory must balloon")]
    MustBalloon {
        /// The class.
        class: &'static str,
    },
    /// A class with an unknown size class.
    #[error("{class}: size class {mib} MiB is not one of 8192, 12288, 16384, 24576")]
    UnknownSizeClass {
        /// The class.
        class: &'static str,
        /// The offending size.
        mib: u64,
    },
    /// A balloon's ceiling is not its largest size class.
    #[error("{class}: max_mib ({max_mib}) must equal the largest size class ({largest})")]
    MaxNotLargestSizeClass {
        /// The class.
        class: &'static str,
        /// The ceiling in the file.
        max_mib: u64,
        /// The largest size class.
        largest: u64,
    },
    /// A class with the wrong persistence.
    #[error("{class}: persistence must be {expected}, not {got}")]
    WrongPersistence {
        /// The class.
        class: &'static str,
        /// The required value.
        expected: &'static str,
        /// The value in the file.
        got: &'static str,
    },
    /// A class with the wrong reset policy.
    #[error("{class}: reset must be {expected}, not {got}")]
    WrongReset {
        /// The class.
        class: &'static str,
        /// The required value.
        expected: &'static str,
        /// The value in the file.
        got: &'static str,
    },
    /// A class with the wrong device rule.
    #[error("{class}: devices must be {expected}, not {got}")]
    WrongDevices {
        /// The class.
        class: &'static str,
        /// The required value.
        expected: &'static str,
        /// The value in the file.
        got: &'static str,
    },
    /// A class with the wrong hibernation rule.
    #[error("{class}: hibernation must be {expected}, not {got}")]
    WrongHibernation {
        /// The class.
        class: &'static str,
        /// The required value.
        expected: &'static str,
        /// The value in the file.
        got: &'static str,
    },
    /// A class with the wrong exec rule.
    #[error("{class}: exec must be {expected}")]
    WrongExec {
        /// The class.
        class: &'static str,
        /// The required value.
        expected: bool,
    },
    /// A `ci` class without a balloon or free-page reporting has no way to
    /// give memory back between jobs.
    #[error("{class}: must balloon or report free pages")]
    NeedsBalloonOrFreePages {
        /// The class.
        class: &'static str,
    },
    /// A GPU handover would hibernate.
    #[error("{class}: a GPU handover never hibernates; handover_stop must be clean")]
    HandoverMustBeClean {
        /// The class.
        class: &'static str,
    },
    /// A timeout of zero would fail every start or stop at once.
    #[error("{class}: {field} must be greater than zero")]
    ZeroTimeout {
        /// The class.
        class: &'static str,
        /// The field.
        field: &'static str,
    },
}

fn class_name(class: InstanceClass) -> &'static str {
    match class {
        InstanceClass::Dev => "dev",
        InstanceClass::Desktop => "desktop",
        InstanceClass::Ci => "ci",
        InstanceClass::Office => "office",
    }
}

fn validate(class: InstanceClass, rules: &ClassRules) -> Result<(), ValidationError> {
    let name = class_name(class);
    match &rules.memory {
        MemoryRule::Fixed { mib } => {
            if *mib == 0 {
                return Err(ValidationError::EmptyFixed { class: name });
            }
        }
        MemoryRule::Balloon {
            min_mib,
            max_mib,
            size_classes_mib,
        } => {
            if min_mib >= max_mib {
                return Err(ValidationError::BalloonOrder {
                    class: name,
                    min_mib: *min_mib,
                    max_mib: *max_mib,
                });
            }
            for mib in size_classes_mib {
                if !DEV_SIZE_CLASSES_MIB.contains(mib) {
                    return Err(ValidationError::UnknownSizeClass {
                        class: name,
                        mib: *mib,
                    });
                }
            }
            if let Some(largest) = size_classes_mib.iter().copied().max()
                && largest != *max_mib
            {
                return Err(ValidationError::MaxNotLargestSizeClass {
                    class: name,
                    max_mib: *max_mib,
                    largest,
                });
            }
        }
    }
    if rules.handover_stop != StopMode::Clean {
        return Err(ValidationError::HandoverMustBeClean { class: name });
    }
    if rules.readiness_timeout_secs == 0 {
        return Err(ValidationError::ZeroTimeout {
            class: name,
            field: "readiness_timeout_secs",
        });
    }
    if rules.stop_timeout_secs == 0 {
        return Err(ValidationError::ZeroTimeout {
            class: name,
            field: "stop_timeout_secs",
        });
    }
    if class != InstanceClass::Ci && rules.exec {
        return Err(ValidationError::WrongExec {
            class: name,
            expected: false,
        });
    }
    match class {
        InstanceClass::Dev => {
            expect_balloon(name, rules)?;
            expect_persistence(name, rules, Persistence::Persistent)?;
            expect_reset(name, rules, ResetPolicy::Never)?;
            expect_devices(name, rules, DeviceRule::None)?;
        }
        InstanceClass::Desktop => {
            match &rules.memory {
                MemoryRule::Fixed { mib } if *mib == DESKTOP_MIB => {}
                MemoryRule::Fixed { mib } => {
                    return Err(ValidationError::WrongFixed {
                        class: name,
                        expected_mib: DESKTOP_MIB,
                        got_mib: *mib,
                    });
                }
                MemoryRule::Balloon { .. } => {
                    return Err(ValidationError::MustBeFixed {
                        class: name,
                        expected_mib: DESKTOP_MIB,
                    });
                }
            }
            expect_persistence(name, rules, Persistence::Persistent)?;
            expect_reset(name, rules, ResetPolicy::Never)?;
            expect_devices(name, rules, DeviceRule::Gpu)?;
        }
        InstanceClass::Ci => {
            expect_persistence(name, rules, Persistence::Ephemeral)?;
            expect_reset(name, rules, ResetPolicy::AfterEachJob)?;
            expect_devices(name, rules, DeviceRule::None)?;
            if !rules.memory.balloons() && !rules.free_page_reporting {
                return Err(ValidationError::NeedsBalloonOrFreePages { class: name });
            }
            if !rules.exec {
                return Err(ValidationError::WrongExec {
                    class: name,
                    expected: true,
                });
            }
        }
        InstanceClass::Office => {
            expect_balloon(name, rules)?;
            expect_persistence(name, rules, Persistence::Persistent)?;
            expect_reset(name, rules, ResetPolicy::Never)?;
            expect_devices(name, rules, DeviceRule::None)?;
            if rules.hibernation != Hibernation::WhenIdle {
                return Err(ValidationError::WrongHibernation {
                    class: name,
                    expected: Hibernation::WhenIdle.name(),
                    got: rules.hibernation.name(),
                });
            }
        }
    }
    Ok(())
}

fn expect_balloon(class: &'static str, rules: &ClassRules) -> Result<(), ValidationError> {
    if rules.memory.balloons() {
        Ok(())
    } else {
        Err(ValidationError::MustBalloon { class })
    }
}

fn expect_persistence(
    class: &'static str,
    rules: &ClassRules,
    expected: Persistence,
) -> Result<(), ValidationError> {
    if rules.persistence == expected {
        Ok(())
    } else {
        Err(ValidationError::WrongPersistence {
            class,
            expected: expected.name(),
            got: rules.persistence.name(),
        })
    }
}

fn expect_reset(
    class: &'static str,
    rules: &ClassRules,
    expected: ResetPolicy,
) -> Result<(), ValidationError> {
    if rules.reset == expected {
        Ok(())
    } else {
        Err(ValidationError::WrongReset {
            class,
            expected: expected.name(),
            got: rules.reset.name(),
        })
    }
}

fn expect_devices(
    class: &'static str,
    rules: &ClassRules,
    expected: DeviceRule,
) -> Result<(), ValidationError> {
    if rules.gpu == expected {
        Ok(())
    } else {
        Err(ValidationError::WrongDevices {
            class,
            expected: expected.name(),
            got: rules.gpu.name(),
        })
    }
}

fn house() -> ClassFile {
    ClassFile {
        dev: ClassRules {
            memory: MemoryRule::Balloon {
                min_mib: 1024,
                max_mib: 24 * 1024,
                size_classes_mib: DEV_SIZE_CLASSES_MIB.to_vec(),
            },
            persistence: Persistence::Persistent,
            reset: ResetPolicy::Never,
            gpu: DeviceRule::None,
            hibernation: Hibernation::Never,
            handover_stop: StopMode::Clean,
            exec: false,
            free_page_reporting: false,
            readiness_timeout_secs: 300,
            stop_timeout_secs: 120,
        },
        desktop: ClassRules {
            memory: MemoryRule::Fixed { mib: DESKTOP_MIB },
            persistence: Persistence::Persistent,
            reset: ResetPolicy::Never,
            gpu: DeviceRule::Gpu,
            hibernation: Hibernation::Never,
            handover_stop: StopMode::Clean,
            exec: false,
            free_page_reporting: false,
            readiness_timeout_secs: 300,
            stop_timeout_secs: 180,
        },
        ci: ClassRules {
            memory: MemoryRule::Balloon {
                min_mib: 2048,
                max_mib: 16 * 1024,
                size_classes_mib: Vec::new(),
            },
            persistence: Persistence::Ephemeral,
            reset: ResetPolicy::AfterEachJob,
            gpu: DeviceRule::None,
            hibernation: Hibernation::Never,
            handover_stop: StopMode::Clean,
            exec: true,
            free_page_reporting: true,
            readiness_timeout_secs: 600,
            stop_timeout_secs: 120,
        },
        office: ClassRules {
            memory: MemoryRule::Balloon {
                min_mib: 4096,
                max_mib: 24 * 1024,
                size_classes_mib: Vec::new(),
            },
            persistence: Persistence::Persistent,
            reset: ResetPolicy::Never,
            gpu: DeviceRule::None,
            hibernation: Hibernation::WhenIdle,
            handover_stop: StopMode::Clean,
            exec: false,
            free_page_reporting: true,
            readiness_timeout_secs: 300,
            stop_timeout_secs: 180,
        },
    }
}
