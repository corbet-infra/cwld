//! The live ladder's observer on a Linux host.
//!
//! `cwld` observes, `cdct` decides, `cwld` executes. This module is the
//! observation half: [`clnx::pressure::watch`] wakes it at every event (a GPU
//! pod or the node changed, the model broker reported a model status change),
//! it reads the facts once per burst of wake-ups, turns them into the
//! conductor's [`PressureObservation`] and reports it. A shed or a restore the
//! conductor decides arrives on the follow stream as a granted plan and is
//! executed there like any switch. Nothing here runs on a timer.
//!
//! Grace is counted in observations: a starvation signal carries how many
//! consecutive observations it has lasted. The desktop's GTT spill has no
//! event source of its own; it is judged only at the events above.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use cdct::{
    Client, Endpoint, GpuTenant, LadderDecision, PressureObservation, RestoreDecision,
    StarvationSignal, TenantState, VramFacts,
};
use clnx::pressure::{PressureConfig, PressureFacts, Wake};
use clnx::{KubeClient, LinuxSysfs};

/// Consecutive-observation counters, one per signal.
#[derive(Debug, Default)]
pub struct Counters {
    starved: BTreeMap<String, u32>,
    gtt_previous: Option<u64>,
}

impl Counters {
    fn bump(&mut self, key: &str, active: bool) -> u32 {
        if active {
            let count = self.starved.entry(key.to_owned()).or_insert(0);
            *count = count.saturating_add(1);
            *count
        } else {
            self.starved.remove(key);
            0
        }
    }

    /// Turn one set of facts into the conductor's observation, advancing the
    /// counters. A tenant asking for replicas it does not have ready is
    /// starved; the broker's stuck models and a rising GTT are the other two
    /// signals.
    pub fn observation(&mut self, facts: &PressureFacts) -> PressureObservation {
        let mut tenants = Vec::new();
        let mut signals = Vec::new();
        for tenant in &facts.tenants {
            let state = if tenant.replicas == 0 {
                TenantState::Stopped
            } else if tenant.ready >= tenant.replicas {
                TenantState::Running
            } else {
                TenantState::NotReady
            };
            let ticks = self.bump(
                &format!("pod:{}", tenant.id),
                state == TenantState::NotReady,
            );
            if ticks > 0 {
                signals.push(StarvationSignal::PodNotReady {
                    tenant: tenant.id.clone(),
                    ladder: tenant.ladder,
                    starved_ticks: ticks,
                });
            }
            tenants.push(GpuTenant {
                id: tenant.id.clone(),
                ladder: tenant.ladder,
                state,
            });
        }
        let stuck = facts.broker_stuck.unwrap_or(0);
        let ticks = self.bump("broker", stuck > 0);
        if ticks > 0 {
            signals.push(StarvationSignal::BrokerModelStarved {
                stuck_models: stuck,
                starved_ticks: ticks,
            });
        }
        // A guarded resource that reads zero while the device plugin runs: a lost
        // registration, cured by a plugin restart (the conductor's guard).
        let zero: Vec<String> = facts
            .zero_resources
            .iter()
            .map(|resource| format!("zombie:{resource}"))
            .collect();
        self.starved
            .retain(|key, _| !key.starts_with("zombie:") || zero.contains(key));
        for resource in &facts.zero_resources {
            let ticks = self.bump(&format!("zombie:{resource}"), true);
            signals.push(StarvationSignal::RegistrationZombie {
                resource: resource.clone(),
                starved_ticks: ticks,
            });
        }
        if let Some(memory) = facts.memory {
            let previous = self.gtt_previous.replace(memory.gtt_used);
            let rising = previous.is_some_and(|previous| memory.gtt_used > previous);
            let ticks = self.bump("gtt", rising);
            if let (true, Some(previous)) = (ticks > 0, previous) {
                signals.push(StarvationSignal::DesktopGttSpill {
                    gtt_used_bytes: memory.gtt_used,
                    gtt_previous_bytes: previous,
                    starved_ticks: ticks,
                });
            }
        } else {
            self.gtt_previous = None;
            self.bump("gtt", false);
        }
        PressureObservation {
            vram: facts.memory.map(|memory| VramFacts {
                used_bytes: memory.vram_used,
                total_bytes: memory.vram_total,
            }),
            tenants,
            signals,
        }
    }
}

/// Start the observer: event sources plus one thread that observes and
/// reports. Returns at once; the thread lives as long as the process.
///
/// # Errors
///
/// The event sources or the Kubernetes client cannot be started.
pub fn start(config: PressureConfig, conductor: PathBuf) -> Result<(), String> {
    let (sender, receiver) = mpsc::channel::<Wake>();
    clnx::pressure::watch(&config, &sender).map_err(|error| error.to_string())?;
    let kube = KubeClient::connect().map_err(|error| error.to_string())?;
    thread::Builder::new()
        .name("cwld-ladder".to_owned())
        .spawn(move || observe_loop(&config, &conductor, &kube, &receiver))
        .map_err(|error| format!("cannot start the ladder observer: {error}"))?;
    // The watch threads keep their own clones of the sender.
    drop(sender);
    Ok(())
}

fn observe_loop(
    config: &PressureConfig,
    conductor: &Path,
    kube: &KubeClient,
    receiver: &mpsc::Receiver<Wake>,
) {
    let sysfs = LinuxSysfs::system();
    let mut counters = Counters::default();
    let mut client: Option<Client> = None;
    while receiver.recv().is_ok() {
        // One observation per burst: every wake-up already queued is covered by it.
        while receiver.try_recv().is_ok() {}
        let facts = match clnx::pressure::observe(config, &sysfs, kube) {
            Ok(facts) => facts,
            Err(error) => {
                eprintln!("cwld ladder: cannot read the pressure facts: {error}");
                continue;
            }
        };
        let observation = counters.observation(&facts);
        if client.is_none() {
            client = Client::connect(&Endpoint::LocalSocket(conductor.to_path_buf())).ok();
        }
        let Some(connected) = client.as_mut() else {
            eprintln!("cwld ladder: the conductor is not reachable; the observation is dropped");
            continue;
        };
        match connected.pressure(&observation) {
            Ok((ladder, restore, restart_plugin)) => {
                report(&ladder, restore.as_ref(), restart_plugin);
            }
            Err(error) => {
                eprintln!("cwld ladder: the conductor did not take the observation: {error}");
                client = None;
            }
        }
    }
}

fn report(ladder: &LadderDecision, restore: Option<&RestoreDecision>, restart_plugin: bool) {
    if restart_plugin {
        println!("cwld ladder: the conductor restarts the GPU device plugin (lost registration)");
    }
    if let LadderDecision::ScaleLowestPriorityToZero { tenant, signal, .. } = ladder {
        println!("cwld ladder: the conductor sheds {tenant} ({signal:?})");
    }
    if let Some(RestoreDecision::Restore {
        tenant, replicas, ..
    }) = restore
    {
        println!("cwld ladder: the conductor restores {tenant} with {replicas} replica(s)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdrv::GpuLadder;
    use clnx::pressure::{CardMemory, TenantFacts};

    const GIB: u64 = 1 << 30;

    fn facts(broker_ready: u32, gtt_gib: u64, stuck: u32) -> PressureFacts {
        PressureFacts {
            memory: Some(CardMemory {
                vram_used: 15 * GIB,
                vram_total: 16 * GIB,
                gtt_used: gtt_gib * GIB,
            }),
            tenants: vec![
                TenantFacts {
                    id: "llm/broker".to_owned(),
                    ladder: GpuLadder::Interactive,
                    replicas: 1,
                    ready: broker_ready,
                },
                TenantFacts {
                    id: "render/farm".to_owned(),
                    ladder: GpuLadder::BestEffort,
                    replicas: 0,
                    ready: 0,
                },
            ],
            broker_stuck: Some(stuck),
            zero_resources: Vec::new(),
        }
    }

    #[test]
    fn a_not_ready_tenant_counts_consecutive_observations_and_resets_when_ready() {
        let mut counters = Counters::default();
        let first = counters.observation(&facts(0, 1, 0));
        assert_eq!(
            first.signals,
            vec![StarvationSignal::PodNotReady {
                tenant: "llm/broker".to_owned(),
                ladder: GpuLadder::Interactive,
                starved_ticks: 1,
            }]
        );
        assert_eq!(first.tenants[0].state, TenantState::NotReady);
        assert_eq!(first.tenants[1].state, TenantState::Stopped);
        let second = counters.observation(&facts(0, 1, 0));
        assert!(matches!(
            second.signals[0],
            StarvationSignal::PodNotReady {
                starved_ticks: 2,
                ..
            }
        ));
        let ready = counters.observation(&facts(1, 1, 0));
        assert!(ready.signals.is_empty());
        assert_eq!(ready.tenants[0].state, TenantState::Running);
        let again = counters.observation(&facts(0, 1, 0));
        assert!(matches!(
            again.signals[0],
            StarvationSignal::PodNotReady {
                starved_ticks: 1,
                ..
            }
        ));
    }

    #[test]
    fn a_rising_gtt_and_stuck_broker_models_are_signals() {
        let mut counters = Counters::default();
        let _ = counters.observation(&facts(1, 1, 0));
        let rising = counters.observation(&facts(1, 3, 2));
        assert!(
            rising
                .signals
                .contains(&StarvationSignal::BrokerModelStarved {
                    stuck_models: 2,
                    starved_ticks: 1,
                })
        );
        assert!(rising.signals.contains(&StarvationSignal::DesktopGttSpill {
            gtt_used_bytes: 3 * GIB,
            gtt_previous_bytes: GIB,
            starved_ticks: 1,
        }));
        let flat = counters.observation(&facts(1, 3, 0));
        assert!(flat.signals.is_empty());
        assert_eq!(
            flat.vram,
            Some(VramFacts {
                used_bytes: 15 * GIB,
                total_bytes: 16 * GIB,
            })
        );
    }
}

#[cfg(test)]
mod guard_tests {
    use super::*;
    use clnx::pressure::PressureFacts;

    #[test]
    fn a_zero_guarded_resource_counts_until_it_reads_again() {
        let mut counters = Counters::default();
        let zero = PressureFacts {
            memory: None,
            tenants: Vec::new(),
            broker_stuck: None,
            zero_resources: vec!["example.com/slot".to_owned()],
        };
        let _ = counters.observation(&zero);
        let second = counters.observation(&zero);
        assert_eq!(
            second.signals,
            vec![StarvationSignal::RegistrationZombie {
                resource: "example.com/slot".to_owned(),
                starved_ticks: 2,
            }]
        );
        let healthy = PressureFacts {
            zero_resources: Vec::new(),
            ..zero.clone()
        };
        assert!(counters.observation(&healthy).signals.is_empty());
        assert!(matches!(
            counters.observation(&zero).signals[0],
            StarvationSignal::RegistrationZombie {
                starved_ticks: 1,
                ..
            }
        ));
    }
}
