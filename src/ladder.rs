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
//! consecutive observations it has lasted. The desktop's GTT spill is the
//! exception: clnx's card sampler reads the counters on a clock (the retired
//! pressure-watcher's 6 s), counts the consecutive spilling readings itself and
//! wakes the observer while the full card spills; the latest reading is the
//! spill signal of every observation.
//!
//! The conductor may also ask for the next observation at a deadline (the end
//! of a cooldown while a shed tenant waits for its restore): the observer then
//! observes at that instant even when no event comes. A deadline, not a poll.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cdct::{
    Client, Endpoint, GpuTenant, LadderDecision, PressureAnswer, PressureObservation,
    RestoreDecision, StarvationSignal, TenantState, VramFacts,
};
use cdrv::Timestamp;
use clnx::pressure::{CardSample, PressureConfig, PressureFacts, Wake};
use clnx::{KubeClient, LinuxSysfs};

/// Consecutive-observation counters, one per signal, and the card sampler's
/// latest reading.
#[derive(Debug, Default)]
pub struct Counters {
    starved: BTreeMap<String, u32>,
    card: Option<CardSample>,
    /// The sampler's spill count at the last shed: a spill that goes on after
    /// a shed must earn its grace again (the watcher reset all its counters).
    spill_base: u32,
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

    /// After a shed every starvation signal earns its grace again, as the
    /// watcher reset its counters after a kill: the freed VRAM lands during
    /// the cooldown, and only a starvation that outlasts it sheds the next
    /// tenant.
    pub fn regrace(&mut self) {
        self.starved.clear();
        self.spill_base = self.card.map_or(0, |sample| sample.spilling);
    }

    /// Take what woke the observer: a card reading replaces the previous one.
    pub fn wake(&mut self, wake: Wake) {
        if let Wake::Card(sample) = wake {
            if sample.spilling == 0 {
                self.spill_base = 0;
            }
            self.card = Some(sample);
        }
    }

    /// Turn one set of facts into the conductor's observation, advancing the
    /// counters. A tenant asking for replicas it does not have ready is
    /// starved; the broker's stuck models and the card sampler's spill are the
    /// other two signals.
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
        // The desktop's GTT spill: the sampler's latest reading, its own count,
        // counted from the last shed.
        if let Some(sample) = self.card.filter(|sample| sample.spilling > self.spill_base) {
            signals.push(StarvationSignal::DesktopGttSpill {
                gtt_used_bytes: sample.memory.gtt_used,
                gtt_previous_bytes: sample.gtt_previous,
                starved_ticks: sample.spilling - self.spill_base,
            });
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
    let mut deadline: Option<Instant> = None;
    loop {
        // An event, or the conductor's deadline: either one is an observation.
        let wake = match deadline {
            Some(at) => match receiver.recv_timeout(at.saturating_duration_since(Instant::now())) {
                Ok(wake) => Some(wake),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            },
            None => match receiver.recv() {
                Ok(wake) => Some(wake),
                Err(_) => return,
            },
        };
        deadline = None;
        // One observation per burst: every wake-up already queued is covered by it.
        if let Some(wake) = wake {
            counters.wake(wake);
        }
        while let Ok(wake) = receiver.try_recv() {
            counters.wake(wake);
        }
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
            Ok(answer) => {
                report(&answer);
                if matches!(
                    answer.ladder,
                    LadderDecision::ScaleLowestPriorityToZero { .. }
                ) {
                    counters.regrace();
                }
                deadline = answer
                    .wake_at
                    .map(|at| Instant::now() + until(at, now_millis()));
            }
            Err(error) => {
                eprintln!("cwld ladder: the conductor did not take the observation: {error}");
                client = None;
            }
        }
    }
}

/// The wait until the conductor's deadline `at` from `now` (both in
/// milliseconds since the epoch), a little past it so the conductor's
/// `now < deadline` test has turned; a deadline already gone is at once.
fn until(at: Timestamp, now: u64) -> Duration {
    Duration::from_millis(
        at.as_millis()
            .saturating_sub(now)
            .saturating_add(DEADLINE_SLACK_MS),
    )
}

/// How far past the conductor's deadline the observer looks again.
const DEADLINE_SLACK_MS: u64 = 100;

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

fn report(answer: &PressureAnswer) {
    let (ladder, restore, restart_plugin) = (
        &answer.ladder,
        answer.restore.as_ref(),
        answer.restart_plugin,
    );
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
mod deadline_tests {
    use super::*;

    #[test]
    fn the_wait_ends_just_past_the_conductors_deadline() {
        assert_eq!(
            until(Timestamp::from_millis(10_000), 4_000),
            Duration::from_millis(6_000 + DEADLINE_SLACK_MS)
        );
        assert_eq!(
            until(Timestamp::from_millis(4_000), 10_000),
            Duration::from_millis(DEADLINE_SLACK_MS),
            "a deadline already gone is observed at once"
        );
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

    fn spill(gtt_gib: u64, previous_gib: u64, spilling: u32) -> Wake {
        Wake::Card(CardSample {
            memory: CardMemory {
                vram_used: 15 * GIB,
                vram_total: 16 * GIB,
                gtt_used: gtt_gib * GIB,
            },
            gtt_previous: previous_gib * GIB,
            spilling,
        })
    }

    #[test]
    fn the_samplers_spill_and_stuck_broker_models_are_signals() {
        let mut counters = Counters::default();
        let quiet = counters.observation(&facts(1, 1, 0));
        assert!(quiet.signals.is_empty(), "no reading, no spill");
        counters.wake(spill(3, 1, 1));
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
        counters.wake(Wake::Kubernetes);
        let still = counters.observation(&facts(1, 3, 0));
        assert_eq!(
            still.signals,
            vec![StarvationSignal::DesktopGttSpill {
                gtt_used_bytes: 3 * GIB,
                gtt_previous_bytes: GIB,
                starved_ticks: 1,
            }],
            "another wake-up keeps the sampler's latest reading"
        );
        counters.wake(spill(3, 3, 0));
        let ended = counters.observation(&facts(1, 3, 0));
        assert!(ended.signals.is_empty(), "the sampler ended the spill");
        assert_eq!(
            ended.vram,
            Some(VramFacts {
                used_bytes: 15 * GIB,
                total_bytes: 16 * GIB,
            })
        );
    }

    #[test]
    fn after_a_shed_every_signal_earns_its_grace_again() {
        let mut counters = Counters::default();
        let _ = counters.observation(&facts(0, 1, 0));
        counters.wake(spill(3, 2, 2));
        let before = counters.observation(&facts(0, 1, 0));
        assert!(before.signals.iter().any(|signal| matches!(
            signal,
            StarvationSignal::PodNotReady {
                starved_ticks: 2,
                ..
            }
        )));
        assert!(before.signals.iter().any(|signal| matches!(
            signal,
            StarvationSignal::DesktopGttSpill {
                starved_ticks: 2,
                ..
            }
        )));

        counters.regrace();
        assert!(
            counters.observation(&facts(1, 1, 0)).signals.is_empty(),
            "right after the shed the running spill counts nothing yet"
        );
        counters.wake(spill(4, 3, 3));
        let again = counters.observation(&facts(0, 1, 0));
        assert!(again.signals.iter().any(|signal| matches!(
            signal,
            StarvationSignal::PodNotReady {
                starved_ticks: 1,
                ..
            }
        )));
        assert!(
            again.signals.iter().any(|signal| matches!(
                signal,
                StarvationSignal::DesktopGttSpill {
                    starved_ticks: 1,
                    ..
                }
            )),
            "one more spilling reading after the shed: {:?}",
            again.signals
        );
        counters.wake(spill(4, 4, 0));
        counters.wake(spill(5, 4, 1));
        assert!(
            counters
                .observation(&facts(1, 1, 0))
                .signals
                .iter()
                .any(|signal| matches!(
                    signal,
                    StarvationSignal::DesktopGttSpill {
                        starved_ticks: 1,
                        ..
                    }
                )),
            "a new spill after the old one ended counts from 1 again"
        );
    }

    #[test]
    fn a_spill_over_two_readings_sheds_the_lowest_tenant_through_the_conductors_ladder() {
        let mut counters = Counters::default();
        let running = |counters: &mut Counters| {
            let mut facts = facts(1, 2, 0);
            facts.tenants[1].replicas = 1;
            facts.tenants[1].ready = 1;
            counters.observation(&facts)
        };
        let decide = |observation: &PressureObservation| {
            cdct::gpu::decide_ladder(
                &cdct::gpu::LadderInput {
                    owner: cdrv::KernelOwner::Host,
                    vram_full: true,
                    now: cdrv::Timestamp::from_millis(10_000),
                    cooldown_until: cdrv::Timestamp::from_millis(0),
                    tenants: observation.tenants.clone(),
                    signals: observation.signals.clone(),
                },
                &cdct::policy::GpuPolicy::default(),
            )
        };
        counters.wake(spill(1, 0, 1));
        assert!(
            matches!(decide(&running(&mut counters)), LadderDecision::None { .. }),
            "one spilling reading is inside the grace"
        );
        counters.wake(spill(2, 1, 2));
        assert_eq!(
            decide(&running(&mut counters)),
            LadderDecision::ScaleLowestPriorityToZero {
                tenant: "render/farm".to_owned(),
                ladder: GpuLadder::BestEffort,
                signal: StarvationSignal::DesktopGttSpill {
                    gtt_used_bytes: 2 * GIB,
                    gtt_previous_bytes: GIB,
                    starved_ticks: 2,
                },
            },
            "the desktop outranks every rung: the lowest running tenant leaves"
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
