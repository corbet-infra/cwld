//! What a guest that stopped by itself means for the card it holds.
//!
//! A desktop guest with the card stops for three reasons: a plan stopped it
//! (the return to the host), the owner shut it down from inside, or the owner
//! rebooted it from inside. A macOS desktop is defined with `on_reboot=destroy`
//! (the AppleSMC halt), so a reboot from inside also ends the domain; QEMU's
//! `SHUTDOWN` reason (`guest-reset`) is what tells the two apart. The owner
//! who reboots the Mac expects the Mac back, not the card handed to Linux:
//!
//! - a stop a plan made: nothing to do (the plan owns it);
//! - a reboot from inside, while the guest still owns the card: start the same
//!   instance again, keeping the card (no hand-back, no tenant restore);
//! - a shutdown from inside (or a stop with no known reason), while the guest
//!   owns the card: ask the conductor for the normal return to the host.
//!
//! This is pure bookkeeping over the events, so it is tested without a host.

use std::collections::{BTreeMap, BTreeSet};

use cdct::{PlanStep, SwitchPlan};

/// What to do about one stopped guest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestStopReaction {
    /// A plan stopped it, or it holds no card: nothing to do.
    Ignore,
    /// It rebooted from inside: start it again, the card stays with it.
    Restart,
    /// It was shut down from inside: the card returns to the host.
    ReturnToHost,
}

/// The stops plans made and the reboots QEMU reported, per instance.
#[derive(Debug, Default)]
pub struct GuestStops {
    expected: BTreeSet<String>,
    /// QEMU's last shutdown reason per domain: `true` for a reset.
    reasons: BTreeMap<String, bool>,
}

impl GuestStops {
    /// Record the instances a plan stopped (its executed `StopInstance` steps),
    /// so their stop events are not taken for a stop from inside.
    pub fn plan_stopped(&mut self, executed: &[PlanStep]) {
        for step in executed {
            if let PlanStep::StopInstance { instance, .. } = step {
                self.expected.insert(instance.as_str().to_owned());
            }
        }
    }

    /// Record QEMU's shutdown reason for a domain: a reset is a reboot from
    /// inside, anything else is not.
    pub fn shutdown(&mut self, domain: &str, reset: bool) {
        self.reasons.insert(domain.to_owned(), reset);
    }

    /// Whether QEMU's reason for this domain's stop is known yet. The reason
    /// and the stop come from two libvirt event clients, so a stop can be read
    /// first; the caller then waits briefly for the reason.
    #[must_use]
    pub fn knows_reason(&self, domain: &str) -> bool {
        self.reasons.contains_key(domain)
    }

    /// A domain stopped. `owns_card` says whether the conductor names it the
    /// card's owner right now.
    pub fn stopped(&mut self, domain: &str, owns_card: bool) -> GuestStopReaction {
        let expected = self.expected.remove(domain);
        let rebooting = self.reasons.remove(domain).unwrap_or(false);
        if expected || !owns_card {
            GuestStopReaction::Ignore
        } else if rebooting {
            GuestStopReaction::Restart
        } else {
            GuestStopReaction::ReturnToHost
        }
    }
}

/// The plan that starts a rebooted guest again; it holds the card already.
#[must_use]
pub fn restart_plan(plan_target: cdrv::KernelOwner, instance: cdrv::InstanceId) -> SwitchPlan {
    SwitchPlan {
        target: plan_target,
        memory: None,
        displaced: Vec::new(),
        steps: vec![PlanStep::StartInstance { instance }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdrv::{InstanceId, StopMode};

    fn stop_step(id: &str) -> PlanStep {
        PlanStep::StopInstance {
            instance: InstanceId::from(id),
            mode: StopMode::Clean,
        }
    }

    #[test]
    fn a_reboot_from_inside_restarts_the_guest_and_keeps_the_card() {
        let mut stops = GuestStops::default();
        stops.shutdown("mac", true);
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::Restart);
        // The reboot is consumed: the next stop is judged afresh.
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::ReturnToHost);
    }

    #[test]
    fn a_shutdown_from_inside_returns_the_card() {
        let mut stops = GuestStops::default();
        stops.shutdown("mac", false);
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::ReturnToHost);
        // With no reason seen at all the card returns too.
        assert_eq!(stops.stopped("win", true), GuestStopReaction::ReturnToHost);
    }

    #[test]
    fn a_stop_a_plan_made_is_left_to_the_plan() {
        let mut stops = GuestStops::default();
        stops.plan_stopped(&[stop_step("mac")]);
        stops.shutdown("mac", false);
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::Ignore);
        // Even a reboot reason does not override a planned stop.
        stops.plan_stopped(&[stop_step("mac")]);
        stops.shutdown("mac", true);
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::Ignore);
    }

    #[test]
    fn a_guest_that_holds_no_card_is_left_alone() {
        let mut stops = GuestStops::default();
        stops.shutdown("dev", true);
        assert_eq!(stops.stopped("dev", false), GuestStopReaction::Ignore);
    }

    #[test]
    fn the_reason_is_known_until_the_stop_consumes_it() {
        let mut stops = GuestStops::default();
        assert!(!stops.knows_reason("mac"));
        stops.shutdown("mac", true);
        assert!(stops.knows_reason("mac"));
        let _ = stops.stopped("mac", true);
        assert!(!stops.knows_reason("mac"));
    }

    #[test]
    fn a_later_power_off_reason_clears_an_earlier_reset() {
        let mut stops = GuestStops::default();
        stops.shutdown("mac", true);
        stops.shutdown("mac", false);
        assert_eq!(stops.stopped("mac", true), GuestStopReaction::ReturnToHost);
    }
}
