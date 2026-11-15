# cwld — the workload runtime

`cwld` is the VM/GPU system's runtime: it executes what the conductor (`cdct`)
decided. It runs a switch plan step by step, drives one lifecycle state machine
per instance, applies the class rules, and remembers enough across a crash to
report what was left half done.

## Purpose

- **Classes as validated data.** `dev`, `desktop`, `ci` and `office` are exactly
  the classes `STATUS.md` decides: `dev` balloons with 8/12/16/24 GiB size
  classes, `desktop` is a fixed 24 GiB with the GPU, `ci` is an ephemeral clone
  reset after one job, `office` balloons and hibernates only when idle. The rules
  load from TOML or JSON and are validated at load; invalid data is an error
  before anything runs.
- **One lifecycle state machine.** `Defined`, `Starting`, `Running`, `Stopping`,
  `Stopped`, `Failed`, with exactly the transitions of `CONTRACTS.md` section 6.
  An illegal transition is a typed error. `cwld` never destroys on its own, and
  a stop timeout ends in `Failed` with an event that says a human is needed.
  Waits never poll: `cwld` reads the instance's state once, then follows the
  lifecycle events the driver pushes, against a deadline on the contract's one
  clock.
- **Plan executor.** A `cdct::SwitchPlan` is data. `cwld` executes it in order,
  stops at the first failed step, emits the event and returns a typed report of
  what ran, what failed and what was never reached. A guest switch that finishes
  records the plan's `displaced` tenants in `cdct`'s holder table, the hand-off
  `cdct` documents.
- **Crash recovery, no guessing.** Every instance has a dirty mark, written
  before a start or a stop and cleared after a clean stop. `ensure_clean` resets
  an ephemeral instance before its next start; a persistent disk is reported,
  never reset automatically. A plan journal records each step before it is
  attempted, so a half-run plan is reported as completed steps, one uncertain
  step and the steps not started.
- **The CI class.** One job is: ensure clean → start → run the job through the
  guest agent's `exec` → stop → reset. The Forgejo runner itself runs inside the
  instance and is out of scope here.

## Boundaries — this crate must never

- decide anything. Which instance starts, who gets the card, whether memory is
  admitted and what a switch does are `cdct`'s decisions; `cwld` executes them.
- run `virsh`, `zfs`, `systemctl`, `kubectl` or any other host tool, or read
  sysfs. Every side effect goes through a trait: the `cdrv` host and guest
  traits, plus the one port in `src/ports.rs` for what `cdrv` does not define
  yet, the disk reset that the storage component (`cdsk`) will own.
- destroy an instance on its own. There is no destroy path in the lifecycle;
  a failed stop stops the plan and needs a human.
- hold host data (PCI addresses, serials, hostnames, IPs, VM names), macOS
  material or Windows material. Examples and tests are synthetic.

## CLI

```sh
# Execute a switch plan against a logging fake driver; changes nothing.
cwld run-plan --plan plan.json --dry-run
```

`plan.json` is a `cdct::SwitchPlan`, or the JSON that `cdct plan switch --json`
prints (the plan is taken from it). The dry run prints every call the driver
would receive. `--gpu <domain:bus:slot.function>` names the card; the default
`0000:01:00.0` is a synthetic placeholder, never a host fact. Without
`--dry-run` the command fails: no OS driver crate is linked in this iteration.
Exit status: `0` complete, `2` a step failed, `1` error.

## How to run the tests

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`tests/it/main.rs` is the readable specification, with a fake `cdrv` driver that
pushes lifecycle events and a fake clock: a full Mac switch and back to the
host, a Windows switch that stops the office profile first, a failing step in
the middle, a start and a stop timeout (`Failed`, never a destroy), a start
driven only by the pushed events (the state is read once, never in a loop), a
driver failure event, an exhausted stream, the deadline on the fake clock, the
CI job cycle including reset, crash recovery of a dirty ephemeral instance,
half-run plan reporting, and class data validation.

`Cargo.toml` patches `cdct`'s git-sourced `cdrv` to the local path so the two
crates share one contract version; the patch goes away when `cwld` follows the
published crates.

## License

`LGPL-3.0-only WITH LGPL-3.0-linking-exception`; see `LICENSE`.
