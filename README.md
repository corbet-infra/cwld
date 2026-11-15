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
  `Stopped`, `Failed`, with the transitions of `CONTRACTS.md` section 6. A
  stopped persistent instance (`desktop`, `dev`, `office`) starts again; only an
  ephemeral class is reset to `Defined` first. An illegal transition is a typed
  error. `cwld` never destroys on its own, and a stop timeout ends in `Failed`
  with an event that says a human is needed. Waits never poll: `cwld` reads the
  instance's state once, then follows the lifecycle events the driver pushes,
  against a deadline on the contract's one clock.
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
would receive. A handover names the card as the hardware inventory does; the
dry run resolves that name through its logging fake host to one synthetic PCI
function (`0000:01:00.0`), which is printed, never used, and is not a host
fact. Without `--dry-run` the command fails: no OS driver crate is linked in
this iteration. Exit status: `0` complete, `2` a step failed, `1` error.

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

### One `cdrv`

`Cargo.toml` names `cdrv` and `cdct` by their git URLs, so `cwld` builds from
the URLs alone. To develop against sibling checkouts, pass patches on the
command line instead of committing them, for example
`cargo --config 'patch."https://git.corbet.ch/corbet-infra/cdrv.git".cdrv.path="../cdrv"' test`.
The URLs must carry the commits the code is written against.

## License

`LGPL-3.0-only WITH LGPL-3.0-linking-exception`; see `LICENSE`.

## `cwld ci`: decisions of the review (2026-11-15)

| Question | Decision | Why |
|---|---|---|
| HTTP client | keep `curl` (config on stdin) | an in-process rustls client needs `rustls-webpki` and `untrusted` (ISC), `ring` (Apache-2.0 AND ISC) and `subtle` (BSD-3-Clause); none is on the `deny.toml` allowlist (MIT, Apache-2.0, Unicode-3.0, and MPL-2.0 for dependencies since 2026-11-14), and MPL-2.0 does not help. Adding ISC and BSD-3-Clause is the owner's licence decision. curl also honours the host trust store (`SSL_CERT_FILE`). The cost is one TLS handshake per request; the listings run eight at a time, as in today's dispatcher |
| systemd | keep `systemd-run --pipe --wait` behind `UnitRunner` | the whole point of the call is the unit's output stream (the cancel line is an output line). `StartTransientUnit` over zbus returns a job, not the stream: it needs fd passing or the journal, plus an async runtime and `zvariant`. That is not a contained change; the trait keeps the swap to one file |
| Account | the dispatcher is an unprivileged account | the deployment (a separate repository) adds a polkit rule that lets exactly that account manage `cwld-ci-*` units. Start is `systemd-run --system`, kill is `systemctl kill --kill-whom=all --signal=SIGKILL`, and the runner-owned job tree is the job unit's own `StateDirectory=` removed by its `ExecStopPost=cwld ci scrub` as `run_as`; the dispatcher owns only the configuration and credential file of a job. Note: an account that may start arbitrary named transient units can still choose their properties, so the rule narrows names, not power |
| Job environment | `environment` per pool, `environment` per slot (sccache port) | the transient unit inherits nothing from the dispatcher; `{credentials}` is the job unit's own credentials directory |
