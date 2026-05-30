# condrun

**Conditional command runner.** Don't burn metered bandwidth on backups.

`condrun` gates command execution on live system-state predicates and kills the child process if those predicates flip mid-run. Wrap a `restic`/`rsync`/`borg`/`duplicacy` cron job with `condrun` and stop worrying about the 2am backup chewing through your phone tether.

## What it does

Cron is dumb. It fires at the scheduled time regardless of whether you're on home Wi-Fi, on a Personal Hotspot, in Low Data Mode on the train, or off-network entirely. A bandwidth-sensitive job like `restic backup` doesn't care — it'll happily push 4GB through your cellular connection and surprise you with an overage bill.

`condrun` sits between cron and your job. Before launching the child, it samples the current network state. If the predicates fail (e.g. you asked for "not metered" and you're tethered), it exits silently — cron sees a clean run, no log spam. If the predicates pass, it spawns the child and keeps polling. The instant a predicate flips (you tether mid-backup, or toggle Low Data Mode), it sends `SIGTERM`, waits for a configurable grace period, then `SIGKILL`s.

Use cases: `restic`, `rsync`, `borg`, `duplicacy`, `rclone`, or any cron task where bandwidth class matters more than punctuality.

## Install

```bash
cargo install condrun
```

Or build from a clone and install the binary directly:

```bash
git clone https://github.com/jordanbaker/condrun
cd condrun
make install                       # → ~/.local/bin/condrun (default)
make install PREFIX=/usr/local     # → /usr/local/bin/condrun (needs sudo)
make uninstall                     # remove
```

Pre-built binaries (when published) will be available from the [GitHub releases page](https://github.com/jordanbaker/condrun/releases). A Homebrew tap (`brew install jordanbaker/tap/condrun`) is planned.

`condrun` currently runs on macOS only — predicate detection uses Apple's `Network.framework` (`NWPathMonitor`), which has no Linux/Windows equivalent. Linux support via NetworkManager / `iwd` is on the roadmap.

## Quick start — restic

The motivating example. Drop this in your crontab:

```cron
0 2 * * * condrun run --reject-expensive --reject-low-data -- restic backup -r s3:bucket ~/work
```

At 2am every night:

1. `condrun` samples the current network path.
2. If the connection is **expensive** (cellular, Personal Hotspot) or **constrained** (Low Data Mode on), it exits 0 silently. Cron is happy. No partial upload, no overage.
3. Otherwise it spawns `restic backup`, polls every 30 seconds, and kills it if you tether mid-job.

## rsync example

```bash
condrun run --reject-expensive -- rsync -av ~/photos backup-host:photos/
```

Single predicate — only refuses to run on metered links, doesn't care about Low Data Mode.

## Predicate reference (v0.1.0)

| Flag | Behavior | macOS source |
|---|---|---|
| `--reject-expensive` | Pass iff current path is **not** marked expensive (cellular, Personal Hotspot) | [`NWPath.isExpensive`](https://developer.apple.com/documentation/network/nwpath/isexpensive) |
| `--reject-low-data` | Pass iff Low Data Mode is **off** | [`NWPath.isConstrained`](https://developer.apple.com/documentation/network/nwpath/isconstrained) |

SSID matching, AC-power gating, and interface-type predicates ship in v0.1.x. Track [SPEC.md](SPEC.md) §4 for the predicate roadmap.

## Lifecycle flags

| Flag | Default | Meaning |
|---|---|---|
| `--strict` | `false` | Exit `1` on pre-flight failure (default: silent exit `0`) |
| `--kill-on-change` | `true` | Kill the child if predicates flip after launch |
| `--no-kill-on-change` | — | Disable kill-on-change; child runs to completion regardless |
| `--grace 30s` | `30s` | SIGTERM → SIGKILL grace period |
| `--poll 30s` | `30s` | Watcher poll interval |
| `--debounce 0s` | `0s` | Predicate must stay failed this long before triggering kill (debounces flapping) |
| `--any` | `false` | Compose predicates with OR instead of AND |

Subcommands:

- `condrun run [predicates] -- <cmd> [args...]` — gate, spawn, supervise.
- `condrun check [predicates]` — evaluate predicates and exit; useful for shell scripts and debugging.

## Exit codes

Per [SPEC.md](SPEC.md) §6:

| Code | Meaning |
|---|---|
| `0` | Success, or pre-flight predicates failed in non-strict mode (silent skip) |
| `1` | Pre-flight predicates failed under `--strict` |
| `2` | Child exited non-zero (exit code preserved when possible) |
| `3` | Child killed by `condrun` because predicates flipped mid-run |
| `4` | CLI parse error, config error, or internal failure |

## How metered detection works

macOS exposes network-path metadata through [`NWPathMonitor`](https://developer.apple.com/documentation/network/nwpathmonitor) in the Network framework. `condrun` instantiates a monitor, polls the current `NWPath`, and reads two boolean flags:

- [`isExpensive`](https://developer.apple.com/documentation/network/nwpath/isexpensive) — true on cellular interfaces and on Personal Hotspot. This is the OS's own classification of "billable bandwidth".
- [`isConstrained`](https://developer.apple.com/documentation/network/nwpath/isconstrained) — true when the user has enabled Low Data Mode for the active interface (Wi-Fi or cellular).

Because these flags come from the OS, they correctly track Personal Hotspot even when the laptop sees the tether as plain Wi-Fi. There's no SSID heuristic to maintain, no per-carrier list, no guessing.

## Architecture

`condrun` is built around small trait-based seams so the supervisor logic can be tested without touching the OS:

- **`NetworkState`** — samples the current path. Real impl wraps `NWPathMonitor`; test impl is a static fixture.
- **`Predicate`** — pure function from `NetworkState` snapshot to pass/fail.
- **`Spawner` / `ChildHandle`** — abstracts `tokio::process::Command` so the supervisor's race logic can be exercised against a fake child.
- **`Signals`** — abstracts SIGINT/SIGTERM delivery.
- **Supervisor** — `tokio::select!` race between poll-tick, child completion, and external signals. Owns the kill-on-change state machine (debounce, SIGTERM, grace, SIGKILL).

v0.1.0 is polling-only (`--poll 30s`). v0.2 will add event-driven `NWPathMonitor` updates so predicate flips are detected without waiting for the next tick.

## Contributing

```bash
# Unit tests against the production binary (no test scaffolding):
cargo test

# Unit + integration tests (requires the test-fixture feature for the
# fake spawner / fake network state used by the integration harness):
cargo test --features test-fixture

# Integration tests only, run serially (the CLI test harness mutates env):
cargo test --features test-fixture --test cli -- --test-threads=1

# Opt-in smoke tests against the real macOS Network.framework. Skipped in CI
# because GitHub Actions runners don't expose realistic network paths;
# meant to be run on a developer machine before tagging a release:
cargo test --features platform-tests
```

Lints and formatting:

```bash
cargo fmt --all -- --check
cargo clippy --all-features --all-targets -- -D warnings
```

## License

MIT OR Apache-2.0 — pick whichever fits your project.
