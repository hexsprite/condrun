# condrun — Specification

**Version:** 0.1.0-draft
**Status:** Pre-implementation
**Last updated:** 2026-05-04

## 1. Purpose

`condrun` is a generic command runner that **gates execution on system state predicates** and **kills the child process if predicates flip during execution**. It fills a gap in cross-platform tooling: there is no equivalent of systemd's `ExecCondition=` plus NetworkManager-dispatcher event-driven kill behavior on macOS, and no unified abstraction for either OS.

### 1.1 Problem it solves

- Cron / launchd / systemd timers fire blindly. They run the command regardless of network state, power state, or other contextual signals.
- Backup tools (vykar, restic, borg, duplicacy, rsync) burn metered bandwidth on hotspots, run on battery, or trigger over expensive cellular tethering.
- Existing escape hatches are ad-hoc: per-tool wrapper scripts, hand-rolled SSID checks, hardcoded Bash predicates. None compose, none are reusable, none kill the child if the network changes mid-backup.

### 1.2 Non-goals

- **Not a scheduler.** `condrun` is invoked by cron / launchd / systemd / shell. It does not maintain its own timer.
- **Not a traffic shaper / firewall.** It does not block packets (use TripMode, Little Snitch, pf, iptables for that).
- **Not a notification system.** It logs predicate state changes; visualization belongs to other tools.
- **Not a multi-process orchestrator.** It supervises exactly one child command per invocation.

## 2. Concepts

### 2.1 Predicate

A boolean function over current system state. Evaluates synchronously against a `NetworkState` snapshot. Stable identifier (e.g. `require-ssid`, `reject-low-data`). Composable.

### 2.2 NetworkState

Read-only view of system state at a point in time:

- Current SSID (if any)
- Low-data-mode flag for current network
- Primary route interface (name + type: wifi / ethernet / cellular-tether / unknown)
- AC power state (plugged / battery)
- Captive portal state (reserved for v0.2)

### 2.3 Predicate Set

Composition of predicates with semantics:

- Default: **AND** — all predicates must pass.
- `--any`: **OR** — at least one predicate must pass.
- Result: `Pass` | `Fail(reason: String)`.

### 2.4 Supervisor

Lifecycle manager for one child command:

1. Pre-flight: evaluate predicate set against current state.
2. If fail and not `--strict`: exit 0 silently (cron-friendly).
3. If fail and `--strict`: log reason, exit 1.
4. If pass: spawn child command, attach to its stdout/stderr.
5. Watcher: subscribe to state-change events. On each event, re-evaluate predicate set.
6. If predicate flips to fail: send `SIGTERM`, wait `--grace` (default 30s), send `SIGKILL`. Exit 3.
7. If child exits naturally: propagate its exit code (mapped per §6).

### 2.5 Profile

Named bundle of predicates stored in YAML config. Lets cron lines stay short:

```yaml
# ~/.config/condrun/profiles.yaml
profiles:
  trusted-wifi:
    require_ssid: [HomeWifi, OfficeWifi]
    reject_ssid: [iPhone-Hotspot]
    reject_low_data: true
    require_ac_power: false
    kill_on_change: true
    grace: 30s
    poll: 30s
```

```bash
condrun run --profile trusted-wifi -- vykar backup -R r2
```

## 3. Subcommands

### 3.1 `condrun run [PREDICATES] -- CMD ARGS...`

The primary subcommand. Pre-flight, spawn, watch, propagate.

### 3.2 `condrun check [PREDICATES]`

Evaluate predicate set once and exit.
- Exit 0 = pass.
- Exit 1 = fail (always strict; intended for shell `if` use).
- `--explain`: print which predicate failed and why.

### 3.3 `condrun watch [PREDICATES]`

Long-running daemon mode. Prints predicate state changes to stdout as they occur. Useful for status bars, debugging, and ad-hoc observation.

- Output format: NDJSON (`{"timestamp": "...", "predicate": "require-ssid", "passed": false, "reason": "current SSID 'iPhone' not in allowlist"}`).
- `--format human` for terminal-friendly output.

### 3.4 `condrun list-predicates`

Print available predicates on this platform with descriptions and current evaluation. For discoverability and config writing.

### 3.5 `condrun explain [PREDICATES]`

One-shot diagnostic: print full state snapshot + each predicate result + composition result. For "why is my backup not running?" debugging.

## 4. Predicate Catalog (v0.1)

| Flag | Type | Description | Platform |
|---|---|---|---|
| `--require-ssid=A,B,C` | allowlist | Pass iff current SSID ∈ list | macOS, Linux |
| `--reject-ssid=A,B,C` | denylist | Pass iff current SSID ∉ list | macOS, Linux |
| `--reject-low-data` | bool | Pass iff current network not flagged Low Data Mode | macOS only (v0.1) |
| `--require-ac-power` | bool | Pass iff laptop on AC power | macOS, Linux |
| `--require-interface-type=wifi\|ethernet` | enum | Pass iff primary interface is given type | macOS, Linux |
| `--reject-tether` | bool | Pass iff primary interface is not a USB/Bluetooth/wifi tether | macOS, Linux |

### 4.1 Reserved for v0.2

- `--require-bandwidth=10mbps` — opt-in, slow (uses `networkQuality` on mac, equivalent on Linux).
- `--require-no-captive-portal` — captive portal detection.
- `--require-vpn` / `--reject-vpn` — VPN state.
- `--require-time-window=22:00-06:00` — local time gating.
- `--require-file-exists=/path` / `--require-cmd=cmd` — generic escape hatches.

## 5. CLI Surface

### 5.1 Global flags

```
--profile NAME              # load predicate set from config
--config PATH               # config file (default: ~/.config/condrun/profiles.yaml)
--strict                    # exit 1 on pre-flight fail (default: silent 0)
--kill-on-change            # default true; --no-kill-on-change to disable
--grace DURATION            # SIGTERM → SIGKILL grace (default: 30s)
--poll DURATION             # watcher cadence (default: 30s)
--any                       # OR composition (default: AND)
-v, --verbose               # increase logging
-q, --quiet                 # suppress logging
```

### 5.2 Predicate flags

See §4. All predicates may be specified multiple times or comma-separated.

### 5.3 Examples

```bash
# Vykar nightly, gated on home wifi + not low-data
condrun run \
  --require-ssid=HomeWifi,OfficeWifi \
  --reject-low-data \
  -- vykar backup -R r2

# Restic only on AC power
condrun run --require-ac-power -- restic backup ~/work

# Quick check in shell pipeline
if condrun check --require-ssid=HomeWifi; then
  rsync -av ~/photos backup-host:photos/
fi

# Debug "why didn't my backup run"
condrun explain --profile trusted-wifi
```

## 6. Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success: child ran and exited 0; OR pre-flight failed in non-strict mode |
| 1 | Pre-flight predicate failed (`--strict` mode) |
| 2 | Child exited non-zero (passes through child's code clamped to 2) |
| 3 | Child killed by predicate flip mid-run |
| 4 | CLI parse / config / internal error |
| 64-78 | Reserved for future sysexits-style errors |

Note: passing through child's exact exit code beyond 2 is considered, but conflicts with our reserved codes. v0.1 collapses non-zero child exits to 2 and logs the original.

## 7. Architecture

### 7.1 Trait seams

```rust
#[async_trait]
trait NetworkState: Send + Sync {
    async fn current_ssid(&self) -> Option<String>;
    async fn is_low_data_mode(&self) -> bool;
    async fn primary_interface(&self) -> Option<Interface>;
    async fn is_on_ac_power(&self) -> bool;
    fn changes(&self) -> tokio::sync::broadcast::Receiver<StateEvent>;
}

trait Predicate: Send + Sync {
    fn name(&self) -> &str;
    async fn evaluate(&self, state: &dyn NetworkState) -> PredicateResult;
}

#[async_trait]
trait Spawner: Send + Sync {
    async fn spawn(&self, cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>>;
}

#[async_trait]
trait ChildHandle: Send {
    async fn wait(&mut self) -> ExitStatus;
    async fn terminate(&mut self, grace: Duration) -> Result<()>;
    fn pid(&self) -> u32;
}

#[async_trait]
trait Clock: Send + Sync {
    async fn sleep(&self, d: Duration);
    fn now(&self) -> Instant;
}
```

### 7.2 Real impls

- `MacOsNetworkState` — shell-out to `wdutil info`, `networksetup`, `pmset`, `plutil` for the known-networks plist; emits `StateEvent` via `launchd` `WatchPaths` proxy or fallback polling.
- `LinuxNetworkState` — shell-out to `nmcli`, `on_ac_power`. Future: `zbus` to NetworkManager directly.
- `TokioSpawner` / `TokioChildHandle` — wraps `tokio::process::Command`. SIGTERM via `nix::sys::signal::kill`; falls back to `Child::kill()`.
- `RealClock` — `tokio::time::sleep`, `Instant::now`.

### 7.3 Test impls

- `FakeNetworkState` — interior mutability over the state fields; `set_ssid`, `set_low_data`, `set_ac_power`, `push_event` for tests to drive transitions.
- `FakeSpawner` — records spawn calls, returns `FakeChildHandle` whose exit code, exit timing, and signal-response behavior is scriptable per-test.
- `FakeChildHandle` — `wait` resolves when the test calls `set_exit`; `terminate` records the signal received and the time delta.
- Use `tokio::time::pause()` for clock control; no separate `FakeClock` needed.

### 7.4 Module layout

```
src/
├── lib.rs              # public API (for integration tests)
├── main.rs             # binary entrypoint
├── cli.rs              # clap definitions, dispatch
├── config.rs           # YAML profile loader
├── state/
│   ├── mod.rs          # NetworkState trait, Interface, StateEvent
│   ├── fake.rs         # FakeNetworkState
│   ├── macos.rs        # cfg(target_os = "macos")
│   └── linux.rs        # cfg(target_os = "linux")
├── predicate/
│   ├── mod.rs          # Predicate trait, PredicateSet, composition
│   ├── ssid.rs
│   ├── lowdata.rs
│   ├── power.rs
│   └── iface.rs
├── process/
│   ├── mod.rs          # Spawner / ChildHandle traits
│   ├── fake.rs
│   └── tokio.rs
└── supervisor.rs       # Supervisor: pre-flight + spawn + watch + kill
tests/
├── cli.rs              # assert_cmd integration
├── supervisor.rs       # supervisor end-to-end with fakes
└── predicates.rs       # cross-predicate composition
```

## 8. Testing Strategy

### 8.1 Coverage targets

- Library code (`src/state`, `src/predicate`, `src/process`, `src/supervisor`, `src/config`): **>90% line coverage** measured by `cargo llvm-cov`.
- Real platform impls (`src/state/{macos,linux}.rs`): exercised via opt-in integration tests behind `--features platform-tests`. Not counted toward coverage gate.
- CLI binary glue (`src/main.rs`, `src/cli.rs` dispatch): smoke-tested via `assert_cmd`.

### 8.2 Test pyramid

1. **Unit (≈70%)** — each Predicate impl against `FakeNetworkState`. Each Supervisor scenario against `FakeSpawner` + `FakeNetworkState` + `tokio::time::pause`. Config parsing.
2. **Integration (≈25%)** — `assert_cmd` invokes the real `condrun` binary with `--state-source=env` mode (test-only flag) so the binary reads canned state from env vars, exercising real CLI parsing + dispatch + supervisor wiring without real network calls.
3. **Platform smoke (≈5%, opt-in)** — `cargo test --features platform-tests` exercises `MacOsNetworkState` / `LinuxNetworkState` against the actual host.

### 8.3 Critical scenarios (must have tests before tagging v0.1.0)

1. Pre-flight pass → child runs → child exits 0 → condrun exits 0.
2. Pre-flight pass → child runs → child exits 7 → condrun exits 2 (logs 7).
3. Pre-flight fail (non-strict) → no spawn → condrun exits 0.
4. Pre-flight fail (strict) → no spawn → condrun exits 1.
5. Pre-flight pass → child runs → state flips → SIGTERM → child exits → condrun exits 3.
6. Pre-flight pass → child runs → state flips → SIGTERM ignored → grace expires → SIGKILL → exits 3.
7. Pre-flight pass → child runs → state flickers fail→pass within poll interval → no kill (debounce check).
8. AND composition: 3 predicates, one fails → set fails. All pass → set passes.
9. OR composition (`--any`): 3 predicates, one passes → set passes. All fail → set fails.
10. Profile loaded from YAML overrides default predicate set. CLI flags override profile.
11. Watcher cadence: `--poll 5s` causes state re-eval every 5s of paused tokio time.
12. SIGINT to condrun → forwarded to child → graceful shutdown → propagates child exit.

### 8.4 Test discipline

- TDD: red-green-refactor for every public function. Test exists before impl.
- One assertion per test where possible; descriptive names (`pre_flight_fail_non_strict_exits_zero`).
- No flaky time-based tests — all use `tokio::time::pause()` + explicit `advance()`.
- No real network or process spawns in unit tests.
- `cargo nextest` for parallel execution; CI fails on >0.5% flake rate.

### 8.5 Bug fix rule

Any bug discovered post-v0.1 ships with a regression test that fails against the broken code and passes against the fix. Test name describes the symptom in behavioral terms; comment cites the bug.

## 9. Configuration

### 9.1 Discovery order

1. `--config PATH` if given.
2. `$CONDRUN_CONFIG` env var.
3. `$XDG_CONFIG_HOME/condrun/profiles.yaml`.
4. `~/.config/condrun/profiles.yaml`.
5. None (CLI-only mode).

### 9.2 Schema

```yaml
defaults:
  grace: 30s
  poll: 30s
  kill_on_change: true

profiles:
  <name>:
    require_ssid: [<string>, ...]
    reject_ssid: [<string>, ...]
    reject_low_data: <bool>
    require_ac_power: <bool>
    require_interface_type: <wifi|ethernet|tether>
    reject_tether: <bool>
    grace: <duration>
    poll: <duration>
    kill_on_change: <bool>
```

CLI flags override profile values. Profile + CLI predicates compose as union (both apply).

## 10. Logging & Observability

- Structured logs via `tracing`. Default: `condrun=info` to stderr.
- `--verbose` raises to `debug`; `RUST_LOG` env override respected.
- Log lines: predicate evaluation start/end, state change events, child spawn, child exit, signal delivery, grace timeout.
- `--log-format json` for machine consumption (NDJSON).
- No metrics endpoint in v0.1. Reserved for v0.3.

## 11. Security & Permissions

- macOS 14+: SSID read requires Location Services entitlement OR sudo. v0.1 ships **unsigned** and relies on `sudo -A` fallback when SSID query fails. v0.2 adds a signed-binary path with proper CoreLocation prompt.
- Reading `/Library/Preferences/com.apple.wifi.known-networks.plist` for Low Data Mode requires root. v0.1 calls `sudo -A plutil -p`. Document this prominently in README.
- Linux: NetworkManager via `nmcli` is unprivileged. Power via `on_ac_power` is unprivileged. No sudo escalation in v0.1.
- `condrun` never writes to system config or modifies network state. Read-only consumer.

## 12. Distribution

- `cargo install condrun` for Rust users.
- Homebrew tap: `jordanbaker/tap/condrun` (or core tap if accepted).
- Pre-built binaries via GitHub Actions: `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`.
- `.deb` via `cargo-deb`. `.rpm` via `cargo-generate-rpm`.
- AUR PKGBUILD post-v0.1.

## 13. Versioning & Stability

- Semver. Pre-1.0: minor bumps may break CLI flags; patch bumps never break.
- v1.0 cuts when: predicate trait stable, CLI surface frozen, both platforms have native (non-shell-out) state impls, signed mac binary available, >90% test coverage sustained.

## 14. Open Questions

1. **Debouncing:** how long must a predicate stay failed before kill triggers? Default 0 (kill on first fail observation) vs configurable `--debounce 60s`. v0.1 picks: configurable, default 0.
2. **Child stdio:** inherit by default (so `condrun` is transparent), or capture-and-tag with `[child]` prefix? v0.1: inherit. Capture mode reserved.
3. **Multi-child:** out of scope for v0.1 by design. Upstream this to a launcher (`prefix`, `concurrently`) that itself runs under condrun.
4. **Windows:** v0.3+. NLA + WMI is doable but not a v0.1 priority.
5. **Native macOS APIs vs shell-out:** v0.1 ships shell-out. v0.2 swaps in `system-configuration` crate per-predicate as we hit perf or permission walls.
6. **Predicate hot-reload during `watch` daemon:** out of scope; restart is fine.

## 15. Implementation Roadmap

### v0.1.0 — vertical slice (this milestone)

- Trait scaffolding (NetworkState, Predicate, Spawner, ChildHandle).
- One predicate fully wired end-to-end: `require-ssid`.
- macOS shell-out `NetworkState` impl for SSID only.
- Tokio-backed `Spawner` with SIGTERM + grace + SIGKILL.
- `condrun run`, `condrun check` subcommands.
- Test suite covering §8.3 scenarios 1–6, 11.
- README with vykar wiring example.

### v0.1.x — full predicate catalog

- Add `reject-ssid`, `reject-low-data`, `require-ac-power`, `require-interface-type`, `reject-tether`.
- Linux state impl.
- `watch`, `explain`, `list-predicates` subcommands.
- Profile YAML loader.
- §8.3 scenarios 7–12.

### v0.2.0 — native APIs + signing

- Replace mac shell-outs with `system-configuration` crate where it removes sudo or improves latency.
- Signed mac binary; CoreLocation prompt for SSID.
- Linux: `zbus` direct to NetworkManager.
- Captive portal predicate.
- Bandwidth predicate.

### v0.3.0+

- Windows support.
- Time-window and generic file/cmd predicates.
- Metrics endpoint.

## 16. References

- [watchexec](https://watchexec.github.io/) — process lifecycle and signal escalation patterns.
- [borgmatic before/after hooks](https://torsion.org/borgmatic/) — soft-skip via exit-75 precedent.
- [systemd ExecCondition= docs](https://www.freedesktop.org/software/systemd/man/systemd.service.html#ExecCondition=) — semantic anchor for pre-flight gating.
- [NetworkManager-dispatcher](https://networkmanager.dev/docs/api/latest/NetworkManager-dispatcher.html) — Linux event-driven dispatcher.
- [system-configuration crate](https://crates.io/crates/system-configuration) — macOS native bindings for v0.2.
- [zbus](https://crates.io/crates/zbus) — Rust D-Bus client for v0.2 Linux.
