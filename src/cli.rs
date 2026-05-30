//! Clap CLI definitions and dispatch.
//!
//! Top-level binary entry point lives in `src/main.rs`. This module owns:
//!
//! * The [`Cli`] / [`Command`] derive structs (clap-parsed).
//! * Manual duration parsing for `--grace`, `--poll`, `--debounce`
//!   (no `humantime` dep — keeps Cargo.toml stable).
//! * [`dispatch`] — the bridge from parsed flags to [`Supervisor`] /
//!   pre-flight check execution. Builds `PredicateSet`, `NetworkState`,
//!   `Spawner`, `Signals`, then delegates.
//!
//! Exit-code mapping (per SPEC §6):
//!
//!   * Successful run/check → 0
//!   * Pre-flight fail (strict) / explicit check fail → 1
//!   * Child exited non-zero (run) → 2
//!   * CLI parse error / dispatch error / fixture-load error → 4
//!     (parse-error mapping is done in `main.rs` because clap's default is 2).

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use crate::predicate::metered::{RejectExpensive, RejectLowData};
use crate::predicate::{Predicate, PredicateResult, PredicateSet};
use crate::process::tokio::TokioSpawner;
use crate::process::{CommandSpec, Spawner};
use crate::signal::Signals;
use crate::signal::real::RealSignals;
use crate::state::NetworkState;
use crate::supervisor::{Supervisor, SupervisorConfig};

/// Conditional command runner — gates execution on system network state.
#[derive(Parser, Debug)]
#[command(name = "condrun", version, about = "Conditional command runner")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Exit non-zero on pre-flight failure (default: silent exit 0).
    #[arg(long, global = true)]
    pub strict: bool,

    /// Disable killing the child if predicates flip mid-run.
    /// (Default behaviour: kill on change enabled.)
    #[arg(long = "no-kill-on-change", global = true)]
    pub no_kill_on_change: bool,

    /// Grace period between SIGTERM and SIGKILL.
    #[arg(long, global = true, value_parser = parse_duration, default_value = "30s")]
    pub grace: Duration,

    /// Watcher poll interval.
    #[arg(long, global = true, value_parser = parse_duration, default_value = "30s")]
    pub poll: Duration,

    /// Wall-clock duration that predicates must stay failed before kill.
    #[arg(long, global = true, value_parser = parse_duration, default_value = "0s")]
    pub debounce: Duration,

    /// Compose predicates with OR (default: AND).
    #[arg(long, global = true)]
    pub any: bool,

    /// Reject expensive connections (cellular / Personal Hotspot).
    #[arg(long, global = true)]
    pub reject_expensive: bool,

    /// Reject Low-Data-Mode connections.
    #[arg(long, global = true)]
    pub reject_low_data: bool,

    /// Increase log verbosity (repeatable: -v, -vv, ...).
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress logs.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Test-only fixture state source. Format: `file:<path>` to a JSON file
    /// matching `state::fixture::StateFixture`. Only available in builds
    /// compiled with `--features test-fixture`; hidden from `--help`.
    #[cfg(feature = "test-fixture")]
    #[arg(long, hide = true, global = true)]
    pub state_source: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run a command, gating + watching predicates.
    Run {
        /// Command to execute. Pass after `--`, e.g. `condrun run -- echo hi`.
        /// `last = true` makes clap require `--` before this positional, which
        /// (a) gives a clean error if a user types `condrun run --bogus` (the
        /// unknown flag isn't silently absorbed) and (b) lets the inner command
        /// freely use its own `--flags` without clap interpreting them.
        #[arg(last = true, num_args = 1..)]
        cmd: Vec<String>,
    },

    /// Evaluate predicates once and exit.
    Check {
        /// Print each predicate's result (PASS / FAIL: <reason>).
        #[arg(long)]
        explain: bool,
    },
}

/// Parse a duration string like `30s`, `5m`, `1h`. Accepts integer values
/// only (no fractional seconds — the supervisor doesn't need them).
///
/// Errors are returned as `String` so clap can present them inline.
fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    if s.is_empty() {
        return Err("empty duration".into());
    }
    let bytes = s.as_bytes();
    // Find the index where the suffix begins (first non-digit).
    let split = bytes
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(bytes.len());
    if split == 0 {
        return Err(format!("missing numeric prefix in duration: {s}"));
    }
    let (num_str, unit) = s.split_at(split);
    let n: u64 = num_str
        .parse()
        .map_err(|e| format!("invalid duration number {num_str:?}: {e}"))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n
            .checked_mul(60)
            .ok_or_else(|| format!("duration overflow: {s}"))?,
        "h" => n
            .checked_mul(3600)
            .ok_or_else(|| format!("duration overflow: {s}"))?,
        other => {
            return Err(format!(
                "invalid duration unit {other:?} (expected s, m, or h)"
            ));
        }
    };
    Ok(Duration::from_secs(secs))
}

/// Build the list of predicates from CLI flags. Returns a fresh boxed vec
/// so callers can either feed it to `PredicateSet` or evaluate elements
/// individually (for `--explain`).
fn build_predicates(cli: &Cli) -> Vec<Box<dyn Predicate>> {
    let mut v: Vec<Box<dyn Predicate>> = Vec::new();
    if cli.reject_expensive {
        v.push(Box::new(RejectExpensive));
    }
    if cli.reject_low_data {
        v.push(Box::new(RejectLowData));
    }
    v
}

#[cfg(feature = "test-fixture")]
fn build_network_state(cli: &Cli) -> Result<Box<dyn NetworkState>> {
    if let Some(src) = &cli.state_source {
        if let Some(path) = src.strip_prefix("file:") {
            let fixture = crate::state::fixture::FixtureNetworkState::load_from_path(
                std::path::Path::new(path),
            )
            .with_context(|| format!("failed to load fixture state from {path}"))?;
            return Ok(Box::new(fixture));
        }
        anyhow::bail!("--state-source must use the `file:<path>` form");
    }
    real_network_state()
}

#[cfg(not(feature = "test-fixture"))]
fn build_network_state(_cli: &Cli) -> Result<Box<dyn NetworkState>> {
    real_network_state()
}

#[cfg(target_os = "macos")]
fn real_network_state() -> Result<Box<dyn NetworkState>> {
    let s =
        crate::state::macos::MacOsNetworkState::new().context("failed to start NWPathMonitor")?;
    Ok(Box::new(s))
}

#[cfg(not(target_os = "macos"))]
fn real_network_state() -> Result<Box<dyn NetworkState>> {
    anyhow::bail!("real NetworkState reader is only implemented for macOS")
}

/// Top-level dispatch. Returns `Ok(exit_code)` on success (caller runs
/// `process::exit`), `Err` on configuration / fixture-load / internal
/// failure (caller maps to exit 4).
pub async fn dispatch(cli: Cli) -> Result<i32> {
    let kill_on_change = !cli.no_kill_on_change;

    // Validate flag interactions.
    if !kill_on_change && cli.debounce > Duration::ZERO {
        tracing::warn!(
            "--debounce is ignored when --no-kill-on-change is set \
             (debounce only applies while watching for predicate flips)"
        );
    }

    match cli.command {
        Command::Run { ref cmd } => {
            if cmd.is_empty() {
                anyhow::bail!(
                    "missing command — pass it after `--`, e.g. `condrun run -- echo hi`"
                );
            }
            let spec = CommandSpec {
                program: cmd[0].clone(),
                args: cmd[1..].to_vec(),
            };

            let predicates = build_predicates(&cli);
            let predicate_set = if cli.any {
                PredicateSet::or(predicates)
            } else {
                PredicateSet::and(predicates)
            };

            let state = build_network_state(&cli)?;
            let spawner: Box<dyn Spawner> = Box::new(TokioSpawner);
            let signals: Box<dyn Signals> =
                Box::new(RealSignals::new().context("failed to install SIGINT/SIGTERM handlers")?);

            let config = SupervisorConfig {
                strict: cli.strict,
                kill_on_change,
                grace: cli.grace,
                poll: cli.poll,
                debounce: cli.debounce,
            };

            let supervisor = Supervisor {
                predicate_set,
                spawner,
                state,
                signals,
                config,
            };
            supervisor.run(&spec).await
        }
        Command::Check { explain } => {
            let predicates = build_predicates(&cli);
            let state = build_network_state(&cli)?;

            if explain {
                // Evaluate each predicate individually so we can report
                // per-predicate PASS / FAIL lines on stdout.
                for p in &predicates {
                    match p.evaluate(state.as_ref()).await {
                        PredicateResult::Pass => println!("PASS: {}", p.name()),
                        PredicateResult::Fail { reason } => {
                            println!("FAIL: {} — {}", p.name(), reason)
                        }
                    }
                }
                // Combined result respects --any (OR) vs default (AND).
                let predicate_set = if cli.any {
                    PredicateSet::or(predicates)
                } else {
                    PredicateSet::and(predicates)
                };
                Ok(match predicate_set.evaluate(state.as_ref()).await {
                    PredicateResult::Pass => 0,
                    PredicateResult::Fail { .. } => 1,
                })
            } else {
                let predicate_set = if cli.any {
                    PredicateSet::or(predicates)
                } else {
                    PredicateSet::and(predicates)
                };
                match predicate_set.evaluate(state.as_ref()).await {
                    PredicateResult::Pass => Ok(0),
                    PredicateResult::Fail { reason } => {
                        eprintln!("predicate failed: {reason}");
                        Ok(1)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_seconds_default() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn parse_duration_seconds_suffix() {
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn parse_duration_zero() {
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
    }

    #[test]
    fn parse_duration_rejects_empty() {
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn parse_duration_rejects_bad_unit() {
        assert!(parse_duration("10d").is_err());
        assert!(parse_duration("10ms").is_err());
    }

    #[test]
    fn parse_duration_rejects_no_number() {
        assert!(parse_duration("s").is_err());
    }

    #[test]
    fn cli_run_with_predicates_parses() {
        let cli = Cli::try_parse_from([
            "condrun",
            "run",
            "--reject-expensive",
            "--reject-low-data",
            "--",
            "echo",
            "hi",
        ])
        .expect("parse");
        match cli.command {
            Command::Run { cmd } => assert_eq!(cmd, vec!["echo", "hi"]),
            _ => panic!("expected Run"),
        }
        assert!(cli.reject_expensive);
        assert!(cli.reject_low_data);
    }

    #[test]
    fn cli_check_parses() {
        let cli = Cli::try_parse_from(["condrun", "check", "--reject-expensive"]).unwrap();
        match cli.command {
            Command::Check { explain } => assert!(!explain),
            _ => panic!("expected Check"),
        }
        assert!(cli.reject_expensive);
    }

    #[test]
    fn cli_grace_45s() {
        let cli =
            Cli::try_parse_from(["condrun", "--grace", "45s", "run", "--", "echo", "hi"]).unwrap();
        assert_eq!(cli.grace, Duration::from_secs(45));
    }

    #[test]
    fn cli_grace_2m() {
        let cli =
            Cli::try_parse_from(["condrun", "--grace", "2m", "run", "--", "echo", "hi"]).unwrap();
        assert_eq!(cli.grace, Duration::from_secs(120));
    }

    #[test]
    fn cli_no_subcommand_errors() {
        // Without a subcommand clap should produce an error (which `main.rs`
        // maps to exit 4). `try_parse_from` surfaces it for the test.
        let res = Cli::try_parse_from(["condrun"]);
        assert!(res.is_err());
    }

    #[test]
    fn cli_unknown_flag_errors() {
        // With `last = true` on `cmd`, hyphen-prefixed tokens before `--`
        // are interpreted as flags and unknown ones are rejected by clap.
        let res = Cli::try_parse_from(["condrun", "run", "--bogus", "--", "echo", "hi"]);
        assert!(res.is_err(), "expected unknown-flag error, got Ok");
    }

    #[test]
    fn cli_inner_command_flags_after_dashdash() {
        // `condrun run -- mycmd --some-flag arg` — inner command's own flags
        // must not collide with condrun's parser.
        let cli =
            Cli::try_parse_from(["condrun", "run", "--", "mycmd", "--inner-flag", "x"]).unwrap();
        match cli.command {
            Command::Run { cmd } => {
                assert_eq!(cmd, vec!["mycmd", "--inner-flag", "x"]);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn cli_no_kill_on_change_flag() {
        let cli =
            Cli::try_parse_from(["condrun", "--no-kill-on-change", "run", "--", "echo", "hi"])
                .unwrap();
        assert!(cli.no_kill_on_change);
    }

    #[cfg(feature = "test-fixture")]
    #[test]
    fn cli_state_source_parses_when_feature_enabled() {
        let cli = Cli::try_parse_from(["condrun", "--state-source", "file:/tmp/x.json", "check"])
            .unwrap();
        assert_eq!(cli.state_source.as_deref(), Some("file:/tmp/x.json"));
    }
}
