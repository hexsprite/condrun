//! Integration tests for condrun via assert_cmd.
//!
//! Run with: `cargo test --features test-fixture --test cli`
//! (or `cargo nextest run --features test-fixture --test cli`).
//!
//! `--features test-fixture` is REQUIRED — Cargo's `required-features` only
//! gates whether the test target compiles, it does NOT propagate features to
//! the binary build that `assert_cmd::Command::cargo_bin` invokes. Without the
//! feature flag the binary won't include `--state-source` and these tests
//! cannot drive the predicate state deterministically.
//!
//! Use `--test-threads=1` if you observe interference from concurrent signal
//! handlers across tests.
//!
//! Covers SPEC §8.3 scenarios 1-7, AND composition, and the `check` subcommand
//! variants (pass / fail / explain), plus CLI parse-error and fixture-load
//! failure paths.

use std::io::Write;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::NamedTempFile;

/// Write a JSON fixture body to a fresh tempfile and return it. The caller
/// keeps the handle alive for the duration of the test; dropping deletes the
/// file. The path stays valid as long as the returned `NamedTempFile` is in
/// scope.
fn write_fixture(json: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("create tempfile");
    f.write_all(json.as_bytes()).expect("write fixture json");
    f.flush().expect("flush fixture json");
    f
}

/// Convenience: build a `Command` for the condrun binary. Fails loudly with a
/// hint if the binary wasn't built with `--features test-fixture`.
fn condrun() -> Command {
    Command::cargo_bin("condrun")
        .expect("condrun binary — build with `--features test-fixture`")
}

/// Build a `file:<path>` URL for the `--state-source` flag.
fn file_url(f: &NamedTempFile) -> String {
    format!("file:{}", f.path().display())
}

// ---------------------------------------------------------------------------
// SPEC §8.3 scenarios
// ---------------------------------------------------------------------------

#[test]
fn scenario_1_pass_run_exit_0() {
    let f = write_fixture(r#"{"expensive":false,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "echo",
            "done",
        ])
        .assert()
        .success();
}

#[test]
fn scenario_2_child_fail_exit_2() {
    let f = write_fixture(r#"{"expensive":false,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "sh",
            "-c",
            "exit 7",
        ])
        .assert()
        .code(2);
}

#[test]
fn scenario_3_preflight_fail_silent() {
    let f = write_fixture(r#"{"expensive":true,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "echo",
            "should-not-run",
        ])
        .assert()
        .code(0)
        .stdout(predicate::str::contains("should-not-run").not());
}

#[test]
fn scenario_4_preflight_fail_strict() {
    let f = write_fixture(r#"{"expensive":true,"low_data":false}"#);
    condrun()
        .args([
            "--strict",
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "echo",
            "no",
        ])
        .assert()
        .code(1);
}

#[test]
fn scenario_5_kill_on_change() {
    // expensive flips true at t=2s while a `sleep 60` child is running.
    let f = write_fixture(
        r#"[
            {"at_secs":0,"state":{"expensive":false,"low_data":false}},
            {"at_secs":2,"state":{"expensive":true,"low_data":false}}
        ]"#,
    );
    condrun()
        .args([
            "--reject-expensive",
            "--poll",
            "1s",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "sleep",
            "60",
        ])
        .timeout(Duration::from_secs(20))
        .assert()
        .code(3);
}

#[test]
fn scenario_6_sigkill_after_grace() {
    // SIGTERM-ignoring child + 1s grace → SIGKILL → exit 3.
    let f = write_fixture(
        r#"[
            {"at_secs":0,"state":{"expensive":false,"low_data":false}},
            {"at_secs":2,"state":{"expensive":true,"low_data":false}}
        ]"#,
    );
    condrun()
        .args([
            "--reject-expensive",
            "--poll",
            "1s",
            "--grace",
            "1s",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "sh",
            "-c",
            "trap '' TERM; sleep 999",
        ])
        .timeout(Duration::from_secs(20))
        .assert()
        .code(3);
}

#[test]
fn scenario_7_debounce_flicker() {
    // Flicker shorter than --debounce → child not killed, exits naturally
    // when the inner `sleep 10` finishes → exit 0.
    let f = write_fixture(
        r#"[
            {"at_secs":0,"state":{"expensive":false,"low_data":false}},
            {"at_secs":2,"state":{"expensive":true,"low_data":false}},
            {"at_secs":3,"state":{"expensive":false,"low_data":false}}
        ]"#,
    );
    condrun()
        .args([
            "--reject-expensive",
            "--debounce",
            "5s",
            "--poll",
            "1s",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "sleep",
            "10",
        ])
        .timeout(Duration::from_secs(25))
        .assert()
        .success();
}

// ---------------------------------------------------------------------------
// Composition + check subcommand
// ---------------------------------------------------------------------------

#[test]
fn and_composition_low_data_fails() {
    // expensive=false (passes reject-expensive) but low_data=true (fails
    // reject-low-data) → AND fails → strict run → exit 1.
    let f = write_fixture(r#"{"expensive":false,"low_data":true}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--reject-low-data",
            "--strict",
            "--state-source",
            &file_url(&f),
            "run",
            "--",
            "echo",
            "x",
        ])
        .assert()
        .code(1)
        .stderr(
            predicate::str::contains("low_data").or(predicate::str::contains("Low Data Mode")),
        );
}

#[test]
fn check_pass() {
    let f = write_fixture(r#"{"expensive":false,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "check",
        ])
        .assert()
        .success();
}

#[test]
fn check_fail() {
    let f = write_fixture(r#"{"expensive":true,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "check",
        ])
        .assert()
        .code(1);
}

#[test]
fn check_explain_output() {
    // `--explain` writes per-predicate PASS/FAIL lines to stdout (println!).
    let f = write_fixture(r#"{"expensive":true,"low_data":false}"#);
    condrun()
        .args([
            "--reject-expensive",
            "--state-source",
            &file_url(&f),
            "check",
            "--explain",
        ])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("FAIL: reject-expensive"))
        .stdout(predicate::str::contains("expensive"));
}

// ---------------------------------------------------------------------------
// CLI parse / fixture-load error paths → exit 4
// ---------------------------------------------------------------------------

#[test]
fn invalid_flag_exits_4() {
    // `last = true` on `cmd` makes clap reject `--bogus` before `--`.
    condrun()
        .args(["run", "--bogus", "--", "echo", "x"])
        .assert()
        .code(4);
}

#[test]
fn nonexistent_fixture_exits_4() {
    condrun()
        .args([
            "--state-source",
            "file:/nonexistent-condrun-fixture-xyz.json",
            "run",
            "--",
            "echo",
            "x",
        ])
        .assert()
        .code(4);
}
