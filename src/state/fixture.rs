//! JSON fixture-backed `NetworkState` implementation for integration tests.
//!
//! Gated by the `test-fixture` cargo feature so production builds never include
//! this code path. Reads either a single static snapshot or a time-indexed
//! sequence of snapshots from JSON, then serves [`NetworkState`] queries by
//! consulting the fixture relative to its own creation time.
//!
//! Time is measured with [`tokio::time::Instant`] (not [`std::time::Instant`])
//! so that `tokio::time::pause()` + `advance()` can drive the fixture
//! deterministically inside `#[tokio::test(start_paused = true)]` tests.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::NetworkState;

/// JSON-deserializable fixture: either a single static snapshot or a sequence
/// of timed transitions.
///
/// `#[serde(untagged)]` lets a fixture file be written as either:
/// ```json
/// { "expensive": true, "low_data": false }
/// ```
/// or:
/// ```json
/// [
///   { "at_secs": 0.0, "state": { "expensive": false, "low_data": false } },
///   { "at_secs": 5.0, "state": { "expensive": true,  "low_data": false } }
/// ]
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StateFixture {
    /// Single snapshot — never changes.
    Static(NetworkSnapshot),
    /// Time-ordered list of snapshots applied at `at_secs` after fixture
    /// creation.
    Sequence(Vec<TimedSnapshot>),
}

/// One observation of network state — mirrors the v0.1.0 [`NetworkState`]
/// trait surface. Extend as new methods are added in v0.1.x.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct NetworkSnapshot {
    #[serde(default)]
    pub expensive: bool,
    #[serde(default)]
    pub low_data: bool,
}

/// A snapshot to apply at `at_secs` seconds after the fixture was created.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct TimedSnapshot {
    pub at_secs: f64,
    pub state: NetworkSnapshot,
}

/// Inner mutable state for [`FixtureNetworkState`].
#[derive(Debug)]
struct Inner {
    fixture: StateFixture,
    created_at: Instant,
}

/// Test-only `NetworkState` backed by a JSON fixture.
///
/// Construct via [`FixtureNetworkState::new`], [`FixtureNetworkState::load_from_str`],
/// or [`FixtureNetworkState::load_from_path`]. Records its creation time at
/// construction and uses it as the origin (`t = 0`) for any `Sequence` lookups.
#[derive(Debug, Clone)]
pub struct FixtureNetworkState {
    inner: Arc<Mutex<Inner>>,
}

impl FixtureNetworkState {
    /// Build a fixture state from an in-memory [`StateFixture`]. The current
    /// `tokio::time::Instant::now()` becomes the fixture's `t = 0`.
    pub fn new(fixture: StateFixture) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                fixture,
                created_at: Instant::now(),
            })),
        }
    }

    /// Parse a JSON string into a [`StateFixture`] and wrap it.
    ///
    /// Returns `Err` with a descriptive message on malformed JSON.
    pub fn load_from_str(s: &str) -> Result<Self> {
        let fixture: StateFixture = serde_json::from_str(s)
            .context("failed to parse state fixture JSON")?;
        Ok(Self::new(fixture))
    }

    /// Read a JSON file from `path` and wrap it as a fixture state.
    ///
    /// Returns `Err` with a descriptive message if the file is missing,
    /// unreadable, or contains malformed JSON.
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).with_context(|| {
            format!("failed to read state fixture file: {}", path.display())
        })?;
        let fixture: StateFixture = serde_json::from_str(&contents).with_context(|| {
            format!("failed to parse state fixture JSON at {}", path.display())
        })?;
        Ok(Self::new(fixture))
    }
}

/// Resolve the snapshot active at `elapsed_secs` for a `Sequence` fixture.
///
/// Returns the latest snapshot whose `at_secs <= elapsed_secs`. If no snapshot
/// has triggered yet (e.g. all `at_secs > 0` and we're still at `t = 0`) or if
/// the sequence is empty, falls back to a default (all-false) snapshot.
fn snapshot_at(snapshots: &[TimedSnapshot], elapsed_secs: f64) -> NetworkSnapshot {
    snapshots
        .iter()
        .filter(|s| s.at_secs <= elapsed_secs)
        .max_by(|a, b| {
            a.at_secs
                .partial_cmp(&b.at_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|s| s.state)
        .unwrap_or_default()
}

#[async_trait]
impl NetworkState for FixtureNetworkState {
    async fn is_expensive(&self) -> bool {
        let guard = self.inner.lock().await;
        match &guard.fixture {
            StateFixture::Static(snap) => snap.expensive,
            StateFixture::Sequence(snaps) => {
                let elapsed = Instant::now()
                    .saturating_duration_since(guard.created_at)
                    .as_secs_f64();
                snapshot_at(snaps, elapsed).expensive
            }
        }
    }

    async fn is_low_data_mode(&self) -> bool {
        let guard = self.inner.lock().await;
        match &guard.fixture {
            StateFixture::Static(snap) => snap.low_data,
            StateFixture::Sequence(snaps) => {
                let elapsed = Instant::now()
                    .saturating_duration_since(guard.created_at)
                    .as_secs_f64();
                snapshot_at(snaps, elapsed).low_data
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn static_fixture_round_trips_fields() {
        let json = r#"{ "expensive": true, "low_data": false }"#;
        let state = FixtureNetworkState::load_from_str(json).expect("valid JSON");
        assert!(state.is_expensive().await, "expensive should be true");
        assert!(
            !state.is_low_data_mode().await,
            "low_data should be false"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn static_fixture_round_trips_inverse_fields() {
        let json = r#"{ "expensive": false, "low_data": true }"#;
        let state = FixtureNetworkState::load_from_str(json).expect("valid JSON");
        assert!(!state.is_expensive().await);
        assert!(state.is_low_data_mode().await);
    }

    #[tokio::test(start_paused = true)]
    async fn sequence_fixture_flips_after_advance() {
        let json = r#"[
            { "at_secs": 0.0, "state": { "expensive": false, "low_data": false } },
            { "at_secs": 5.0, "state": { "expensive": true,  "low_data": false } }
        ]"#;
        let state = FixtureNetworkState::load_from_str(json).expect("valid JSON");

        // At t=0, the first snapshot applies.
        assert!(
            !state.is_expensive().await,
            "is_expensive should be false at t=0"
        );

        // Advance past the 5s transition.
        tokio::time::advance(Duration::from_secs(5)).await;

        assert!(
            state.is_expensive().await,
            "is_expensive should flip to true at t=5"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sequence_fixture_holds_at_latest_snapshot() {
        let json = r#"[
            { "at_secs": 0.0, "state": { "expensive": false, "low_data": false } },
            { "at_secs": 2.0, "state": { "expensive": true,  "low_data": true  } }
        ]"#;
        let state = FixtureNetworkState::load_from_str(json).expect("valid JSON");

        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(state.is_expensive().await);
        assert!(state.is_low_data_mode().await);
    }

    #[tokio::test]
    async fn invalid_json_returns_descriptive_error() {
        let err = FixtureNetworkState::load_from_str("{ not valid json")
            .expect_err("malformed JSON should error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("state fixture"),
            "error message should mention fixture context, got: {msg}"
        );
    }

    #[tokio::test]
    async fn nonexistent_path_returns_error() {
        let path = PathBuf::from("/definitely/does/not/exist/condrun-fixture.json");
        let err = FixtureNetworkState::load_from_path(&path)
            .expect_err("nonexistent file should error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("state fixture file") || msg.contains("definitely"),
            "error should reference the file path, got: {msg}"
        );
    }

    #[tokio::test]
    async fn malformed_json_does_not_panic() {
        // Schema-valid JSON but wrong shape (number instead of object/array).
        let result = FixtureNetworkState::load_from_str("42");
        assert!(result.is_err());
    }
}
