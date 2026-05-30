//! Metered-connection predicates.
//!
//! Two unit-struct predicates that veto execution when the active path is
//! flagged as expensive (Personal Hotspot / cellular) or has Low Data Mode
//! enabled. Both are "reject"-style: they `Pass` when the corresponding
//! NWPath signal is `false`, and `Fail` (with a human-readable reason) when
//! it is `true`.

use async_trait::async_trait;

use crate::predicate::{Predicate, PredicateResult};
use crate::state::NetworkState;

/// Vetoes execution while the active path is "expensive" per `NWPath`
/// (Personal Hotspot, cellular tether, etc.).
pub struct RejectExpensive;

#[async_trait]
impl Predicate for RejectExpensive {
    fn name(&self) -> &str {
        "reject-expensive"
    }

    async fn evaluate(&self, state: &dyn NetworkState) -> PredicateResult {
        if state.is_expensive().await {
            PredicateResult::Fail {
                reason: "current connection is expensive (Personal Hotspot or cellular)".into(),
            }
        } else {
            PredicateResult::Pass
        }
    }
}

/// Vetoes execution while the active path has Low Data Mode enabled.
pub struct RejectLowData;

#[async_trait]
impl Predicate for RejectLowData {
    fn name(&self) -> &str {
        "reject-low-data"
    }

    async fn evaluate(&self, state: &dyn NetworkState) -> PredicateResult {
        if state.is_low_data_mode().await {
            PredicateResult::Fail {
                reason: "current connection has Low Data Mode enabled".into(),
            }
        } else {
            PredicateResult::Pass
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::fake::FakeNetworkState;

    // Acceptance #1
    #[tokio::test]
    async fn reject_expensive_pass() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        assert_eq!(
            RejectExpensive.evaluate(&state).await,
            PredicateResult::Pass
        );
    }

    // Acceptance #2
    #[tokio::test]
    async fn reject_expensive_fail() {
        let state = FakeNetworkState::new();
        state.set_expensive(true);
        match RejectExpensive.evaluate(&state).await {
            PredicateResult::Fail { reason } => {
                assert!(
                    reason.contains("expensive"),
                    "reason missing 'expensive': {reason}"
                );
                assert!(
                    reason.contains("Personal Hotspot") || reason.contains("cellular"),
                    "reason missing source attribution: {reason}"
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    // Acceptance #3
    #[tokio::test]
    async fn reject_expensive_name() {
        assert_eq!(RejectExpensive.name(), "reject-expensive");
    }

    // Acceptance #4
    #[tokio::test]
    async fn reject_low_data_pass() {
        let state = FakeNetworkState::new();
        state.set_low_data(false);
        assert_eq!(RejectLowData.evaluate(&state).await, PredicateResult::Pass);
    }

    // Acceptance #5
    #[tokio::test]
    async fn reject_low_data_fail() {
        let state = FakeNetworkState::new();
        state.set_low_data(true);
        match RejectLowData.evaluate(&state).await {
            PredicateResult::Fail { reason } => {
                assert!(
                    reason.contains("Low Data Mode"),
                    "reason missing 'Low Data Mode': {reason}"
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    // Acceptance #6
    #[tokio::test]
    async fn reject_low_data_name() {
        assert_eq!(RejectLowData.name(), "reject-low-data");
    }

    // Acceptance #7 — both Pass when state has expensive=false AND low_data=false.
    // PredicateSet is being added by a parallel agent; verify the AND outcome
    // by evaluating each predicate individually.
    #[tokio::test]
    async fn both_pass_via_and() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        state.set_low_data(false);

        let r1 = RejectExpensive.evaluate(&state).await;
        let r2 = RejectLowData.evaluate(&state).await;
        assert_eq!(r1, PredicateResult::Pass);
        assert_eq!(r2, PredicateResult::Pass);
    }

    // Acceptance #8 — AND fails when only one of the two fails. Combined
    // result should be Fail with a reason mentioning Low Data Mode.
    #[tokio::test]
    async fn and_fails_when_one_fails() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        state.set_low_data(true);

        let r1 = RejectExpensive.evaluate(&state).await;
        let r2 = RejectLowData.evaluate(&state).await;

        // RejectExpensive passes; RejectLowData fails. AND is the failing one.
        assert_eq!(r1, PredicateResult::Pass);
        let combined = match (&r1, &r2) {
            (PredicateResult::Pass, PredicateResult::Pass) => PredicateResult::Pass,
            (PredicateResult::Fail { reason }, _) | (_, PredicateResult::Fail { reason }) => {
                PredicateResult::Fail {
                    reason: reason.clone(),
                }
            }
        };
        match combined {
            PredicateResult::Fail { reason } => {
                assert!(
                    reason.contains("Low Data Mode"),
                    "combined reason missing 'Low Data Mode': {reason}"
                );
            }
            other => panic!("expected combined Fail, got {other:?}"),
        }
    }
}
