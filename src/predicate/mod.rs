use async_trait::async_trait;

use crate::state::NetworkState;

pub mod metered;

/// Outcome of evaluating a single predicate against a network state snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateResult {
    Pass,
    Fail { reason: String },
}

/// A boolean condition over `NetworkState`. Predicates compose via `PredicateSet`.
#[async_trait]
pub trait Predicate: Send + Sync {
    /// Stable identifier for logging / error messages (e.g. "reject-expensive").
    fn name(&self) -> &str;

    /// Evaluate the predicate against the current state snapshot.
    async fn evaluate(&self, state: &dyn NetworkState) -> PredicateResult;
}

/// Composition mode for combining multiple predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Composition {
    And,
    Or,
}

/// A set of predicates evaluated together with a composition rule.
pub struct PredicateSet {
    predicates: Vec<Box<dyn Predicate>>,
    composition: Composition,
}

impl PredicateSet {
    pub fn new(predicates: Vec<Box<dyn Predicate>>, composition: Composition) -> Self {
        Self { predicates, composition }
    }

    pub fn and(predicates: Vec<Box<dyn Predicate>>) -> Self {
        Self::new(predicates, Composition::And)
    }

    pub fn or(predicates: Vec<Box<dyn Predicate>>) -> Self {
        Self::new(predicates, Composition::Or)
    }

    pub async fn evaluate(&self, state: &dyn NetworkState) -> PredicateResult {
        match self.composition {
            Composition::And => {
                if self.predicates.is_empty() {
                    return PredicateResult::Pass; // vacuous truth
                }
                for p in &self.predicates {
                    match p.evaluate(state).await {
                        PredicateResult::Pass => continue,
                        fail @ PredicateResult::Fail { .. } => return fail,
                    }
                }
                PredicateResult::Pass
            }
            Composition::Or => {
                if self.predicates.is_empty() {
                    return PredicateResult::Fail { reason: "no predicates".into() };
                }
                let mut reasons = Vec::new();
                for p in &self.predicates {
                    match p.evaluate(state).await {
                        PredicateResult::Pass => return PredicateResult::Pass,
                        PredicateResult::Fail { reason } => reasons.push(reason),
                    }
                }
                PredicateResult::Fail { reason: reasons.join("; ") }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::fake::FakeNetworkState;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct AlwaysPass;

    #[async_trait]
    impl Predicate for AlwaysPass {
        fn name(&self) -> &str {
            "always-pass"
        }
        async fn evaluate(&self, _state: &dyn NetworkState) -> PredicateResult {
            PredicateResult::Pass
        }
    }

    struct AlwaysFail(&'static str);

    #[async_trait]
    impl Predicate for AlwaysFail {
        fn name(&self) -> &str {
            "always-fail"
        }
        async fn evaluate(&self, _state: &dyn NetworkState) -> PredicateResult {
            PredicateResult::Fail { reason: self.0.to_string() }
        }
    }

    struct CountingPredicate {
        counter: Arc<AtomicUsize>,
        result: PredicateResult,
    }

    #[async_trait]
    impl Predicate for CountingPredicate {
        fn name(&self) -> &str {
            "counting"
        }
        async fn evaluate(&self, _state: &dyn NetworkState) -> PredicateResult {
            self.counter.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }
    }

    #[tokio::test]
    async fn and_all_pass() {
        let state = FakeNetworkState::new();
        let set = PredicateSet::and(vec![
            Box::new(AlwaysPass),
            Box::new(AlwaysPass),
            Box::new(AlwaysPass),
        ]);
        assert_eq!(set.evaluate(&state).await, PredicateResult::Pass);
    }

    #[tokio::test]
    async fn and_short_circuits() {
        let state = FakeNetworkState::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let set = PredicateSet::and(vec![
            Box::new(AlwaysPass),
            Box::new(AlwaysFail("x")),
            Box::new(CountingPredicate {
                counter: counter.clone(),
                result: PredicateResult::Pass,
            }),
        ]);
        assert_eq!(
            set.evaluate(&state).await,
            PredicateResult::Fail { reason: "x".to_string() }
        );
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn or_short_circuits() {
        let state = FakeNetworkState::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let set = PredicateSet::or(vec![
            Box::new(AlwaysFail("a")),
            Box::new(AlwaysPass),
            Box::new(CountingPredicate {
                counter: counter.clone(),
                result: PredicateResult::Pass,
            }),
        ]);
        assert_eq!(set.evaluate(&state).await, PredicateResult::Pass);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn or_all_fail() {
        let state = FakeNetworkState::new();
        let set = PredicateSet::or(vec![
            Box::new(AlwaysFail("a")),
            Box::new(AlwaysFail("b")),
            Box::new(AlwaysFail("c")),
        ]);
        match set.evaluate(&state).await {
            PredicateResult::Fail { reason } => {
                assert!(reason.contains("a"), "missing 'a' in {reason}");
                assert!(reason.contains("b"), "missing 'b' in {reason}");
                assert!(reason.contains("c"), "missing 'c' in {reason}");
            }
            PredicateResult::Pass => panic!("expected Fail, got Pass"),
        }
    }

    #[tokio::test]
    async fn and_empty() {
        let state = FakeNetworkState::new();
        let set = PredicateSet::new(vec![], Composition::And);
        assert_eq!(set.evaluate(&state).await, PredicateResult::Pass);
    }

    #[tokio::test]
    async fn or_empty() {
        let state = FakeNetworkState::new();
        let set = PredicateSet::new(vec![], Composition::Or);
        match set.evaluate(&state).await {
            PredicateResult::Fail { .. } => {}
            PredicateResult::Pass => panic!("expected Fail, got Pass"),
        }
    }
}
