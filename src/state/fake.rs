//! `FakeNetworkState` — scriptable test double for the [`NetworkState`] trait.
//!
//! Tests construct one with [`FakeNetworkState::new`], flip flags via
//! [`set_expensive`](FakeNetworkState::set_expensive) /
//! [`set_low_data`](FakeNetworkState::set_low_data), and the supervisor's
//! polling loop will observe the new values on its next read.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use super::NetworkState;

#[derive(Debug, Default, Clone, Copy)]
struct FakeState {
    expensive: bool,
    low_data: bool,
}

/// Scriptable [`NetworkState`] for tests. Cheap to clone — shares state via `Arc`.
#[derive(Debug, Clone, Default)]
pub struct FakeNetworkState {
    inner: Arc<Mutex<FakeState>>,
}

impl FakeNetworkState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_expensive(&self, value: bool) {
        self.inner.lock().expect("FakeNetworkState mutex poisoned").expensive = value;
    }

    pub fn set_low_data(&self, value: bool) {
        self.inner.lock().expect("FakeNetworkState mutex poisoned").low_data = value;
    }
}

#[async_trait]
impl NetworkState for FakeNetworkState {
    async fn is_expensive(&self) -> bool {
        self.inner.lock().expect("FakeNetworkState mutex poisoned").expensive
    }

    async fn is_low_data_mode(&self) -> bool {
        self.inner.lock().expect("FakeNetworkState mutex poisoned").low_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_expensive_round_trips() {
        let s = FakeNetworkState::new();
        assert!(!s.is_expensive().await);
        s.set_expensive(true);
        assert!(s.is_expensive().await);
        s.set_expensive(false);
        assert!(!s.is_expensive().await);
    }

    #[tokio::test]
    async fn set_low_data_round_trips() {
        let s = FakeNetworkState::new();
        assert!(!s.is_low_data_mode().await);
        s.set_low_data(true);
        assert!(s.is_low_data_mode().await);
        s.set_low_data(false);
        assert!(!s.is_low_data_mode().await);
    }

    #[tokio::test]
    async fn flags_are_independent() {
        let s = FakeNetworkState::new();
        s.set_expensive(true);
        assert!(s.is_expensive().await);
        assert!(!s.is_low_data_mode().await);
        s.set_low_data(true);
        assert!(s.is_expensive().await);
        assert!(s.is_low_data_mode().await);
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let a = FakeNetworkState::new();
        let b = a.clone();
        a.set_expensive(true);
        assert!(b.is_expensive().await);
    }

    #[tokio::test]
    async fn send_sync_compile() {
        // Compile-time check that the fake crosses thread boundaries.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FakeNetworkState>();

        let s = FakeNetworkState::new();
        let s2 = s.clone();
        let handle = tokio::spawn(async move {
            s2.set_expensive(true);
        });
        handle.await.unwrap();
        assert!(s.is_expensive().await);
    }
}
