//! `FakeSignals` — scriptable test double for the [`Signals`] trait.
//!
//! Tests call [`FakeSignals::inject`] to push a [`SignalKind`] into the queue.
//! [`Signals::next`] pops the head, blocking on a [`Notify`] when the queue
//! is empty.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use tokio::sync::Notify;

use super::{SignalKind, Signals};

/// Scriptable [`Signals`] for tests.
///
/// Cheap to clone — shares state via `Arc`, so a test can inject through one
/// handle while the supervisor pulls from another.
#[derive(Debug, Clone, Default)]
pub struct FakeSignals {
    queue: Arc<Mutex<VecDeque<SignalKind>>>,
    notify: Arc<Notify>,
}

impl FakeSignals {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a signal into the queue. Wakes any waiter blocked on `next()`.
    pub fn inject(&self, kind: SignalKind) {
        self.queue
            .lock()
            .expect("FakeSignals queue mutex poisoned")
            .push_back(kind);
        self.notify.notify_one();
    }
}

#[async_trait]
impl Signals for FakeSignals {
    async fn next(&mut self) -> SignalKind {
        loop {
            // Register interest BEFORE checking the queue, so we don't miss
            // a notification that fires between the check and the wait.
            let notified = self.notify.notified();
            tokio::pin!(notified);

            if let Some(s) = self
                .queue
                .lock()
                .expect("FakeSignals queue mutex poisoned")
                .pop_front()
            {
                return s;
            }

            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn inject_then_next_returns_injected_kind() {
        let mut s = FakeSignals::new();
        s.inject(SignalKind::Interrupt);
        let got = s.next().await;
        assert_eq!(got, SignalKind::Interrupt);
    }

    #[tokio::test(start_paused = true)]
    async fn inject_terminate_then_next_returns_terminate() {
        let mut s = FakeSignals::new();
        s.inject(SignalKind::Terminate);
        assert_eq!(s.next().await, SignalKind::Terminate);
    }

    #[tokio::test(start_paused = true)]
    async fn next_returns_in_fifo_order() {
        let mut s = FakeSignals::new();
        s.inject(SignalKind::Interrupt);
        s.inject(SignalKind::Terminate);
        assert_eq!(s.next().await, SignalKind::Interrupt);
        assert_eq!(s.next().await, SignalKind::Terminate);
    }

    #[tokio::test(start_paused = true)]
    async fn next_blocks_until_inject() {
        let signals = FakeSignals::new();
        let injector = signals.clone();
        let mut consumer = signals;

        let waiter = tokio::spawn(async move { consumer.next().await });

        // Advance virtual time without injecting — the waiter must remain pending.
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            !waiter.is_finished(),
            "next() should block while queue is empty"
        );

        // Inject and the waiter resolves with the injected kind.
        injector.inject(SignalKind::Terminate);
        let got = waiter.await.unwrap();
        assert_eq!(got, SignalKind::Terminate);
    }

    #[tokio::test]
    async fn send_sync_compile() {
        // Compile-time check.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FakeSignals>();

        let signals = FakeSignals::new();
        let injector = signals.clone();
        let handle = tokio::spawn(async move {
            injector.inject(SignalKind::Interrupt);
        });
        handle.await.unwrap();
        let mut consumer = signals;
        assert_eq!(consumer.next().await, SignalKind::Interrupt);
    }
}
