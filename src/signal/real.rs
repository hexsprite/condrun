//! `RealSignals` — production [`Signals`] impl wrapping `tokio::signal::unix`.
//!
//! Installs handlers for SIGINT and SIGTERM at construction time. Each call
//! to [`Signals::next`] races the two streams via `tokio::select!` and
//! returns the corresponding [`SignalKind`] variant.
//!
//! Tokio's own `SignalKind` is fully qualified (`TokioSignalKind`) to keep it
//! visibly distinct from condrun's plan-level [`SignalKind`] enum, which is
//! the type that flows through condrun's API.

use async_trait::async_trait;
use tokio::signal::unix::{
    Signal as TokioSignal, SignalKind as TokioSignalKind, signal as tokio_signal,
};

use crate::signal::{SignalKind, Signals};

/// Production [`Signals`] implementation. Wraps `tokio::signal::unix` streams
/// for SIGINT, SIGTERM, SIGQUIT, SIGHUP and returns whichever fires first.
///
/// We catch SIGQUIT and SIGHUP explicitly so that Ctrl+\ and terminal
/// disconnect don't kill condrun via the default disposition (which would
/// orphan the child). Both are forwarded to the child process group like
/// SIGINT/SIGTERM.
pub struct RealSignals {
    int: TokioSignal,
    term: TokioSignal,
    quit: TokioSignal,
    hup: TokioSignal,
}

impl RealSignals {
    /// Install handlers for SIGINT, SIGTERM, SIGQUIT, SIGHUP. Fails if the
    /// underlying syscall to register the handler fails (extremely rare —
    /// usually a permissions or fd-exhaustion issue).
    pub fn new() -> std::io::Result<Self> {
        let int = tokio_signal(TokioSignalKind::interrupt())?;
        let term = tokio_signal(TokioSignalKind::terminate())?;
        let quit = tokio_signal(TokioSignalKind::quit())?;
        let hup = tokio_signal(TokioSignalKind::hangup())?;
        Ok(Self {
            int,
            term,
            quit,
            hup,
        })
    }
}

#[async_trait]
impl Signals for RealSignals {
    async fn next(&mut self) -> SignalKind {
        // Disjoint borrows of four fields through a single `&mut self` —
        // each `recv()` future holds a borrow of a separate field, which the
        // compiler accepts.
        tokio::select! {
            _ = self.int.recv() => SignalKind::Interrupt,
            _ = self.term.recv() => SignalKind::Terminate,
            _ = self.quit.recv() => SignalKind::Quit,
            _ = self.hup.recv() => SignalKind::Hup,
        }
    }
}
