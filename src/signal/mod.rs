use async_trait::async_trait;

pub mod fake;
pub mod real;

/// OS signal we observe on condrun and forward to the child process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    /// SIGINT — Ctrl+C from terminal.
    Interrupt,
    /// SIGTERM — polite termination request.
    Terminate,
    /// SIGQUIT — Ctrl+\ from terminal. Default disposition is core dump; we
    /// catch and forward instead so the child gets a chance to exit cleanly.
    Quit,
    /// SIGHUP — controlling terminal closed (e.g. ssh disconnect).
    Hup,
}

/// Source of OS signals delivered to the supervisor. Seam for testability.
#[async_trait]
pub trait Signals: Send + Sync {
    /// Wait for the next observable signal (SIGINT, SIGTERM, SIGQUIT, SIGHUP).
    async fn next(&mut self) -> SignalKind;
}
