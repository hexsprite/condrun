use std::process::ExitStatus;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::signal::SignalKind;

pub mod fake;
pub mod tokio;

/// Description of a child process to spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
}

/// Spawns child processes. Implementations: real Tokio spawner, test fakes.
#[async_trait]
pub trait Spawner: Send + Sync {
    async fn spawn(&self, cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>>;
}

/// Handle to a running child process.
#[async_trait]
pub trait ChildHandle: Send + Sync {
    /// Wait for the child to exit and return its status.
    async fn wait(&mut self) -> ExitStatus;

    /// Predicate-flip kill path: SIGTERM, wait `grace`, escalate to SIGKILL if still alive.
    async fn terminate(&mut self, grace: Duration) -> Result<()>;

    /// Raw signal forwarding (no escalation). Used for SIGINT/SIGTERM passthrough.
    async fn signal(&mut self, kind: SignalKind) -> Result<()>;

    /// PID of the child (or process-group leader).
    fn pid(&self) -> u32;
}
