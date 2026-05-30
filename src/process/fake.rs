//! `FakeSpawner` + `FakeChildHandle` — scriptable test doubles for child processes.
//!
//! Records every [`CommandSpec`] passed to [`Spawner::spawn`], and produces
//! [`FakeChildHandle`]s with scriptable exit code, exit delay, and termination
//! behaviour.
//!
//! The handle distinguishes three paths:
//!   * graceful `terminate()` — `terminate_delay <= grace`, child exits within delay
//!   * forced kill — `terminate_delay > grace`, after grace expires `killed = true` and `wait()` resolves
//!   * raw signal forwarding via `signal()` — only records `last_signal`, no escalation

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::signal::SignalKind;

use super::{ChildHandle, CommandSpec, Spawner};

const FAKE_PID: u32 = 99_999;

/// Records every [`CommandSpec`] handed to [`Spawner::spawn`] and returns
/// [`FakeChildHandle`]s for the test to drive.
///
/// Tests script return values via [`FakeSpawner::with_handles`] (FIFO queue);
/// when the queue is empty, [`FakeSpawner::spawn`] returns a default handle
/// (`exit_code = 0`, `exit_delay = 0`).
#[derive(Default)]
pub struct FakeSpawner {
    spawned: Arc<Mutex<Vec<CommandSpec>>>,
    handles: Arc<Mutex<std::collections::VecDeque<FakeChildHandle>>>,
}

impl FakeSpawner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a child handle to be returned on the next `spawn()` call.
    pub fn enqueue_handle(&self, handle: FakeChildHandle) {
        self.handles
            .lock()
            .expect("FakeSpawner.handles mutex poisoned")
            .push_back(handle);
    }

    /// Snapshot of every `CommandSpec` passed to `spawn()`, in call order.
    pub fn spawned(&self) -> Vec<CommandSpec> {
        self.spawned
            .lock()
            .expect("FakeSpawner.spawned mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl Spawner for FakeSpawner {
    async fn spawn(&self, cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>> {
        self.spawned
            .lock()
            .expect("FakeSpawner.spawned mutex poisoned")
            .push(cmd.clone());

        let handle = self
            .handles
            .lock()
            .expect("FakeSpawner.handles mutex poisoned")
            .pop_front()
            .unwrap_or_else(|| FakeChildHandle::new(0, Duration::ZERO));
        Ok(Box::new(handle))
    }
}

#[derive(Debug, Default)]
struct ChildState {
    exited: bool,
    exit_code: i32,
    terminated: bool,
    killed: bool,
    last_signal: Option<SignalKind>,
}

/// Scriptable [`ChildHandle`].
///
/// * Constructor [`new`](Self::new) takes `exit_code` + `exit_delay` (how long
///   `wait()` runs before resolving naturally).
/// * Builder [`with_terminate_delay`](Self::with_terminate_delay) sets how long
///   the simulated child resists `SIGTERM` (default `0` = exits immediately on
///   terminate; set higher than `grace` to force SIGKILL escalation).
pub struct FakeChildHandle {
    exit_delay: Duration,
    terminate_delay: Duration,
    state: Arc<Mutex<ChildState>>,
    notify: Arc<Notify>,
}

impl FakeChildHandle {
    pub fn new(exit_code: i32, exit_delay: Duration) -> Self {
        let state = ChildState {
            exited: false,
            exit_code,
            terminated: false,
            killed: false,
            last_signal: None,
        };
        Self {
            exit_delay,
            terminate_delay: Duration::ZERO,
            state: Arc::new(Mutex::new(state)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Builder: how long the simulated child waits before exiting after
    /// `terminate()` is called. Default `0` means it exits immediately
    /// (graceful). Set higher than `grace` to simulate a SIGTERM-resistant
    /// process that requires SIGKILL escalation.
    pub fn with_terminate_delay(mut self, d: Duration) -> Self {
        self.terminate_delay = d;
        self
    }

    pub fn terminated(&self) -> bool {
        self.state
            .lock()
            .expect("FakeChildHandle mutex poisoned")
            .terminated
    }

    pub fn killed(&self) -> bool {
        self.state
            .lock()
            .expect("FakeChildHandle mutex poisoned")
            .killed
    }

    pub fn last_signal(&self) -> Option<SignalKind> {
        self.state
            .lock()
            .expect("FakeChildHandle mutex poisoned")
            .last_signal
    }

    fn mark_exited(&self) {
        let mut s = self.state.lock().expect("FakeChildHandle mutex poisoned");
        s.exited = true;
        drop(s);
        self.notify.notify_waiters();
    }

    fn snapshot_exit_code(&self) -> i32 {
        self.state
            .lock()
            .expect("FakeChildHandle mutex poisoned")
            .exit_code
    }
}

#[cfg(unix)]
fn make_status(code: i32) -> ExitStatus {
    // On unix, raw status word: low byte = signal/0, second byte = exit code.
    ExitStatus::from_raw(code << 8)
}

#[cfg(not(unix))]
fn make_status(code: i32) -> ExitStatus {
    use std::os::windows::process::ExitStatusExt;
    ExitStatus::from_raw(code as u32)
}

#[async_trait]
impl ChildHandle for FakeChildHandle {
    async fn wait(&mut self) -> ExitStatus {
        // Fast path: already exited (e.g., after terminate()).
        if self
            .state
            .lock()
            .expect("FakeChildHandle mutex poisoned")
            .exited
        {
            return make_status(self.snapshot_exit_code());
        }

        let notify = self.notify.clone();
        // Race the natural exit_delay against an external signal (terminate).
        // `Notify::notified()` is created before the timer to avoid losing
        // a notification that fires between checks.
        let notified = notify.notified();
        tokio::pin!(notified);

        tokio::select! {
            _ = tokio::time::sleep(self.exit_delay) => {
                self.mark_exited();
            }
            _ = &mut notified => {
                // Some other path (terminate) marked us exited.
            }
        }

        make_status(self.snapshot_exit_code())
    }

    async fn terminate(&mut self, grace: Duration) -> Result<()> {
        {
            let mut s = self.state.lock().expect("FakeChildHandle mutex poisoned");
            s.terminated = true;
        }

        if self.terminate_delay <= grace {
            // Graceful: child exits within grace.
            tokio::time::sleep(self.terminate_delay).await;
            self.mark_exited();
        } else {
            // Resistant child: wait the full grace, then escalate to SIGKILL.
            tokio::time::sleep(grace).await;
            {
                let mut s = self.state.lock().expect("FakeChildHandle mutex poisoned");
                s.killed = true;
            }
            self.mark_exited();
        }
        Ok(())
    }

    async fn signal(&mut self, kind: SignalKind) -> Result<()> {
        let mut s = self.state.lock().expect("FakeChildHandle mutex poisoned");
        s.last_signal = Some(kind);
        Ok(())
    }

    fn pid(&self) -> u32 {
        FAKE_PID
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn exit_code_of(status: ExitStatus) -> Option<i32> {
        status.code()
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_records_command_spec() {
        let spawner = FakeSpawner::new();
        let cmd = CommandSpec {
            program: "echo".into(),
            args: vec!["hello".into()],
        };
        let _h = spawner.spawn(&cmd).await.unwrap();

        let recorded = spawner.spawned();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], cmd);
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_records_multiple_in_order() {
        let spawner = FakeSpawner::new();
        let a = CommandSpec {
            program: "a".into(),
            args: vec![],
        };
        let b = CommandSpec {
            program: "b".into(),
            args: vec!["x".into()],
        };
        let _ = spawner.spawn(&a).await.unwrap();
        let _ = spawner.spawn(&b).await.unwrap();
        let recorded = spawner.spawned();
        assert_eq!(recorded, vec![a, b]);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_resolves_with_configured_exit_code_after_delay() {
        let mut h = FakeChildHandle::new(0, Duration::from_secs(1));
        let status = h.wait().await;
        assert_eq!(exit_code_of(status), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_resolves_with_nonzero_exit_code() {
        let mut h = FakeChildHandle::new(42, Duration::from_millis(500));
        let status = h.wait().await;
        assert_eq!(exit_code_of(status), Some(42));
    }

    #[tokio::test(start_paused = true)]
    async fn terminate_marks_terminated_and_resolves_wait_when_graceful() {
        let mut h =
            FakeChildHandle::new(0, Duration::from_secs(60)).with_terminate_delay(Duration::ZERO);
        assert!(!h.terminated());

        h.terminate(Duration::from_secs(5)).await.unwrap();
        assert!(h.terminated());
        assert!(!h.killed(), "graceful termination must NOT set killed");

        // wait() must resolve quickly because mark_exited fired.
        let status = h.wait().await;
        assert_eq!(exit_code_of(status), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn terminate_escalates_to_kill_when_grace_exceeded() {
        let mut h = FakeChildHandle::new(0, Duration::from_secs(600))
            .with_terminate_delay(Duration::from_secs(10));

        let grace = Duration::from_secs(2);
        h.terminate(grace).await.unwrap();

        assert!(h.terminated(), "terminate() must always set terminated");
        assert!(h.killed(), "exceeding grace must set killed");
    }

    #[tokio::test(start_paused = true)]
    async fn signal_records_kind() {
        let mut h = FakeChildHandle::new(0, Duration::from_secs(60));
        assert_eq!(h.last_signal(), None);

        h.signal(SignalKind::Interrupt).await.unwrap();
        assert_eq!(h.last_signal(), Some(SignalKind::Interrupt));

        // Subsequent signal overwrites — most recent wins.
        h.signal(SignalKind::Terminate).await.unwrap();
        assert_eq!(h.last_signal(), Some(SignalKind::Terminate));

        // Signal does NOT escalate or terminate.
        assert!(!h.terminated());
        assert!(!h.killed());
    }

    #[tokio::test(start_paused = true)]
    async fn pid_returns_synthetic_value() {
        let h = FakeChildHandle::new(0, Duration::ZERO);
        assert_eq!(h.pid(), FAKE_PID);
    }

    #[tokio::test(start_paused = true)]
    async fn enqueued_handle_used_by_spawn() {
        let spawner = FakeSpawner::new();
        spawner.enqueue_handle(FakeChildHandle::new(7, Duration::from_millis(100)));

        let cmd = CommandSpec {
            program: "x".into(),
            args: vec![],
        };
        let mut handle = spawner.spawn(&cmd).await.unwrap();
        let status = handle.wait().await;
        assert_eq!(exit_code_of(status), Some(7));
    }

    #[tokio::test]
    async fn send_sync_compile() {
        // Compile-time check.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FakeSpawner>();
        assert_send_sync::<FakeChildHandle>();

        let spawner = Arc::new(FakeSpawner::new());
        let s2 = spawner.clone();
        let cmd = CommandSpec {
            program: "p".into(),
            args: vec![],
        };
        tokio::spawn(async move {
            let _ = s2.spawn(&cmd).await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(spawner.spawned().len(), 1);
    }
}
