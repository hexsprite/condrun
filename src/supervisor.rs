//! Supervisor — pre-flight gate + child spawn + exit-code propagation.
//!
//! Wave 4 lays the foundation: evaluate the predicate set against current
//! [`NetworkState`] before spawning, then either short-circuit (predicates
//! fail) or hand off to the [`Spawner`] and wait for the child. Wave 5 will
//! extend the wait with a watcher loop (kill-on-change), Wave 6 with debounce
//! + SIGINT/SIGTERM forwarding. The struct + `run()` signature defined here
//! is FROZEN — later waves extend behaviour without changing the public API.
//!
//! Exit-code mapping (per SPEC §8.3):
//!   * pre-flight pass + child exits 0 → return 0
//!   * pre-flight pass + child exits N (N != 0) → return 2 (logs original N)
//!   * pre-flight fail + `!strict` → return 0 (silent skip)
//!   * pre-flight fail + `strict` → return 1 (logs failing predicate + reason)

use std::time::Duration;

use anyhow::Result;
use tracing::{debug, error, info};

use crate::predicate::{PredicateResult, PredicateSet};
use crate::process::{ChildHandle, CommandSpec, Spawner};
use crate::signal::{SignalKind, Signals};
use crate::state::NetworkState;

/// After forwarding the first signal, race child exit vs another signal.
/// A second signal escalates via [`ChildHandle::terminate`] (SIGTERM with
/// grace + SIGKILL fallback) so the user can recover from an unresponsive
/// child by hitting Ctrl+C twice.
async fn wait_with_escalation(
    child: &mut dyn ChildHandle,
    signals: &mut Box<dyn Signals>,
    grace: std::time::Duration,
) -> Result<i32> {
    tokio::select! {
        status = child.wait() => {
            Ok(if status.success() { 0 } else { 2 })
        }
        sig2 = signals.next() => {
            info!(?sig2, "child unresponsive, escalating via terminate(grace)");
            if let Err(err) = child.terminate(grace).await {
                error!(%err, "terminate failed during signal escalation");
            }
            // Drain wait so the OS reaps the process.
            let _ = child.wait().await;
            Ok(3)
        }
    }
}

/// Runtime configuration for [`Supervisor`].
///
/// All fields are populated up-front so the struct signature is stable across
/// waves. Wave 4 only consumes `strict`; later waves consume the rest.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// If pre-flight predicates fail and `strict` is true, exit with code 1
    /// instead of silently skipping with code 0.
    pub strict: bool,
    /// If true, the supervisor watches state mid-run and kills the child when
    /// predicates flip. (Wave 5 wires this; Wave 4 ignores it.)
    pub kill_on_change: bool,
    /// Grace period between SIGTERM and SIGKILL when killing on flip.
    pub grace: Duration,
    /// Polling interval for the state watcher.
    pub poll: Duration,
    /// Debounce window — how long predicates must stay failed before kill.
    pub debounce: Duration,
}

/// Conditional command runner. Owns its dependencies; consumed by [`Supervisor::run`].
///
/// The struct fields are FROZEN as of Wave 4 — Wave 5/6 extend behaviour in
/// `run()` without touching the public surface.
pub struct Supervisor {
    pub predicate_set: PredicateSet,
    pub spawner: Box<dyn Spawner>,
    pub state: Box<dyn NetworkState>,
    pub signals: Box<dyn Signals>,
    pub config: SupervisorConfig,
}

impl Supervisor {
    /// Run the conditional command. Returns the supervisor's exit code.
    ///
    /// Consumes `self` because it owns the spawner / signals / state.
    pub async fn run(self, cmd: &CommandSpec) -> Result<i32> {
        // Destructure so we can hold disjoint mutable borrows of the
        // signals stream alongside the spawned child handle.
        let Supervisor {
            predicate_set,
            spawner,
            state,
            mut signals,
            config,
        } = self;

        // Pre-flight: evaluate predicates against the current state snapshot.
        // Race evaluation against signal arrival: if a signal lands while we
        // are still gating (no child spawned yet), exit 0 immediately.
        let preflight = tokio::select! {
            res = predicate_set.evaluate(state.as_ref()) => res,
            sig = signals.next() => {
                info!(?sig, "signal received during pre-flight, exiting cleanly");
                return Ok(0);
            }
        };

        match preflight {
            PredicateResult::Pass => {
                // Fall through to spawn.
            }
            PredicateResult::Fail { reason } => {
                if config.strict {
                    error!(reason = %reason, "pre-flight predicate failed (strict): {reason}");
                    return Ok(1);
                } else {
                    info!(reason = %reason, "pre-flight predicate failed (non-strict), skipping");
                    return Ok(0);
                }
            }
        }

        // Spawn the child.
        let mut child = spawner.spawn(cmd).await?;

        // Wave 5: when `kill_on_change` is enabled, race child completion
        // against a periodic predicate re-evaluation. Otherwise fall back to
        // the Wave 4 behaviour (simple wait). Both branches race signal
        // arrival — Wave 6b forwards SIGINT/SIGTERM to the child without
        // SIGKILL escalation (raw passthrough only).
        let status = if config.kill_on_change {
            // Watcher loop: each tick of `poll`, re-evaluate predicates
            // against the live state. If they flip to Fail, terminate the
            // child (with `grace`) and return 3. Otherwise keep waiting.
            //
            // Borrow note: `ChildHandle::wait()` takes `&mut self`, so the
            // wait future borrows `child` exclusively. We can't call
            // `child.terminate()` while a wait future is alive. Instead, we
            // poll-tick in the outer loop and only call `child.wait()` for
            // a single tick window inside `select!`. When that arm fires
            // we get the status; when the timer arm fires we drop the wait
            // future (loop iteration ends) before reaching terminate.
            //
            // Debounce: on first observed Fail we record `fail_started_at`.
            // We only terminate once `(now - fail_started_at) >= debounce`,
            // so brief blips don't kill the child. Recovery to Pass clears
            // the timer (re-arms on next Fail). With `debounce == 0` the
            // elapsed check trivially passes on the first Fail observation,
            // matching Wave 5 immediate-kill behaviour.
            //
            // Use `tokio::time::Instant` (NOT `std::time::Instant`) so
            // paused-time tests advance the timer with `tokio::time::advance`.
            let mut fail_started_at: Option<tokio::time::Instant> = None;
            loop {
                let tick = tokio::time::sleep(config.poll);
                tokio::pin!(tick);

                enum WatchOutcome {
                    Exited(std::process::ExitStatus),
                    Tick,
                    Signal(SignalKind),
                }

                let outcome = tokio::select! {
                    biased;
                    // Child exited naturally during this poll window.
                    status = child.wait() => WatchOutcome::Exited(status),
                    // SIGINT/SIGTERM — forward to child, then wait for
                    // natural exit. No SIGKILL escalation on this path.
                    sig = signals.next() => WatchOutcome::Signal(sig),
                    // Poll tick — re-evaluate predicates.
                    _ = &mut tick => WatchOutcome::Tick,
                };

                match outcome {
                    WatchOutcome::Exited(status) => break status,
                    WatchOutcome::Signal(sig) => {
                        info!(?sig, "forwarding signal to child");
                        if let Err(err) = child.signal(sig).await {
                            error!(%err, "failed to forward signal to child");
                        }
                        // Race child exit vs another signal. If the user
                        // hits Ctrl+C twice (or sends a different signal)
                        // because the child is unresponsive, escalate to
                        // terminate(grace) which does SIGTERM → grace →
                        // SIGKILL on the child process group.
                        return wait_with_escalation(&mut *child, &mut signals, config.grace).await;
                    }
                    WatchOutcome::Tick => {
                        // fall through to predicate re-evaluation
                    }
                }

                // Re-evaluate predicates. Only the wait future borrowed
                // `child`; it has been dropped, so `child` is free now.
                match predicate_set.evaluate(state.as_ref()).await {
                    PredicateResult::Pass => {
                        // Predicates still pass — clear any pending debounce
                        // timer (recovery resets) and keep watching.
                        if fail_started_at.is_some() {
                            debug!("predicate recovered, resetting debounce timer");
                            fail_started_at = None;
                        }
                        continue;
                    }
                    PredicateResult::Fail { reason } => {
                        let now = tokio::time::Instant::now();
                        let started = *fail_started_at.get_or_insert(now);
                        let elapsed = now.saturating_duration_since(started);
                        if elapsed >= config.debounce {
                            info!(
                                reason = %reason,
                                debounce_ms = config.debounce.as_millis() as u64,
                                elapsed_ms = elapsed.as_millis() as u64,
                                "predicate flipped to fail mid-run, terminating child"
                            );
                            child.terminate(config.grace).await?;
                            // Drain wait so the OS reaps the process.
                            let _ = child.wait().await;
                            return Ok(3);
                        } else {
                            debug!(
                                reason = %reason,
                                debounce_ms = config.debounce.as_millis() as u64,
                                elapsed_ms = elapsed.as_millis() as u64,
                                "predicate failing within debounce window, waiting"
                            );
                            continue;
                        }
                    }
                }
            }
        } else {
            // Wave 4 path: simple wait, no watcher. Still race signals so
            // SIGINT/SIGTERM are forwarded to the child even without
            // kill-on-change.
            tokio::select! {
                status = child.wait() => status,
                sig = signals.next() => {
                    info!(?sig, "forwarding signal to child");
                    if let Err(err) = child.signal(sig).await {
                        error!(%err, "failed to forward signal to child");
                    }
                    return wait_with_escalation(&mut *child, &mut signals, config.grace).await;
                }
            }
        };

        // Map child exit status → supervisor exit code per SPEC §8.3.
        if status.success() {
            Ok(0)
        } else {
            let original = status.code().unwrap_or(-1);
            error!(child_code = original, "child exited with code {original}");
            Ok(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::process::ExitStatus;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::predicate::Predicate;
    use crate::predicate::metered::RejectExpensive;
    use crate::process::fake::{FakeChildHandle, FakeSpawner};
    use crate::process::{ChildHandle, CommandSpec, Spawner};
    use crate::signal::fake::FakeSignals;
    use crate::signal::SignalKind;
    use crate::state::fake::FakeNetworkState;

    // --------------------------------------------------------------------
    // Test helpers
    // --------------------------------------------------------------------

    /// Wrapper around `Arc<FakeSpawner>` so the test can keep a handle for
    /// inspection while the supervisor consumes the trait object.
    struct SharedSpawner(Arc<FakeSpawner>);

    #[async_trait::async_trait]
    impl Spawner for SharedSpawner {
        async fn spawn(&self, cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>> {
            self.0.spawn(cmd).await
        }
    }

    fn shared_spawner() -> (Arc<FakeSpawner>, Box<dyn Spawner>) {
        let spawner = Arc::new(FakeSpawner::new());
        let handle: Box<dyn Spawner> = Box::new(SharedSpawner(spawner.clone()));
        (spawner, handle)
    }

    // --------------------------------------------------------------------
    // Inspectable child handle — for Wave 5 watcher tests.
    //
    // `FakeChildHandle` exposes `terminated()` / `killed()`, but is consumed
    // when handed to the supervisor. `InspectableHandle` re-implements the
    // FakeChildHandle behaviour over an `Arc<Mutex<...>>`, so the test can
    // hold an inspection clone after the handle is moved into the spawner.
    // --------------------------------------------------------------------

    #[derive(Debug, Default, Clone)]
    struct InspectState {
        terminated: bool,
        killed: bool,
        last_signal: Option<SignalKind>,
        exited: bool,
    }

    #[derive(Clone)]
    struct InspectInner {
        exit_code: i32,
        exit_delay: Duration,
        terminate_delay: Duration,
        state: Arc<Mutex<InspectState>>,
    }

    /// Inspector handle the test keeps after the child handle is consumed.
    #[derive(Clone)]
    struct ChildInspector {
        state: Arc<Mutex<InspectState>>,
    }

    impl ChildInspector {
        fn terminated(&self) -> bool {
            self.state.lock().unwrap().terminated
        }
        fn killed(&self) -> bool {
            self.state.lock().unwrap().killed
        }
        fn last_signal(&self) -> Option<SignalKind> {
            self.state.lock().unwrap().last_signal
        }
    }

    struct InspectableHandle(InspectInner);

    impl InspectableHandle {
        fn new(exit_code: i32, exit_delay: Duration) -> (Self, ChildInspector) {
            let state = Arc::new(Mutex::new(InspectState::default()));
            let inner = InspectInner {
                exit_code,
                exit_delay,
                terminate_delay: Duration::ZERO,
                state: state.clone(),
            };
            (Self(inner), ChildInspector { state })
        }

        fn with_terminate_delay(mut self, d: Duration) -> Self {
            self.0.terminate_delay = d;
            self
        }
    }

    fn make_status(code: i32) -> ExitStatus {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            ExitStatus::from_raw(code << 8)
        }
        #[cfg(not(unix))]
        {
            use std::os::windows::process::ExitStatusExt;
            ExitStatus::from_raw(code as u32)
        }
    }

    #[async_trait]
    impl ChildHandle for InspectableHandle {
        async fn wait(&mut self) -> ExitStatus {
            // Already exited (e.g. terminate already ran)?
            if self.0.state.lock().unwrap().exited {
                return make_status(self.0.exit_code);
            }
            tokio::time::sleep(self.0.exit_delay).await;
            self.0.state.lock().unwrap().exited = true;
            make_status(self.0.exit_code)
        }

        async fn terminate(&mut self, grace: Duration) -> Result<()> {
            self.0.state.lock().unwrap().terminated = true;
            if self.0.terminate_delay <= grace {
                tokio::time::sleep(self.0.terminate_delay).await;
            } else {
                tokio::time::sleep(grace).await;
                self.0.state.lock().unwrap().killed = true;
            }
            self.0.state.lock().unwrap().exited = true;
            Ok(())
        }

        async fn signal(&mut self, kind: SignalKind) -> Result<()> {
            self.0.state.lock().unwrap().last_signal = Some(kind);
            Ok(())
        }

        fn pid(&self) -> u32 {
            12_345
        }
    }

    /// Spawner that yields a single pre-built [`InspectableHandle`].
    struct OneShotSpawner {
        handle: Mutex<Option<InspectableHandle>>,
    }

    impl OneShotSpawner {
        fn new(handle: InspectableHandle) -> Self {
            Self { handle: Mutex::new(Some(handle)) }
        }
    }

    #[async_trait]
    impl Spawner for OneShotSpawner {
        async fn spawn(&self, _cmd: &CommandSpec) -> Result<Box<dyn ChildHandle>> {
            let h = self.handle.lock().unwrap().take().expect("OneShotSpawner already consumed");
            Ok(Box::new(h))
        }
    }

    // --------------------------------------------------------------------
    // Counting predicate wrapper — records how many times `evaluate` ran.
    // --------------------------------------------------------------------

    struct CountingRejectExpensive {
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Predicate for CountingRejectExpensive {
        fn name(&self) -> &str {
            "counting-reject-expensive"
        }
        async fn evaluate(
            &self,
            state: &dyn crate::state::NetworkState,
        ) -> PredicateResult {
            self.counter.fetch_add(1, Ordering::SeqCst);
            RejectExpensive.evaluate(state).await
        }
    }

    fn cfg(strict: bool, kill_on_change: bool) -> SupervisorConfig {
        SupervisorConfig {
            strict,
            kill_on_change,
            grace: Duration::ZERO,
            poll: Duration::ZERO,
            debounce: Duration::ZERO,
        }
    }

    fn echo_cmd() -> CommandSpec {
        CommandSpec {
            program: "echo".into(),
            args: vec!["hello".into()],
        }
    }

    // --------------------------------------------------------------------
    // SPEC §8.3 scenarios
    // --------------------------------------------------------------------

    /// Scenario 1: pre-flight pass + child exits 0 → supervisor returns 0.
    #[tokio::test(start_paused = true)]
    async fn scenario_1_happy_path() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (spawner_handle, spawner) = shared_spawner();
        spawner_handle.enqueue_handle(FakeChildHandle::new(0, Duration::ZERO));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(false, false),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 0);
        assert_eq!(spawner_handle.spawned().len(), 1);
    }

    /// Scenario 2: pre-flight pass + child exits 7 → supervisor returns 2.
    #[tokio::test(start_paused = true)]
    async fn scenario_2_child_nonzero() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (spawner_handle, spawner) = shared_spawner();
        spawner_handle.enqueue_handle(FakeChildHandle::new(7, Duration::ZERO));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(false, false),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 2);
        assert_eq!(spawner_handle.spawned().len(), 1);
    }

    /// Scenario 3: pre-flight fail + non-strict → no spawn, supervisor returns 0.
    #[tokio::test(start_paused = true)]
    async fn scenario_3_preflight_fail_silent() {
        let state = FakeNetworkState::new();
        state.set_expensive(true);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (spawner_handle, spawner) = shared_spawner();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(false, false),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 0);
        assert!(
            spawner_handle.spawned().is_empty(),
            "child must not be spawned when pre-flight fails"
        );
    }

    /// Scenario 4: pre-flight fail + strict → no spawn, supervisor returns 1.
    #[tokio::test(start_paused = true)]
    async fn scenario_4_preflight_fail_strict() {
        let state = FakeNetworkState::new();
        state.set_expensive(true);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (spawner_handle, spawner) = shared_spawner();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(true, false),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 1);
        assert!(
            spawner_handle.spawned().is_empty(),
            "child must not be spawned when pre-flight fails"
        );
    }

    /// Acceptance #5: log message on pre-flight fail mentions the failing
    /// predicate's reason. We capture tracing events by installing a
    /// process-local subscriber that records into a buffer.
    ///
    /// Note: `tracing` allows only one global subscriber per process. To keep
    /// this test isolated, we use `tracing::subscriber::with_default`, which
    /// scopes a subscriber to the current task only.
    #[tokio::test(start_paused = true)]
    async fn preflight_fail_log_message() {
        use std::sync::Mutex;
        use tracing::subscriber;
        use tracing_subscriber::fmt::MakeWriter;

        // A `MakeWriter` that appends every formatted log line into a shared buffer.
        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);

        impl<'a> MakeWriter<'a> for BufWriter {
            type Writer = BufHandle;
            fn make_writer(&'a self) -> Self::Writer {
                BufHandle(self.0.clone())
            }
        }

        struct BufHandle(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for BufHandle {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer = BufWriter(buf.clone());

        let collector = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_max_level(tracing::Level::ERROR)
            .with_ansi(false)
            .finish();

        let state = FakeNetworkState::new();
        state.set_expensive(true);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);
        let (_spawner_handle, spawner) = shared_spawner();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(true, false),
        };

        // `set_default` returns a guard that scopes the subscriber to the
        // current thread for as long as it lives. Because tokio's current-
        // thread runtime parks the test on the same thread, events emitted
        // inside `.await` are routed to our buffer.
        let _guard = subscriber::set_default(collector);
        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 1);

        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("expensive"),
            "log must mention 'expensive', got: {logs}"
        );
    }

    /// Acceptance #6: with `kill_on_change: false`, the supervisor must NOT
    /// kill the child even if state flips mid-run. Wave 4 trivially satisfies
    /// this (no watcher); the test guards against future regression when the
    /// watcher lands in Wave 5.
    #[tokio::test(start_paused = true)]
    async fn no_kill_on_change() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        // Keep a clone so the test can flip state mid-run.
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (spawner_handle, spawner) = shared_spawner();
        // Child takes 10s to exit (paused-time virtual seconds).
        spawner_handle.enqueue_handle(FakeChildHandle::new(0, Duration::from_secs(10)));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: cfg(false, false), // kill_on_change = false
        };

        let cmd = echo_cmd();
        let run_fut = supervisor.run(&cmd);
        tokio::pin!(run_fut);

        // Flip state mid-run after a virtual 5s — supervisor must ignore it.
        let flipper = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            state_writer.set_expensive(true);
        };

        let (_, code) = tokio::join!(flipper, run_fut);
        let code = code.unwrap();
        assert_eq!(code, 0, "child should run to completion (exit 0)");
    }

    // --------------------------------------------------------------------
    // Wave 5: watcher loop + kill-on-change (SPEC §8.3 scenarios 5, 6, 11).
    // --------------------------------------------------------------------

    fn watcher_cfg(grace: Duration, poll: Duration) -> SupervisorConfig {
        SupervisorConfig {
            strict: false,
            kill_on_change: true,
            grace,
            poll,
            debounce: Duration::ZERO,
        }
    }

    /// Scenario 5: state flips mid-run → SIGTERM → graceful exit → return 3.
    #[tokio::test(start_paused = true)]
    async fn scenario_5_kill_on_change() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Child runs for a virtual minute; predicate flip should kill it long
        // before that.
        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(60));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: watcher_cfg(Duration::from_secs(5), Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // First poll fires at t=2s; predicates still pass.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!inspector.terminated(), "child must not be killed before flip");

        // Flip state — next poll (t≈4s) should see Fail and terminate.
        state_writer.set_expensive(true);

        let code = run.await.unwrap();
        assert_eq!(code, 3, "predicate flip → exit code 3");
        assert!(inspector.terminated(), "child.terminate() must have been called");
        assert!(!inspector.killed(), "graceful terminate must NOT escalate to SIGKILL");
    }

    /// Scenario 6: child ignores SIGTERM longer than grace → SIGKILL → return 3.
    #[tokio::test(start_paused = true)]
    async fn scenario_6_sigterm_ignored_then_sigkill() {
        let state = FakeNetworkState::new();
        state.set_expensive(false); // Pre-flight passes.
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let grace = Duration::from_secs(2);
        // Child ignores SIGTERM for 10s — well past grace.
        let (handle, inspector) = InspectableHandle::new(0, Duration::from_secs(600));
        let handle = handle.with_terminate_delay(Duration::from_secs(10));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: watcher_cfg(grace, Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // Let pre-flight pass + first poll fire (t=2s). Then flip state so
        // the next poll (t=4s) catches the change and starts terminate().
        tokio::time::sleep(Duration::from_secs(3)).await;
        state_writer.set_expensive(true);

        let code = run.await.unwrap();
        assert_eq!(code, 3);
        assert!(inspector.terminated(), "terminate() must always set terminated");
        assert!(
            inspector.killed(),
            "exceeding grace must escalate to SIGKILL (killed=true)"
        );
    }

    /// Acceptance #3: child exits naturally during watch → return 0, no kill.
    #[tokio::test(start_paused = true)]
    async fn child_exits_naturally_during_watch() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Child exits at t=2s; first poll wouldn't fire until t=5s.
        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(2));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: watcher_cfg(Duration::from_secs(1), Duration::from_secs(5)),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 0, "natural success exit must propagate as 0");
        assert!(!inspector.terminated(), "supervisor must not terminate a healthy child");
        assert!(!inspector.killed());
    }

    /// SPEC §8.3 scenario 11: poll interval is respected. With poll=5s and
    /// state stable for 12s, the watcher must evaluate the predicate roughly
    /// 2-3 times — not on every yield of the runtime.
    #[tokio::test(start_paused = true)]
    async fn poll_interval_respected() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let counter = Arc::new(AtomicUsize::new(0));
        let predicates = PredicateSet::and(vec![Box::new(CountingRejectExpensive {
            counter: counter.clone(),
        })]);
        let preflight_count = 1; // run() always evaluates once before spawn.

        // Child outlives the test window so only the watcher loop drives counts.
        let (handle, _inspector) =
            InspectableHandle::new(0, Duration::from_secs(600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: watcher_cfg(Duration::from_secs(1), Duration::from_secs(5)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await });

        // Advance virtual time by 12s. Polls at 5s, 10s — count=2 watcher
        // ticks plus the pre-flight evaluation = 3 total.
        tokio::time::sleep(Duration::from_secs(12)).await;
        let count = counter.load(Ordering::SeqCst);
        run.abort();
        let _ = run.await;

        let watcher_calls = count.saturating_sub(preflight_count);
        assert!(
            (2..=3).contains(&watcher_calls),
            "watcher must evaluate predicate 2-3 times in 12s with poll=5s, got {watcher_calls} (total {count})",
        );
    }

    // --------------------------------------------------------------------
    // Wave 6a: Supervisor debounce + wall-clock timer (cr-yqf.4.3).
    //
    // Debounce delays kill on predicate flip until the predicate has been
    // failing continuously for `config.debounce`. Recovery (back to Pass)
    // resets the timer. `debounce == 0` is the Wave 5 backward-compatible
    // path: first observed Fail terminates immediately.
    // --------------------------------------------------------------------

    fn debounce_cfg(debounce: Duration, poll: Duration) -> SupervisorConfig {
        SupervisorConfig {
            strict: false,
            kill_on_change: true,
            grace: Duration::from_secs(1),
            poll,
            debounce,
        }
    }

    /// debounce=0 → first poll observing Fail terminates immediately.
    /// Acceptance #1 + Wave 5 backward-compat: debounce=0 preserves the
    /// kill-on-first-flip behaviour exercised by scenario_5.
    #[tokio::test(start_paused = true)]
    async fn debounce_zero_immediate_kill() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: debounce_cfg(Duration::ZERO, Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // First poll fires at t=2s (Pass). Flip at t=3s; next poll at t=4s
        // observes Fail with elapsed=0, debounce=0 → terminates immediately.
        tokio::time::sleep(Duration::from_secs(3)).await;
        state_writer.set_expensive(true);

        let code = run.await.unwrap();
        assert_eq!(code, 3, "debounce=0 → kill on first observed fail");
        assert!(inspector.terminated());
    }

    /// debounce=10s → predicate must fail continuously for full window.
    /// Acceptance #2: flip at t=4s, polls at t=6,8,10,12 must NOT terminate;
    /// poll at t=14s (elapsed >= 10s from first observed fail) terminates.
    #[tokio::test(start_paused = true)]
    async fn debounce_waits_full_duration() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: debounce_cfg(Duration::from_secs(10), Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // Flip just before the t=4s poll so it observes Fail and starts the
        // debounce timer at ~t=4s.
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        state_writer.set_expensive(true);

        // Advance to t=12s — polls at 4, 6, 8, 10, 12. From t=4 the elapsed
        // is at most 8s, still < 10s debounce → must NOT terminate yet.
        tokio::time::sleep(Duration::from_millis(8_500)).await; // now ~t=12s
        assert!(
            !inspector.terminated(),
            "must not terminate before debounce window elapses (t=12s, elapsed=8s, debounce=10s)"
        );

        // Continue: poll at t=14s sees elapsed=10s ≥ 10s → terminate.
        let code = run.await.unwrap();
        assert_eq!(code, 3, "debounce window expired → kill");
        assert!(inspector.terminated());
    }

    /// debounce=10s, predicate recovers before window expires → no kill.
    /// Acceptance #3: fail at t=4s, recover at t=8s. Timer must reset; even
    /// after advancing to t=20s the supervisor must NOT have terminated.
    #[tokio::test(start_paused = true)]
    async fn debounce_resets_on_recovery() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: debounce_cfg(Duration::from_secs(10), Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // Fail at ~t=3.5s (caught by t=4s poll), recover at ~t=7.5s
        // (caught by t=8s poll — clears fail_started_at).
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        state_writer.set_expensive(true);
        tokio::time::sleep(Duration::from_secs(4)).await; // t=7.5s
        state_writer.set_expensive(false);

        // Advance to t=20s — far past 10s from original fail, but timer was
        // reset on recovery so no kill.
        tokio::time::sleep(Duration::from_millis(12_500)).await; // t=20s
        assert!(
            !inspector.terminated(),
            "recovery must reset debounce timer; supervisor must not terminate"
        );

        run.abort();
        let _ = run.await;
    }

    /// Recovery resets the timer, then a fresh fail must wait the FULL
    /// debounce window from the new fail-start.
    /// Acceptance #4: fail t=4, recover t=8, fail t=12. At t=20 only 8s
    /// elapsed since re-fail → no kill. At t=22+ elapsed >= 10s → kill.
    #[tokio::test(start_paused = true)]
    async fn debounce_resets_then_retrips() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);
        let state_writer = state.clone();

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(FakeSignals::new()),
            config: debounce_cfg(Duration::from_secs(10), Duration::from_secs(2)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // Fail at ~t=3.5s — caught by t=4s poll (fail_started_at = ~4s).
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        state_writer.set_expensive(true);

        // Recover at ~t=7.5s — caught by t=8s poll (timer cleared).
        tokio::time::sleep(Duration::from_secs(4)).await; // t=7.5s
        state_writer.set_expensive(false);

        // Fail again at ~t=11.5s — caught by t=12s poll (new
        // fail_started_at = ~12s). Window now restarts.
        tokio::time::sleep(Duration::from_secs(4)).await; // t=11.5s
        state_writer.set_expensive(true);

        // Advance to t=20s. From new fail-start at ~12s, only 8s elapsed
        // (poll at t=20s: elapsed=8s, debounce=10s → no kill yet).
        tokio::time::sleep(Duration::from_millis(8_500)).await; // t=20s
        assert!(
            !inspector.terminated(),
            "re-fail timer must restart from zero; only 8s elapsed at t=20s"
        );

        // Run on — at t=22s elapsed=10s ≥ 10s → terminate.
        let code = run.await.unwrap();
        assert_eq!(code, 3);
        assert!(inspector.terminated());
    }

    // --------------------------------------------------------------------
    // Wave 6b: SIGINT/SIGTERM forwarding (cr-yqf.4.4).
    //
    // The supervisor races `signals.next()` against child completion, the
    // poll tick, and (for pre-flight) predicate evaluation. On signal:
    // forward via `child.signal(kind)` (raw, no escalation) and wait for
    // natural exit. Map exit code per existing rules (0 → 0, else → 2).
    // --------------------------------------------------------------------

    /// SIGINT forwarded to running child, then exit code maps from natural exit.
    /// Acceptance #1, #3 — covers SPEC §8.3 scenario 12.
    #[tokio::test(start_paused = true)]
    async fn sigint_forwarded_to_child() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Child takes 60s of virtual time to exit; will exit cleanly with 0.
        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(60));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let signals = FakeSignals::new();
        let injector = signals.clone();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(signals),
            // Use simple-wait branch (kill_on_change=false) to verify the
            // signal-racing arm there too.
            config: cfg(false, false),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // Give the supervisor a tick to reach the wait+select arm.
        tokio::time::sleep(Duration::from_millis(10)).await;
        injector.inject(SignalKind::Interrupt);

        let code = run.await.unwrap();
        assert_eq!(
            inspector.last_signal(),
            Some(SignalKind::Interrupt),
            "SIGINT must be forwarded to child"
        );
        assert_eq!(code, 0, "child exited 0 → supervisor returns 0");
        assert!(!inspector.terminated(), "signal-forwarding path must NOT call terminate()");
        assert!(!inspector.killed(), "signal-forwarding path must NOT escalate to SIGKILL");
    }

    /// SIGTERM forwarded to running child. Acceptance #2.
    #[tokio::test(start_paused = true)]
    async fn sigterm_forwarded_to_child() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Child exits with non-zero → supervisor must map to 2.
        let (handle, inspector) =
            InspectableHandle::new(7, Duration::from_secs(60));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let signals = FakeSignals::new();
        let injector = signals.clone();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(signals),
            // Watcher branch — verify signal arm there too.
            config: watcher_cfg(Duration::from_secs(1), Duration::from_secs(5)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(10)).await;
        injector.inject(SignalKind::Terminate);

        let code = run.await.unwrap();
        assert_eq!(
            inspector.last_signal(),
            Some(SignalKind::Terminate),
            "SIGTERM must be forwarded to child"
        );
        assert_eq!(code, 2, "child exited 7 → supervisor maps to 2");
        assert!(!inspector.terminated(), "raw forward must not invoke terminate()");
    }

    // ------------------------------------------------------------------
    // Custom slow predicate for the pre-flight signal-race test. Sleeps
    // for `delay` before returning Pass. The supervisor's pre-flight
    // wraps `evaluate()` in `tokio::select!` against `signals.next()`,
    // so a signal injected before evaluation completes must short-circuit
    // the supervisor with exit code 0.
    // ------------------------------------------------------------------
    struct SlowPassPredicate {
        delay: Duration,
    }

    #[async_trait]
    impl Predicate for SlowPassPredicate {
        fn name(&self) -> &str {
            "slow-pass"
        }
        async fn evaluate(
            &self,
            _state: &dyn crate::state::NetworkState,
        ) -> PredicateResult {
            tokio::time::sleep(self.delay).await;
            PredicateResult::Pass
        }
    }

    /// Signal during pre-flight evaluation → exit 0, child never spawned.
    /// Acceptance #5.
    #[tokio::test(start_paused = true)]
    async fn signal_during_preflight() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        // Predicate takes a virtual hour to evaluate. Signal must win.
        let predicates = PredicateSet::and(vec![Box::new(SlowPassPredicate {
            delay: Duration::from_secs(3600),
        })]);

        let (spawner_handle, spawner) = shared_spawner();

        let signals = FakeSignals::new();
        // Inject before run() so the signal is immediately ready when the
        // pre-flight `select!` polls; the slow predicate is still pending.
        signals.inject(SignalKind::Interrupt);

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(signals),
            config: cfg(false, false),
        };

        let code = supervisor.run(&echo_cmd()).await.unwrap();
        assert_eq!(code, 0, "signal during pre-flight → clean exit 0");
        assert!(
            spawner_handle.spawned().is_empty(),
            "no child must be spawned when signal interrupts pre-flight"
        );
    }

    /// Regression test for cr-14a: a second signal during the wait-for-child
    /// phase MUST escalate via terminate(grace). Without this, an unresponsive
    /// child (e.g. claude swallowing first SIGINT) leaves condrun hung.
    #[tokio::test(start_paused = true)]
    async fn second_signal_escalates_to_terminate() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Child takes a virtual hour to exit on its own. Without escalation,
        // a single forwarded signal would block the supervisor for that long.
        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(3600));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let signals = FakeSignals::new();
        let injector = signals.clone();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(signals),
            // Use simple-wait branch; the escalation path is shared.
            config: cfg(false, false),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        // First signal: supervisor forwards via child.signal(...).
        tokio::time::sleep(Duration::from_millis(10)).await;
        injector.inject(SignalKind::Interrupt);
        // Give the supervisor a tick to enter the wait-with-escalation arm.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Second signal: supervisor must escalate via terminate(grace).
        injector.inject(SignalKind::Interrupt);

        let code = run.await.unwrap();
        assert_eq!(code, 3, "second signal must escalate → exit 3");
        assert_eq!(
            inspector.last_signal(),
            Some(SignalKind::Interrupt),
            "first signal still forwarded as raw signal()"
        );
        assert!(
            inspector.terminated(),
            "second signal must trigger terminate() escalation"
        );
    }

    /// Regression test for cr-8o7: SIGQUIT and SIGHUP must be forwardable
    /// through the supervisor (FakeSignals injection covers the wiring).
    #[tokio::test(start_paused = true)]
    async fn sigquit_and_sighup_forwarded() {
        for kind in [SignalKind::Quit, SignalKind::Hup] {
            let state = FakeNetworkState::new();
            state.set_expensive(false);

            let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

            let (handle, inspector) =
                InspectableHandle::new(0, Duration::from_secs(60));
            let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

            let signals = FakeSignals::new();
            let injector = signals.clone();

            let supervisor = Supervisor {
                predicate_set: predicates,
                spawner,
                state: Box::new(state),
                signals: Box::new(signals),
                config: cfg(false, false),
            };

            let cmd = echo_cmd();
            let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

            tokio::time::sleep(Duration::from_millis(10)).await;
            injector.inject(kind);

            let code = run.await.unwrap();
            assert_eq!(
                inspector.last_signal(),
                Some(kind),
                "{kind:?} must be forwarded to child via signal()"
            );
            assert_eq!(code, 0, "child exited 0 → supervisor returns 0");
        }
    }

    /// Signal-forwarding path does NOT escalate to SIGKILL even if the child
    /// would exceed grace. The forwarded signal is recorded; supervisor
    /// waits for natural exit; never calls terminate(). Acceptance #4.
    #[tokio::test(start_paused = true)]
    async fn signal_no_escalation() {
        let state = FakeNetworkState::new();
        state.set_expensive(false);

        let predicates = PredicateSet::and(vec![Box::new(RejectExpensive)]);

        // Configure terminate_delay > grace — IF terminate() were called
        // the child would be SIGKILLed. We assert that never happens.
        let grace = Duration::from_secs(2);
        let (handle, inspector) =
            InspectableHandle::new(0, Duration::from_secs(30));
        let handle = handle.with_terminate_delay(grace + Duration::from_secs(1));
        let spawner: Box<dyn Spawner> = Box::new(OneShotSpawner::new(handle));

        let signals = FakeSignals::new();
        let injector = signals.clone();

        let supervisor = Supervisor {
            predicate_set: predicates,
            spawner,
            state: Box::new(state),
            signals: Box::new(signals),
            config: watcher_cfg(grace, Duration::from_secs(5)),
        };

        let cmd = echo_cmd();
        let run = tokio::spawn(async move { supervisor.run(&cmd).await.unwrap() });

        tokio::time::sleep(Duration::from_millis(10)).await;
        injector.inject(SignalKind::Interrupt);

        let code = run.await.unwrap();
        assert_eq!(
            inspector.last_signal(),
            Some(SignalKind::Interrupt),
            "signal must be recorded as forwarded"
        );
        assert!(
            !inspector.terminated(),
            "signal path must NOT call terminate() (no SIGTERM-then-SIGKILL escalation)"
        );
        assert!(
            !inspector.killed(),
            "signal path must NEVER reach the SIGKILL escalation branch"
        );
        assert_eq!(code, 0, "child exited 0 naturally → supervisor returns 0");
    }
}
