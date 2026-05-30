//! macOS implementation of [`NetworkState`] backed by `NWPathMonitor`
//! from `Network.framework`.
//!
//! Architecture:
//!
//! `NWPathMonitor` is a C-based push API. It calls an update handler block
//! every time the system path changes. We pin a closure on the heap via
//! [`block2::RcBlock`] (NOT [`block2::StackBlock`] — Network.framework
//! retains the block via `Block_copy` and invokes it long after `set_update_handler`
//! returns), and that closure writes the latest [`NwPathSnapshot`] into a shared
//! `Arc<Mutex<...>>`.
//!
//! The supervisor in v0.1 polls (`is_expensive` / `is_low_data_mode`), so the
//! cached snapshot is the bridge between push (Network.framework) and pull
//! (supervisor loop). v0.2 may wire the update handler directly into the
//! supervisor's signal channel without changing the trait surface.
//!
//! # Memory management
//!
//! NW objects (`nw_path_monitor_t`, `nw_path_t`) and dispatch objects
//! (`dispatch_queue_t`) are reference-counted opaque types. In Obj-C with ARC,
//! release is automatic; from raw `extern "C"` Rust we participate in neither
//! ARC nor the OS_OBJECT autorelease integration, so [`Drop`] must call
//! `nw_release` / `dispatch_release` explicitly.
//!
//! # Drop / cancel race
//!
//! `nw_path_monitor_cancel` is asynchronous: any blocks already enqueued on
//! the dispatch queue may still run after `cancel` returns. This is safe
//! because the update handler captures the `Arc<Mutex<NwPathSnapshot>>`, not
//! a borrow of `MacOsNetworkState`. When the struct is dropped, those pending
//! blocks finish writing into the now-orphaned snapshot — wasted work, but
//! never use-after-free. Drop sequence is:
//!
//! 1. `nw_path_monitor_cancel(monitor)` — stop scheduling new callbacks
//! 2. `nw_release(monitor)` — drop our +1 ref on the monitor
//! 3. `dispatch_release(queue)` — drop our +1 ref on the queue
//!
//! Cancel must come before release so the framework can fire any final
//! "cancel" notifications while the monitor object still exists.

use std::ffi::c_void;
use std::ptr::null_mut;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use block2::RcBlock;

use super::NetworkState;

/// Bounded wait for the first NWPathMonitor update after `start`. Real
/// callbacks usually land within tens of ms; we cap the wait so a hung
/// framework doesn't deadlock construction.
const FIRST_UPDATE_TIMEOUT: Duration = Duration::from_millis(1000);

// ---------------------------------------------------------------------------
// FFI
// ---------------------------------------------------------------------------

#[link(name = "Network", kind = "framework")]
unsafe extern "C" {
    /// Returns a `nw_path_monitor_t` with +1 reference count. Caller releases
    /// via `nw_release`.
    fn nw_path_monitor_create() -> *mut c_void;

    /// Block is `void (^)(nw_path_t)`. The framework retains the block
    /// (via `Block_copy`) — pass an `RcBlock`, not a `StackBlock`.
    fn nw_path_monitor_set_update_handler(monitor: *mut c_void, handler: *mut c_void);

    /// Sets the dispatch queue on which the update handler is invoked.
    fn nw_path_monitor_set_queue(monitor: *mut c_void, queue: *mut c_void);

    /// Begin monitoring; the update handler will be called with the current
    /// path shortly after this returns.
    fn nw_path_monitor_start(monitor: *mut c_void);

    /// Stop monitoring. Async — pending blocks may still fire afterward.
    fn nw_path_monitor_cancel(monitor: *mut c_void);

    /// True if the path uses an interface flagged as expensive (cellular,
    /// Personal Hotspot).
    fn nw_path_is_expensive(path: *mut c_void) -> bool;

    /// True if the path is constrained (Low Data Mode).
    fn nw_path_is_constrained(path: *mut c_void) -> bool;

    /// Decrement the reference count of an NW object.
    fn nw_release(obj: *mut c_void);
}

#[link(name = "System", kind = "framework")]
unsafe extern "C" {
    /// Create a serial dispatch queue. `attr = NULL` means serial.
    fn dispatch_queue_create(label: *const i8, attr: *mut c_void) -> *mut c_void;

    /// Decrement the reference count of a dispatch object.
    fn dispatch_release(obj: *mut c_void);
}

// ---------------------------------------------------------------------------
// Cached snapshot
// ---------------------------------------------------------------------------

/// Mirrors the trait surface — what the update handler writes and the
/// trait methods read. Cheap to clone (`Copy`) so we don't hold the lock
/// across `await`.
#[derive(Debug, Clone, Copy, Default)]
struct NwPathSnapshot {
    expensive: bool,
    low_data: bool,
}

/// Mutex contents: latest snapshot plus a `ready` flag set true by the
/// first update handler invocation. `new()` waits on the paired Condvar
/// for `ready` to flip before returning, so `is_expensive` /
/// `is_low_data_mode` never see uninitialized defaults.
#[derive(Debug, Default)]
struct SharedState {
    snapshot: NwPathSnapshot,
    ready: bool,
}

// ---------------------------------------------------------------------------
// MacOsNetworkState
// ---------------------------------------------------------------------------

/// `NetworkState` backed by `NWPathMonitor`.
pub struct MacOsNetworkState {
    /// Shared with the update handler block. The block holds an `Arc` clone;
    /// when we drop the struct, the block's clone keeps the snapshot alive
    /// until any in-flight callback finishes. The Condvar is signalled by
    /// every update — `new()` waits on it for the first-update barrier;
    /// after that nothing waits, so `notify_all()` is a cheap no-op.
    shared: Arc<(Mutex<SharedState>, Condvar)>,

    /// `nw_path_monitor_t` — we own +1 ref, released in `Drop`.
    monitor: *mut c_void,

    /// Dedicated serial `dispatch_queue_t` for update-handler delivery.
    /// Owned (+1 ref), released in `Drop`.
    queue: *mut c_void,

    /// Keep the heap block alive for the struct's lifetime. Network.framework
    /// also retains its own +1 via `Block_copy`, but holding our handle here
    /// avoids relying on framework internals.
    _handler: RcBlock<dyn Fn(*mut c_void)>,
}

// SAFETY: The underlying NW objects (`nw_path_monitor_t`) and dispatch
// objects are documented as thread-safe (any thread may call
// `nw_path_monitor_cancel`, `nw_release`, etc.), and the snapshot Mutex
// already serializes shared-state access.
unsafe impl Send for MacOsNetworkState {}
unsafe impl Sync for MacOsNetworkState {}

impl MacOsNetworkState {
    /// Construct, start monitoring, and return. Update handler will fire
    /// asynchronously on the dedicated queue with the current path shortly
    /// after this returns.
    pub fn new() -> Result<Self> {
        let shared: Arc<(Mutex<SharedState>, Condvar)> =
            Arc::new((Mutex::new(SharedState::default()), Condvar::new()));

        // Dedicated serial queue. Label is a debug aid (visible in lldb /
        // Instruments). NULL attr == serial queue.
        let queue = unsafe { dispatch_queue_create(c"condrun.nwpath".as_ptr(), null_mut()) };
        if queue.is_null() {
            anyhow::bail!("dispatch_queue_create returned NULL");
        }

        let monitor = unsafe { nw_path_monitor_create() };
        if monitor.is_null() {
            // Roll back: release the queue we just created.
            unsafe { dispatch_release(queue) };
            anyhow::bail!("nw_path_monitor_create returned NULL");
        }

        // Build the update handler. Capture an Arc clone so the snapshot
        // survives even if MacOsNetworkState is dropped while a callback is
        // in flight.
        let shared_for_handler = Arc::clone(&shared);
        let handler: RcBlock<dyn Fn(*mut c_void)> = RcBlock::new(move |path: *mut c_void| {
            // SAFETY: `path` is a borrowed `nw_path_t` owned by the framework
            // for the duration of this callback. We do not retain it.
            if path.is_null() {
                return;
            }
            let expensive = unsafe { nw_path_is_expensive(path) };
            let low_data = unsafe { nw_path_is_constrained(path) };
            let (lock, cvar) = &*shared_for_handler;
            if let Ok(mut guard) = lock.lock() {
                guard.snapshot = NwPathSnapshot {
                    expensive,
                    low_data,
                };
                guard.ready = true;
                cvar.notify_all();
            }
            // If the lock is poisoned we silently skip — supervisor will
            // continue reading the last-good value.
        });

        // Pass the block as a raw `*mut c_void`. `&*handler` gives us a
        // `&Block<...>`; cast to a mutable raw pointer for the FFI signature.
        // Network.framework calls `Block_copy` internally to retain it.
        let block_ptr: *const _ = &*handler;
        unsafe {
            nw_path_monitor_set_update_handler(monitor, block_ptr as *mut c_void);
            nw_path_monitor_set_queue(monitor, queue);
            nw_path_monitor_start(monitor);
        }

        // Block until first NWPath update lands, so callers don't see
        // uninitialized defaults. Bounded — if the framework is wedged we
        // log and continue rather than hanging forever.
        {
            let (lock, cvar) = &*shared;
            let guard = lock
                .lock()
                .map_err(|_| anyhow::anyhow!("snapshot mutex poisoned during construction"))?;
            let (guard, wait_result) = cvar
                .wait_timeout_while(guard, FIRST_UPDATE_TIMEOUT, |s| !s.ready)
                .map_err(|_| anyhow::anyhow!("snapshot mutex poisoned while waiting"))?;
            if wait_result.timed_out() && !guard.ready {
                tracing::warn!(
                    "NWPathMonitor delivered no update within {:?}; \
                     proceeding with default (non-expensive, no Low Data Mode) \
                     until the first callback lands",
                    FIRST_UPDATE_TIMEOUT
                );
            }
        }

        Ok(Self {
            shared,
            monitor,
            queue,
            _handler: handler,
        })
    }
}

#[async_trait]
impl NetworkState for MacOsNetworkState {
    async fn is_expensive(&self) -> bool {
        // Lock is brief — only across a Copy read. Never held over an await.
        let (lock, _cvar) = &*self.shared;
        match lock.lock() {
            Ok(guard) => guard.snapshot.expensive,
            // Poisoned mutex means a previous handler panicked. Default to
            // "not expensive" so we don't spuriously kill child processes.
            Err(poisoned) => poisoned.into_inner().snapshot.expensive,
        }
    }

    async fn is_low_data_mode(&self) -> bool {
        let (lock, _cvar) = &*self.shared;
        match lock.lock() {
            Ok(guard) => guard.snapshot.low_data,
            Err(poisoned) => poisoned.into_inner().snapshot.low_data,
        }
    }
}

impl Drop for MacOsNetworkState {
    fn drop(&mut self) {
        // Drop order matters:
        //   1. cancel — stops scheduling new callbacks (async; pending
        //      blocks may still run, but they only touch the snapshot Arc
        //      which lives until the last block drops its clone).
        //   2. nw_release(monitor) — drops our +1 ref.
        //   3. dispatch_release(queue) — drops our +1 ref. Any in-flight
        //      blocks on the queue keep it alive via their own +1 from
        //      libdispatch internals.
        unsafe {
            nw_path_monitor_cancel(self.monitor);
            nw_release(self.monitor);
            dispatch_release(self.queue);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — gated behind `platform-tests` feature; require a real macOS host
// with a network interface. Skipped in `cargo test` by default.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "platform-tests"))]
mod tests {
    use super::*;

    /// Acceptance #1: `MacOsNetworkState::new()` succeeds without errors on
    /// macOS 14+ AND blocks until the first NWPath update lands. Regression
    /// for cr-npv: `condrun check` previously read the default snapshot
    /// (expensive=false, low_data=false) before the update handler fired,
    /// returning PASS even on a cellular tether.
    #[test]
    fn construct_blocks_until_first_update() {
        let state = MacOsNetworkState::new().expect("construct MacOsNetworkState");
        let (lock, _) = &*state.shared;
        let guard = lock.lock().expect("lock");
        assert!(
            guard.ready,
            "first NWPath update must land before new() returns"
        );
    }

    /// Acceptance #3: on regular wifi or wired ethernet, `is_expensive()`
    /// returns `false`. Developer-machine only — virtualized CI runners
    /// have undocumented NWPath state.
    #[tokio::test]
    #[ignore = "developer-machine only — assumes runner is on regular wifi/ethernet"]
    async fn is_expensive_on_wifi() {
        let state = MacOsNetworkState::new().expect("construct");
        assert!(!state.is_expensive().await, "expected non-expensive path");
    }

    /// Acceptance #5: with Low Data Mode disabled, `is_low_data_mode()`
    /// returns `false`.
    #[tokio::test]
    #[ignore = "developer-machine only — assumes Low Data Mode is OFF"]
    async fn is_low_data_off_default() {
        let state = MacOsNetworkState::new().expect("construct");
        assert!(
            !state.is_low_data_mode().await,
            "expected Low Data Mode off"
        );
    }

    /// Acceptance: dropping the state cancels the monitor without panicking.
    /// Pending blocks finish on the orphaned Arc — exercised here implicitly
    /// by constructing/dropping in tight succession.
    #[test]
    fn drop_cancels_monitor() {
        for _ in 0..8 {
            let state = MacOsNetworkState::new().expect("construct");
            drop(state);
        }
    }
}
