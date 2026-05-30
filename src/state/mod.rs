use async_trait::async_trait;

pub mod fake;
#[cfg(feature = "test-fixture")]
pub mod fixture;
#[cfg(target_os = "macos")]
pub mod macos;

/// Type of network interface providing connectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InterfaceType {
    Wifi,
    Ethernet,
    CellularTether,
    Unknown,
}

/// A network interface (name + classified type).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interface {
    pub name: String,
    pub iface_type: InterfaceType,
}

/// Read-only snapshot of system network state.
///
/// Implementations: real macOS reader (NWPathMonitor), fixture reader (JSON file),
/// and test fakes. v0.1 is poll-only — no `changes()` channel.
#[async_trait]
pub trait NetworkState: Send + Sync {
    /// True iff the current path is expensive (cellular / Personal Hotspot),
    /// per `NWPath.isExpensive`.
    async fn is_expensive(&self) -> bool;

    /// True iff Low Data Mode is enabled on the current path,
    /// per `NWPath.isConstrained`.
    async fn is_low_data_mode(&self) -> bool;
}
