use std::fmt;

/// Priority levels for message transmission (5-level simplified mapping to Zenoh's 7-level system)
///
/// Aorta uses a simplified 5-level priority system that maps to Zenoh's native 7-level priority:
/// - Realtime   → Zenoh::RealTime  (highest)
/// - High       → Zenoh::InteractiveLow
/// - Normal     → Zenoh::DataLow (default)
/// - Low        → Zenoh::Background
/// - Background → Zenoh::Background
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
    /// Realtime priority (highest) - time-critical messages (e.g., emergency stop, safety signals)
    Realtime,
    /// High priority - important control messages (e.g., motion commands, sensor fusion data)
    High,
    /// Normal priority (default) - standard application messages
    Normal,
    /// Low priority - non-critical periodic updates (e.g., status reports)
    Low,
    /// Background priority (lowest) - bulk data transfer, logs
    Background,
}

impl Priority {
    /// Default priority level
    pub const DEFAULT: Priority = Priority::Normal;
}

impl Default for Priority {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<Priority> for zenoh::qos::Priority {
    /// Maps Aorta 5-level priority to Zenoh 7-level priority
    ///
    /// Mapping strategy:
    /// - Realtime   → RealTime (7)
    /// - High       → InteractiveLow (5)  
    /// - Normal     → DataLow (3) [default]
    /// - Low        → Background (1)
    /// - Background → Background (1)
    fn from(p: Priority) -> Self {
        match p {
            Priority::Realtime => zenoh::qos::Priority::RealTime,
            Priority::High => zenoh::qos::Priority::InteractiveLow,
            Priority::Normal => zenoh::qos::Priority::DataLow,
            Priority::Low | Priority::Background => zenoh::qos::Priority::Background,
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Priority::Realtime => write!(f, "Realtime"),
            Priority::High => write!(f, "High"),
            Priority::Normal => write!(f, "Normal"),
            Priority::Low => write!(f, "Low"),
            Priority::Background => write!(f, "Background"),
        }
    }
}

/// Reliability guarantees for message delivery
///
/// Note: In Zenoh, reliability is a wire marker that selects the appropriate transport
/// when the session is configured with multiple transports (e.g., TCP+UDP).
/// - Reliable → Prefer reliable hop-to-hop links (e.g., TCP/QUIC)
/// - BestEffort → UDP (low latency, may drop)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reliability {
    /// Best effort delivery - messages may be lost (uses UDP when available)
    /// Suitable for high-frequency sensor data where latest value matters more than history
    BestEffort,
    /// Reliable hop-to-hop transport preference (uses TCP/QUIC when available)
    /// Does not imply end-to-end exactly-once delivery across network partitions
    /// Suitable for commands, state updates, and critical data
    Reliable,
}

impl Reliability {
    /// Default reliability level
    pub const DEFAULT: Reliability = Reliability::Reliable;
}

impl Default for Reliability {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<Reliability> for zenoh::qos::Reliability {
    fn from(r: Reliability) -> Self {
        match r {
            Reliability::BestEffort => zenoh::qos::Reliability::BestEffort,
            Reliability::Reliable => zenoh::qos::Reliability::Reliable,
        }
    }
}

impl fmt::Display for Reliability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reliability::BestEffort => write!(f, "BestEffort"),
            Reliability::Reliable => write!(f, "Reliable"),
        }
    }
}

/// Congestion control policy for message transmission
///
/// Determines behavior when the zenoh transport queue is congested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CongestionControl {
    /// Drop messages when the queue is full (lowest latency, may lose data)
    Drop,
    /// Block the sender until the queue has space (preserves data, may stall)
    Block,
}

impl CongestionControl {
    /// Matches Zenoh's publisher default so adding the field to
    /// `PublisherOptions::default()` does not change behavior for existing
    /// publishers that previously relied on the builder default.
    pub const DEFAULT: CongestionControl = CongestionControl::Drop;
}

impl Default for CongestionControl {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<CongestionControl> for zenoh::qos::CongestionControl {
    fn from(c: CongestionControl) -> Self {
        match c {
            CongestionControl::Drop => zenoh::qos::CongestionControl::Drop,
            CongestionControl::Block => zenoh::qos::CongestionControl::Block,
        }
    }
}

impl fmt::Display for CongestionControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CongestionControl::Drop => write!(f, "Drop"),
            CongestionControl::Block => write!(f, "Block"),
        }
    }
}

/// Quality of Service (QoS) for publishers and subscribers.
///
/// Every field maps to a verified Zenoh primitive — there are **no silent
/// no-ops** (QoS & buffering redesign). Last-value / latched / late-joiner
/// snapshot semantics are **not** a QoS concern: use `ContextStore` for state.
/// Per-topic receive depth / isolation is `SubscriberOptions::receive_depth`.
///
/// (Historical note: the removed `durability`/`history_depth` fields were
/// forwarded to Zenoh nowhere — `TransientLocal` was an unimplemented no-op —
/// so deleting them changes no running behavior.)
///
/// # Examples
///
/// ```
/// use aorta_core::qos::{QoS, Priority, Reliability, CongestionControl};
///
/// // Default QoS (Normal priority, Reliable, Drop congestion, express off)
/// let qos = QoS::default();
///
/// // High-priority reliable command stream that blocks rather than drops
/// let cmd_qos = QoS::default()
///     .with_priority(Priority::High)
///     .with_reliability(Reliability::Reliable)
///     .with_congestion_control(CongestionControl::Block)
///     .with_express(true);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QoS {
    /// Message priority level → Zenoh publisher priority (+ local sub dispatch).
    pub priority: Priority,
    /// Delivery reliability marker (publisher side) → Zenoh link-class hint.
    pub reliability: Reliability,
    /// Behavior when the Zenoh transport queue is congested. `Block` is the
    /// honest "don't silently drop" lever; `Drop` is lowest-latency lossy.
    pub congestion_control: CongestionControl,
    /// When true, messages bypass Zenoh's internal batching and are sent
    /// immediately. Reduces latency (especially p99) at the cost of
    /// throughput—each message incurs its own syscall instead of being
    /// coalesced with neighbours.
    /// Default: false. Set express explicitly for latency-sensitive publishers.
    pub express: bool,
}

impl QoS {
    /// Creates a new QoS with the given priority and reliability; congestion
    /// control defaults to `Drop` and express to `false`.
    pub const fn new(priority: Priority, reliability: Reliability) -> Self {
        Self {
            priority,
            reliability,
            congestion_control: CongestionControl::DEFAULT,
            express: false,
        }
    }

    /// Sets the priority level (builder pattern)
    pub const fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the reliability level (builder pattern)
    pub const fn with_reliability(mut self, reliability: Reliability) -> Self {
        self.reliability = reliability;
        self
    }

    /// Sets the congestion-control policy (builder pattern)
    pub const fn with_congestion_control(mut self, congestion_control: CongestionControl) -> Self {
        self.congestion_control = congestion_control;
        self
    }

    /// Sets the express flag (builder pattern)
    ///
    /// When true, messages bypass Zenoh's internal batching and are sent
    /// immediately. Default is false.
    pub const fn with_express(mut self, express: bool) -> Self {
        self.express = express;
        self
    }

    /// Realtime control messages: Realtime priority, Reliable, `Drop`, express
    /// on. **No preset blocks by default** — pre-redesign every publisher
    /// effectively ran `Drop` (the old `PublisherOptions` default), and a
    /// preset silently flipping to `Block` would let a congested transport
    /// stall the control thread with zero code change (publish stays
    /// fire-and-forget). Lossless delivery is an explicit, informed
    /// opt-in: `with_congestion_control(Block)` on the QoS, or a per-publisher
    /// `PublisherOptions::congestion_control` override.
    pub const fn realtime_control() -> Self {
        Self {
            priority: Priority::Realtime,
            reliability: Reliability::Reliable,
            congestion_control: CongestionControl::Drop,
            express: true,
        }
    }

    /// High-rate sensor data: Normal priority, BestEffort, Drop (latest value
    /// wins), express off.
    pub const fn sensor_data() -> Self {
        Self {
            priority: Priority::Normal,
            reliability: Reliability::BestEffort,
            congestion_control: CongestionControl::Drop,
            express: false,
        }
    }

    /// State updates: High priority, Reliable, Drop, express off.
    ///
    /// Note: this preset no longer implies any latched/last-value replay (the
    /// old `TransientLocal` was a no-op). For "late joiner gets the current
    /// value", publish state through `ContextStore`. If you need a state stream
    /// that must never silently drop, set `with_congestion_control(Block)`
    /// explicitly — no preset blocks by default.
    pub const fn state_update() -> Self {
        Self {
            priority: Priority::High,
            reliability: Reliability::Reliable,
            congestion_control: CongestionControl::Drop,
            express: false,
        }
    }

    /// Bulk data transfer: Background priority, Reliable, Drop, express off
    /// (throughput optimised). Set `with_congestion_control(Block)` if the
    /// transfer must be lossless.
    pub const fn bulk_transfer() -> Self {
        Self {
            priority: Priority::Background,
            reliability: Reliability::Reliable,
            congestion_control: CongestionControl::Drop,
            express: false,
        }
    }
}

impl Default for QoS {
    /// Default QoS settings: Normal priority, Reliable, Drop congestion control,
    /// express disabled.
    fn default() -> Self {
        Self {
            priority: Priority::DEFAULT,
            reliability: Reliability::DEFAULT,
            congestion_control: CongestionControl::DEFAULT,
            express: false,
        }
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QoS(priority={}, reliability={}, congestion_control={}, express={})",
            self.priority, self.reliability, self.congestion_control, self.express
        )
    }
}

#[cfg(test)]
#[path = "../test/qos_tests.rs"]
mod tests;
