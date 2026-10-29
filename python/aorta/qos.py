"""Quality-of-Service profile for publishers/subscribers — parity with the
Rust ``QoS`` (``core/src/qos.rs``) and C++ ``QoS`` (``cpp/include/aorta/qos.h``).

QoS is the 4 wired fields
``priority`` / ``reliability`` / ``congestion_control`` / ``express``. The former
``durability``/``history_depth`` were silent no-ops and are gone — latched /
last-value semantics are :class:`~aorta.ContextStore`'s job; per-topic receive
depth is ``SubscriberOptions.receive_depth``.

The C-ABI ``aorta_qos_t`` now mirrors this struct 1:1 (priority / reliability /
congestion_control / express); a publisher may still override the base
``congestion_control`` per-publisher via ``aorta_pub_options_t``.

The presets are **not hardcoded**: they are read from the C-ABI
``aorta_qos_preset`` (which returns the full struct), so there is a single
source of preset truth across all SDKs.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from ._ffi import bindings, loader, structs

# Mirror of the C-ABI ``aorta_qos_preset_t`` discriminants.
_PRESET_DEFAULT = 0
_PRESET_REALTIME_CONTROL = 1
_PRESET_SENSOR_DATA = 2
_PRESET_STATE_UPDATE = 3
_PRESET_BULK_TRANSFER = 4


class Priority(IntEnum):
    """Message scheduling priority (``aorta_priority_t``)."""

    REALTIME = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class Reliability(IntEnum):
    """Delivery guarantee (``aorta_reliability_t``)."""

    BEST_EFFORT = 0
    RELIABLE = 1


class CongestionControl(IntEnum):
    """Behavior when the transport queue is congested
    (``AORTA_CONGESTION_CONTROL_*``). ``BLOCK`` is the honest "don't silently
    drop" lever; ``DROP`` (default) is lowest-latency lossy.
    """

    DROP = 0
    BLOCK = 1


@dataclass(frozen=True)
class QoS:
    """Quality-of-service for a publisher/subscriber. Pass to
    ``Node.create_publisher`` / ``Node.create_subscriber`` (and their typed
    variants). Defaults match the core: Normal priority, Reliable, Drop
    congestion control, express off.
    """

    priority: Priority = Priority.NORMAL
    reliability: Reliability = Reliability.RELIABLE
    congestion_control: CongestionControl = CongestionControl.DROP
    express: bool = False

    def _to_ffi(self) -> "structs.AortaQoS":
        """Build ``aorta_qos_t`` — a 1:1 mirror of this struct (priority /
        reliability / congestion_control / express)."""
        return structs.AortaQoS(
            priority=int(self.priority),
            reliability=int(self.reliability),
            congestion_control=int(self.congestion_control),
            express=1 if self.express else 0,
        )

    @classmethod
    def _from_preset(cls, preset: int) -> "QoS":
        lib = bindings.configure(loader.library())
        q = lib.aorta_qos_preset(preset)
        return cls(
            priority=Priority(q.priority),
            reliability=Reliability(q.reliability),
            congestion_control=CongestionControl(q.congestion_control),
            express=bool(q.express),
        )

    @classmethod
    def default(cls) -> "QoS":
        """The canonical default profile (``aorta_qos_preset`` DEFAULT)."""
        return cls._from_preset(_PRESET_DEFAULT)

    @classmethod
    def realtime_control(cls) -> "QoS":
        """Low-latency control loops: Realtime + Reliable + Drop + express.

        No preset blocks by default; lossless delivery is an explicit opt-in
        via ``dataclasses.replace(qos, congestion_control=BLOCK)`` or a
        per-publisher congestion override.
        """
        return cls._from_preset(_PRESET_REALTIME_CONTROL)

    @classmethod
    def sensor_data(cls) -> "QoS":
        """High-rate sensor streams: BestEffort + Drop (latest-wins), no express."""
        return cls._from_preset(_PRESET_SENSOR_DATA)

    @classmethod
    def state_update(cls) -> "QoS":
        """State: High + Reliable + Drop, no express. No latched replay — publish
        state through ContextStore for late-joiner snapshots. Set
        congestion_control=BLOCK explicitly if the stream must never drop."""
        return cls._from_preset(_PRESET_STATE_UPDATE)

    @classmethod
    def bulk_transfer(cls) -> "QoS":
        """Large/bulk payloads: Reliable + Drop, lower priority. Set
        congestion_control=BLOCK explicitly for a lossless transfer."""
        return cls._from_preset(_PRESET_BULK_TRANSFER)
