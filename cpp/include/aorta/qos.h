#pragma once

#include <cstddef>
#include <cstdint>

#include <aorta.h>

namespace aorta {

enum class Priority {
  kRealtime,
  kHigh,
  kNormal,
  kLow,
  kBackground,
};

/// EXECUTOR dispatch priority (which ready event runs first) — the C++
/// spelling of Rust `EventPriority`, one of the SDK's two priority
/// vocabularies:
/// - `EventPriority` (this): local scheduling — `flow::Scope::Options`,
///   `flow::offload`'s lane pick, `LaneRuntime` posts, the 5→4 lane fold.
/// - `Priority` (above): the ZENOH WIRE priority (network QoS on publish).
/// A subscriber's wire priority maps 1:1 name-for-name into the dispatch
/// vocabulary at RX unless overridden (`map_event_priority`, Rust core).
/// The explicit values ARE the C-ABI priority byte / Rust
/// `priority_index`; 255 is the inherit SENTINEL owned by the
/// C plane, never a value here.
enum class EventPriority : uint8_t {
  kRealtime = 0,
  kHigh = 1,
  kNormal = 2,
  kLow = 3,
  kBackground = 4,
};

enum class Reliability {
  kBestEffort,
  kReliable,
};

// Congestion control policy: behavior when the publisher's egress queue is
// congested. kDrop (default) drops messages for lowest latency; kBlock applies
// backpressure for lossless delivery on critical topics.
enum class CongestionControl {
  kDrop,
  kBlock,
};

// Quality of Service — the 4 wired fields. The
// former `durability`/`history_depth` were silent no-ops and are gone; latched
// / last-value semantics are `ContextStore`'s job. Per-topic receive depth is
// `SubscriberOptions::receive_depth`.
struct QoS {
  Priority priority = Priority::kNormal;
  Reliability reliability = Reliability::kReliable;
  /// Behavior when the transport queue is congested. kBlock is the honest
  /// "don't silently drop" lever. Default kDrop.
  CongestionControl congestion_control = CongestionControl::kDrop;
  /// When true, messages bypass Zenoh's internal batching (express mode).
  /// Reduces latency at the cost of throughput. Default: false.
  bool express = false;

  // Preset profiles. Single source of truth lives in `core/src/qos.rs`;
  // these wrappers fetch the canonical values through the C-ABI
  // (`aorta_qos_preset`, which now returns the full struct), so re-tuning a
  // profile only requires editing one place.
  //
  // Implementations live in `cpp/src/qos.cc` rather than this header
  // so the link dependency on `aorta_core_ffi` propagates through the
  // full `aorta_cpp` library only — keeping `aorta_cpp_headers`
  // header-only-with-no-runtime-symbols for the mock-based unit test
  // surface.
  static QoS RealtimeControl();
  static QoS SensorData();
  static QoS StateUpdate();
  static QoS BulkTransfer();

  // Maps onto `aorta_qos_t`, which now mirrors this struct 1:1 (priority /
  // reliability / congestion_control / express). For a publisher,
  // `aorta_pub_options_t.congestion_control` can still override this base value
  // per-publisher (see Node::CreatePublisher).
  aorta_qos_t ToFfi() const {
    aorta_qos_t ffi{};
    switch (priority) {
      case Priority::kRealtime:
        ffi.priority = AORTA_PRIORITY_REALTIME;
        break;
      case Priority::kHigh:
        ffi.priority = AORTA_PRIORITY_HIGH;
        break;
      case Priority::kNormal:
        ffi.priority = AORTA_PRIORITY_NORMAL;
        break;
      case Priority::kLow:
        ffi.priority = AORTA_PRIORITY_LOW;
        break;
      case Priority::kBackground:
        ffi.priority = AORTA_PRIORITY_BACKGROUND;
        break;
    }

    switch (reliability) {
      case Reliability::kBestEffort:
        ffi.reliability = AORTA_RELIABILITY_BEST_EFFORT;
        break;
      case Reliability::kReliable:
        ffi.reliability = AORTA_RELIABILITY_RELIABLE;
        break;
    }

    ffi.congestion_control = congestion_control == CongestionControl::kBlock
                                 ? AORTA_CONGESTION_CONTROL_BLOCK
                                 : AORTA_CONGESTION_CONTROL_DROP;
    ffi.express = express ? 1 : 0;
    return ffi;
  }
};

}  // namespace aorta
