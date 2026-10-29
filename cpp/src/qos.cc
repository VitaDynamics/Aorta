// QoS preset implementations.
//
// Pulled out of `cpp/include/aorta/qos.h` so the inline body's call into
// `aorta_qos_preset` is emitted exactly
// once — from `aorta_cpp`, which already links `aorta_core_ffi`.
//
// Keeping the helpers in the header turned `aorta_cpp_headers` into a
// stealth link-time dependency on the FFI library: any translation unit
// including `qos.h` and calling a preset would emit a reference to the
// C-ABI symbol, breaking mock-based unit tests that don't link the real
// Rust runtime. The four wrappers and
// the `FromPresetFfi` decoder now live here; the header keeps only the
// declarations.

#include "aorta/qos.h"

#include <aorta.h>

namespace aorta {
namespace {

QoS FromPresetFfi(aorta_qos_preset_t preset) {
  const aorta_qos_t ffi = aorta_qos_preset(preset);
  QoS out{};
  switch (ffi.priority) {
    case AORTA_PRIORITY_REALTIME:
      out.priority = Priority::kRealtime;
      break;
    case AORTA_PRIORITY_HIGH:
      out.priority = Priority::kHigh;
      break;
    case AORTA_PRIORITY_NORMAL:
      out.priority = Priority::kNormal;
      break;
    case AORTA_PRIORITY_LOW:
      out.priority = Priority::kLow;
      break;
    case AORTA_PRIORITY_BACKGROUND:
      out.priority = Priority::kBackground;
      break;
  }
  switch (ffi.reliability) {
    case AORTA_RELIABILITY_BEST_EFFORT:
      out.reliability = Reliability::kBestEffort;
      break;
    case AORTA_RELIABILITY_RELIABLE:
      out.reliability = Reliability::kReliable;
      break;
  }
  // congestion_control + express now ride aorta_qos_t directly.
  out.congestion_control = ffi.congestion_control == AORTA_CONGESTION_CONTROL_BLOCK
                               ? CongestionControl::kBlock
                               : CongestionControl::kDrop;
  out.express = ffi.express != 0;
  return out;
}

}  // namespace

QoS QoS::RealtimeControl() {
  return FromPresetFfi(AORTA_QOS_PRESET_REALTIME_CONTROL);
}

QoS QoS::SensorData() { return FromPresetFfi(AORTA_QOS_PRESET_SENSOR_DATA); }

QoS QoS::StateUpdate() { return FromPresetFfi(AORTA_QOS_PRESET_STATE_UPDATE); }

QoS QoS::BulkTransfer() {
  return FromPresetFfi(AORTA_QOS_PRESET_BULK_TRANSFER);
}

}  // namespace aorta
