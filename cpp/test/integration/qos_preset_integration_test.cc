// Cross-language QoS preset consistency test.
//
// `QoS::RealtimeControl()` and the three sibling presets are defined in the C++
// header `cpp/include/aorta/qos.h`, but they call into the Rust-side
// `aorta_qos_preset` C-ABI so the canonical values live in one place
// (core/src/qos.rs).
//
// QoS & buffering redesign: QoS collapsed to { priority, reliability,
// congestion_control, express } and `aorta_qos_t` now mirrors it 1:1 (no more
// frozen durability/history bytes, no companion accessors).
//
// Lives in `cpp/test/integration/` rather than the unit test suite because it
// links against `//core:aorta_core_ffi` (the real Rust library).

#include "aorta/qos.h"

#include <aorta.h>
#include <gtest/gtest.h>

namespace aorta {
namespace {

struct PresetExpectation {
  aorta_qos_preset_t preset;
  aorta_priority_t priority;
  aorta_reliability_t reliability;
  uint8_t congestion;  // AORTA_CONGESTION_CONTROL_*
  bool express;
  Priority cpp_priority;
  Reliability cpp_reliability;
  CongestionControl cpp_congestion;
};

constexpr PresetExpectation kPresets[] = {
    {AORTA_QOS_PRESET_REALTIME_CONTROL, AORTA_PRIORITY_REALTIME,
     AORTA_RELIABILITY_RELIABLE, AORTA_CONGESTION_CONTROL_DROP, true,
     Priority::kRealtime, Reliability::kReliable, CongestionControl::kDrop},
    {AORTA_QOS_PRESET_SENSOR_DATA, AORTA_PRIORITY_NORMAL,
     AORTA_RELIABILITY_BEST_EFFORT, AORTA_CONGESTION_CONTROL_DROP, false,
     Priority::kNormal, Reliability::kBestEffort, CongestionControl::kDrop},
    {AORTA_QOS_PRESET_STATE_UPDATE, AORTA_PRIORITY_HIGH,
     AORTA_RELIABILITY_RELIABLE, AORTA_CONGESTION_CONTROL_DROP, false,
     Priority::kHigh, Reliability::kReliable, CongestionControl::kDrop},
    {AORTA_QOS_PRESET_BULK_TRANSFER, AORTA_PRIORITY_BACKGROUND,
     AORTA_RELIABILITY_RELIABLE, AORTA_CONGESTION_CONTROL_DROP, false,
     Priority::kBackground, Reliability::kReliable, CongestionControl::kDrop},
};

// (1) FFI-level: the preset accessors return the canonical fields. A silent
//     drift in core/src/qos.rs is caught here.
TEST(QoSPresetIntegrationTest, FfiPresetsReturnCanonicalValues) {
  for (const auto& exp : kPresets) {
    const aorta_qos_t ffi = aorta_qos_preset(exp.preset);
    EXPECT_EQ(ffi.priority, exp.priority) << "preset = " << exp.preset;
    EXPECT_EQ(ffi.reliability, exp.reliability) << "preset = " << exp.preset;
    EXPECT_EQ(ffi.congestion_control, exp.congestion)
        << "preset = " << exp.preset;
    EXPECT_EQ(ffi.express, exp.express ? 1 : 0) << "preset = " << exp.preset;
  }
}

// (2) C++ wrapper layer: QoS::RealtimeControl() etc. produce a C++ QoS whose
//     fields match what the FFI returns directly.
TEST(QoSPresetIntegrationTest, CppPresetsMatchFfiPresets) {
  QoS cpp_presets[] = {
      QoS::RealtimeControl(),
      QoS::SensorData(),
      QoS::StateUpdate(),
      QoS::BulkTransfer(),
  };
  for (std::size_t i = 0; i < std::size(cpp_presets); ++i) {
    const QoS& qos = cpp_presets[i];
    const PresetExpectation& exp = kPresets[i];
    EXPECT_EQ(qos.priority, exp.cpp_priority) << "preset index " << i;
    EXPECT_EQ(qos.reliability, exp.cpp_reliability) << "preset index " << i;
    EXPECT_EQ(qos.congestion_control, exp.cpp_congestion)
        << "preset index " << i;
    EXPECT_EQ(qos.express, exp.express) << "preset index " << i;
  }
}

// (3) Round-trip: C++ preset → ToFfi() → equals the raw FFI preset for every
//     carried field (priority/reliability/congestion_control/express).
TEST(QoSPresetIntegrationTest, CppToFfiMatchesRawFfiPreset) {
  const QoS cpp_presets[] = {
      QoS::RealtimeControl(),
      QoS::SensorData(),
      QoS::StateUpdate(),
      QoS::BulkTransfer(),
  };
  for (std::size_t i = 0; i < std::size(cpp_presets); ++i) {
    const aorta_qos_t round_tripped = cpp_presets[i].ToFfi();
    const aorta_qos_t direct = aorta_qos_preset(kPresets[i].preset);
    EXPECT_EQ(round_tripped.priority, direct.priority) << "preset index " << i;
    EXPECT_EQ(round_tripped.reliability, direct.reliability)
        << "preset index " << i;
    EXPECT_EQ(round_tripped.congestion_control, direct.congestion_control)
        << "preset index " << i;
    EXPECT_EQ(round_tripped.express, direct.express) << "preset index " << i;
  }
}

// Defensive fallback: an out-of-range preset value must return DEFAULT, not UB.
TEST(QoSPresetIntegrationTest, OutOfRangePresetFallsBackToDefault) {
  const aorta_qos_preset_t bogus = static_cast<aorta_qos_preset_t>(99);
  const aorta_qos_t ffi = aorta_qos_preset(bogus);
  const QoS cpp_default;
  const aorta_qos_t default_ffi = cpp_default.ToFfi();
  EXPECT_EQ(ffi.priority, default_ffi.priority);
  EXPECT_EQ(ffi.reliability, default_ffi.reliability);
  EXPECT_EQ(ffi.congestion_control, default_ffi.congestion_control);
  EXPECT_EQ(ffi.express, default_ffi.express);
}

// AORTA_QOS_PRESET_DEFAULT matches the default-constructed C++ QoS.
TEST(QoSPresetIntegrationTest, FfiDefaultMatchesCppDefault) {
  const aorta_qos_t ffi = aorta_qos_preset(AORTA_QOS_PRESET_DEFAULT);
  const QoS cpp_default;
  const aorta_qos_t cpp_default_ffi = cpp_default.ToFfi();
  EXPECT_EQ(ffi.priority, cpp_default_ffi.priority);
  EXPECT_EQ(ffi.reliability, cpp_default_ffi.reliability);
  EXPECT_EQ(ffi.congestion_control, cpp_default_ffi.congestion_control);
  EXPECT_EQ(ffi.express, cpp_default_ffi.express);
}

}  // namespace
}  // namespace aorta
