// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for QoS (Quality of Service) class.
//
// QoS & buffering redesign: QoS is now the 4 wired fields
// { priority, reliability, congestion_control, express }. The former
// durability/history_depth were silent no-ops and are gone; the frozen
// aorta_qos_t still carries those bytes for ABI, so ToFfi() emits the canonical
// Volatile / 0.

#include "aorta/qos.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

// Test Priority default value
TEST(QoSPriorityTest, DefaultValue) {
  EXPECT_EQ(Priority::kNormal, Priority::kNormal);
}

// Test all Priority enum values exist
TEST(QoSPriorityTest, AllEnumValues) {
  Priority priorities[] = {
      Priority::kRealtime, Priority::kHigh,       Priority::kNormal,
      Priority::kLow,      Priority::kBackground,
  };

  // Ensure all values are distinct
  for (size_t i = 0; i < 5; ++i) {
    for (size_t j = i + 1; j < 5; ++j) {
      EXPECT_NE(priorities[i], priorities[j])
          << "Priority values should be distinct";
    }
  }
}

// Test Reliability enum values
TEST(QoSReliabilityTest, AllEnumValues) {
  Reliability reliabilities[] = {
      Reliability::kBestEffort,
      Reliability::kReliable,
  };

  EXPECT_NE(reliabilities[0], reliabilities[1]);
}

// Test CongestionControl enum values
TEST(QoSCongestionControlTest, AllEnumValues) {
  CongestionControl ccs[] = {
      CongestionControl::kDrop,
      CongestionControl::kBlock,
  };

  EXPECT_NE(ccs[0], ccs[1]);
}

// Test QoS default construction
TEST(QoSTest, DefaultConstruction) {
  QoS qos;
  EXPECT_EQ(qos.priority, Priority::kNormal);
  EXPECT_EQ(qos.reliability, Reliability::kReliable);
  EXPECT_EQ(qos.congestion_control, CongestionControl::kDrop);
  EXPECT_FALSE(qos.express);
}

// Preset-method tests (RealtimeControl / SensorData / StateUpdate /
// BulkTransfer) live in `cpp/test/integration/qos_preset_integration_test.cc`.
// The presets fetch their canonical values through the
// C-ABI (`aorta_qos_preset`, which returns the full struct), so calling them at
// the C++ level requires linking against `aorta_core_ffi`.

// Test QoS ToFfi() conversion - Priority mapping
TEST(QoSToFfiTest, PriorityMapping) {
  struct TestCase {
    Priority cpp_priority;
    aorta_priority_t expected_ffi;
  };

  TestCase cases[] = {
      {Priority::kRealtime, AORTA_PRIORITY_REALTIME},
      {Priority::kHigh, AORTA_PRIORITY_HIGH},
      {Priority::kNormal, AORTA_PRIORITY_NORMAL},
      {Priority::kLow, AORTA_PRIORITY_LOW},
      {Priority::kBackground, AORTA_PRIORITY_BACKGROUND},
  };

  for (const auto& tc : cases) {
    QoS qos;
    qos.priority = tc.cpp_priority;
    aorta_qos_t ffi = qos.ToFfi();
    EXPECT_EQ(ffi.priority, tc.expected_ffi)
        << "Priority mapping failed for enum value";
  }
}

// Test QoS ToFfi() conversion - Reliability mapping
TEST(QoSToFfiTest, ReliabilityMapping) {
  struct TestCase {
    Reliability cpp_reliability;
    aorta_reliability_t expected_ffi;
  };

  TestCase cases[] = {
      {Reliability::kBestEffort, AORTA_RELIABILITY_BEST_EFFORT},
      {Reliability::kReliable, AORTA_RELIABILITY_RELIABLE},
  };

  for (const auto& tc : cases) {
    QoS qos;
    qos.reliability = tc.cpp_reliability;
    aorta_qos_t ffi = qos.ToFfi();
    EXPECT_EQ(ffi.reliability, tc.expected_ffi) << "Reliability mapping failed";
  }
}

// ToFfi() maps congestion_control + express onto aorta_qos_t (which now mirrors
// the C++ QoS struct 1:1 — no more frozen durability/history bytes).
TEST(QoSToFfiTest, EmitsCongestionControlAndExpress) {
  QoS block_express{Priority::kHigh, Reliability::kReliable,
                    CongestionControl::kBlock, true};
  aorta_qos_t ffi = block_express.ToFfi();
  EXPECT_EQ(ffi.congestion_control, AORTA_CONGESTION_CONTROL_BLOCK);
  EXPECT_EQ(ffi.express, 1);

  QoS drop_plain{Priority::kNormal, Reliability::kReliable,
                 CongestionControl::kDrop, false};
  aorta_qos_t ffi2 = drop_plain.ToFfi();
  EXPECT_EQ(ffi2.congestion_control, AORTA_CONGESTION_CONTROL_DROP);
  EXPECT_EQ(ffi2.express, 0);
}

// Test QoS ToFfi() full conversion (literal-constructed; preset-via-FFI
// roundtrip lives in qos_preset_integration_test).
TEST(QoSToFfiTest, FullConversion) {
  QoS qos{Priority::kRealtime, Reliability::kReliable, CongestionControl::kBlock,
          true};
  aorta_qos_t ffi = qos.ToFfi();

  EXPECT_EQ(ffi.priority, AORTA_PRIORITY_REALTIME);
  EXPECT_EQ(ffi.reliability, AORTA_RELIABILITY_RELIABLE);
  EXPECT_EQ(ffi.congestion_control, AORTA_CONGESTION_CONTROL_BLOCK);
  EXPECT_EQ(ffi.express, 1);
}

// Test QoS copy semantics
TEST(QoSTest, CopySemantics) {
  QoS original{Priority::kNormal, Reliability::kBestEffort,
               CongestionControl::kDrop, true};
  QoS copied = original;

  EXPECT_EQ(copied.priority, original.priority);
  EXPECT_EQ(copied.reliability, original.reliability);
  EXPECT_EQ(copied.congestion_control, original.congestion_control);
  EXPECT_EQ(copied.express, original.express);
}

// Test QoS modification
TEST(QoSTest, Modification) {
  QoS qos;

  qos.priority = Priority::kHigh;
  EXPECT_EQ(qos.priority, Priority::kHigh);

  qos.reliability = Reliability::kBestEffort;
  EXPECT_EQ(qos.reliability, Reliability::kBestEffort);

  qos.congestion_control = CongestionControl::kBlock;
  EXPECT_EQ(qos.congestion_control, CongestionControl::kBlock);

  qos.express = true;
  EXPECT_TRUE(qos.express);
}

// Test all combinations of QoS settings
TEST(QoSTest, AllCombinations) {
  for (auto priority : {Priority::kRealtime, Priority::kHigh, Priority::kNormal,
                        Priority::kLow, Priority::kBackground}) {
    for (auto reliability :
         {Reliability::kBestEffort, Reliability::kReliable}) {
      for (auto cc : {CongestionControl::kDrop, CongestionControl::kBlock}) {
        QoS qos;
        qos.priority = priority;
        qos.reliability = reliability;
        qos.congestion_control = cc;

        aorta_qos_t ffi = qos.ToFfi();

        // Verify priority converted correctly.
        EXPECT_EQ(ffi.priority, static_cast<aorta_priority_t>(
                                    priority == Priority::kRealtime ? 0
                                    : priority == Priority::kHigh   ? 1
                                    : priority == Priority::kNormal ? 2
                                    : priority == Priority::kLow    ? 3
                                                                    : 4));
      }
    }
  }
}

}  // namespace
}  // namespace aorta
