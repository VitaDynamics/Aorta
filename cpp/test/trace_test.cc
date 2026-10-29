// Tests for the C++ tracing wrapper (Step 11).
//
// Links the real trace C-ABI (//core:aorta_core_ffi), so these exercise the
// SAME process-global engine the Rust core uses — the recorded-data tests read
// back through `aorta_trace_user_metric_count` to prove the C++ path actually
// landed samples in the engine, not just that it didn't crash.
//
// The header's inline-gate / RAII-timer logic is additionally compile-checked
// standalone (no link) in CI.

#include "aorta/trace.h"

#include <gtest/gtest.h>

namespace {

using aorta::trace::Level;
using aorta::trace::Scope;

// Resets the master gate around each test so ordering can't leak enabled state.
// Scope names are unique per test, so the leaked per-(scope,name) histograms
// never collide and counts are deltas from a clean zero.
class AortaTraceTest : public ::testing::Test {
 protected:
  void SetUp() override { aorta::trace::set_enabled(false); }
  void TearDown() override { aorta::trace::set_enabled(false); }
};

TEST_F(AortaTraceTest, GateOffSkipsRecord) {
  aorta::trace::set_enabled(false);
  Scope scope("cpp.test.off");
  scope.set_enabled(true);  // scope on, but master off
  EXPECT_FALSE(scope.enabled(Level::Info));

  const uint64_t before = scope.recorded_count("blk");
  { AORTA_TRACE_SCOPE(scope, Level::Info, "blk"); }
  // Inline gate is master-off -> the RAII timer is inactive -> nothing recorded.
  EXPECT_EQ(scope.recorded_count("blk"), before);
}

TEST_F(AortaTraceTest, ScopeTimerRecordsWhenEnabled) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.on");
  scope.set_enabled(true);
  EXPECT_TRUE(scope.enabled(Level::Info));

  const uint64_t before = scope.recorded_count("work");
  { AORTA_TRACE_SCOPE(scope, Level::Info, "work"); }
  EXPECT_EQ(scope.recorded_count("work") - before, 1u)
      << "an enabled RAII span must land exactly one sample in the engine";
}

TEST_F(AortaTraceTest, LevelThresholdGates) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.level");
  scope.set_enabled(true);  // threshold = Trace -> all levels enabled
  EXPECT_TRUE(scope.enabled(Level::Trace));
  EXPECT_TRUE(scope.enabled(Level::Error));
  scope.set_enabled(false);
  EXPECT_FALSE(scope.enabled(Level::Error));
}

TEST_F(AortaTraceTest, RecordLandsSampleWhenEnabled) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.record");
  scope.set_enabled(true);

  const uint64_t before = scope.recorded_count("lat");
  scope.record("lat", Level::Info, 1234);
  scope.record("lat", Level::Info, 5678);
  EXPECT_EQ(scope.recorded_count("lat") - before, 2u);
}

TEST_F(AortaTraceTest, InstantCountsOccurrences) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.instant");
  scope.set_enabled(true);

  const uint64_t before = scope.recorded_count("evt");
  scope.instant("evt", Level::Info);
  scope.instant("evt", Level::Info);
  scope.instant("evt", Level::Info);
  EXPECT_EQ(scope.recorded_count("evt") - before, 3u);
}

TEST_F(AortaTraceTest, CounterRecordsValues) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.counter");
  scope.set_enabled(true);

  const uint64_t before = scope.recorded_count("ctr");
  scope.counter("ctr", Level::Info, 42);
  EXPECT_EQ(scope.recorded_count("ctr") - before, 1u);
}

TEST_F(AortaTraceTest, InstantAndCounterSkipWhenDisabled) {
  aorta::trace::set_enabled(false);  // master off
  Scope scope("cpp.test.disabled.events");
  scope.set_enabled(true);

  const uint64_t inst_before = scope.recorded_count("evt");
  const uint64_t ctr_before = scope.recorded_count("ctr");
  scope.instant("evt", Level::Info);
  scope.counter("ctr", Level::Info, 7);
  EXPECT_EQ(scope.recorded_count("evt"), inst_before);
  EXPECT_EQ(scope.recorded_count("ctr"), ctr_before);
}

TEST_F(AortaTraceTest, DisabledScopeRecordsNothingWithMasterOn) {
  aorta::trace::set_enabled(true);  // master on...
  Scope scope("cpp.test.scope.off");
  scope.set_enabled(false);  // ...but this scope is off
  EXPECT_FALSE(scope.enabled(Level::Error));

  const uint64_t rec_before = scope.recorded_count("x");
  // All three are inline-gated on the scope (no FFI call when the scope is off);
  // the engine also gates internally as defense in depth.
  scope.record("x", Level::Info, 10);
  scope.instant("x", Level::Info);
  { AORTA_TRACE_SCOPE(scope, Level::Info, "x"); }  // inline-gated, inactive
  EXPECT_EQ(scope.recorded_count("x"), rec_before);
}

TEST_F(AortaTraceTest, MetricHandleRecordsLockFree) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.metric");
  scope.set_enabled(true);
  aorta::trace::Metric m = scope.metric("h", Level::Info);
  EXPECT_TRUE(m.valid());

  const uint64_t before = scope.recorded_count("h");
  m.record(111);
  m.record(222);
  EXPECT_EQ(scope.recorded_count("h") - before, 2u)
      << "the cached metric handle must record into the real engine";
}

TEST_F(AortaTraceTest, MetricHandleInlineGatesWhenDisabled) {
  aorta::trace::set_enabled(true);  // master on...
  Scope scope("cpp.test.metric.off");
  scope.set_enabled(false);  // ...scope off
  aorta::trace::Metric m = scope.metric("h", Level::Info);
  const uint64_t before = scope.recorded_count("h");
  m.record(111);  // inline-gated by the cached scope gate -> no record
  EXPECT_EQ(scope.recorded_count("h"), before);
}

TEST_F(AortaTraceTest, CachingMacrosRecordWhenEnabled) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.macros");
  scope.set_enabled(true);

  const uint64_t r0 = scope.recorded_count("rec");
  AORTA_TRACE_RECORD(scope, Level::Info, "rec", 123);
  AORTA_TRACE_RECORD(scope, Level::Info, "rec", 456);
  EXPECT_EQ(scope.recorded_count("rec") - r0, 2u);

  const uint64_t i0 = scope.recorded_count("inst");
  AORTA_TRACE_INSTANT(scope, Level::Info, "inst");
  EXPECT_EQ(scope.recorded_count("inst") - i0, 1u);

  const uint64_t c0 = scope.recorded_count("cnt");
  AORTA_TRACE_COUNTER(scope, Level::Info, "cnt", 7);
  EXPECT_EQ(scope.recorded_count("cnt") - c0, 1u);

  // The RAII span macro now records via the cached handle, not by name.
  const uint64_t s0 = scope.recorded_count("span");
  { AORTA_TRACE_SCOPE(scope, Level::Info, "span"); }
  EXPECT_EQ(scope.recorded_count("span") - s0, 1u);
}

TEST_F(AortaTraceTest, CachingMacroGatesBeforeEval) {
  aorta::trace::set_enabled(false);  // master off
  Scope scope("cpp.test.macros.eval");
  scope.set_enabled(true);
  int evals = 0;
  const uint64_t before = scope.recorded_count("rec");
  AORTA_TRACE_RECORD(scope, Level::Info, "rec", static_cast<uint64_t>(++evals));
  EXPECT_EQ(evals, 0) << "the value expression must not be evaluated when disabled";
  EXPECT_EQ(scope.recorded_count("rec"), before);
}

// --- Phase-2: waterfall span + dual-tier macros + context propagation ---

TEST_F(AortaTraceTest, SpanMacroRecordsAggregateWhenEnabled) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.span2");
  scope.set_enabled(true);
  // The unified span macro feeds BOTH the ring (waterfall) and the aggregate
  // histogram; we verify the aggregate side here (the ring side is covered by
  // the Rust tests + the end-to-end smoke).
  const uint64_t before = scope.recorded_count("blk");
  { AORTA_TRACE_SPAN(scope, Level::Info, "blk"); }
  EXPECT_EQ(scope.recorded_count("blk") - before, 1u)
      << "an enabled AORTA_TRACE_SPAN must land one aggregate sample";
}

TEST_F(AortaTraceTest, SpanMacroSkipsWhenDisabled) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.span2.off");
  scope.set_enabled(false);  // scope tier off
  const uint64_t before = scope.recorded_count("blk");
  { AORTA_TRACE_SPAN(scope, Level::Info, "blk"); }
  EXPECT_EQ(scope.recorded_count("blk"), before);
}

TEST_F(AortaTraceTest, SpanBeginEndTokenLifecycle) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.span2.tok");
  scope.set_enabled(true);
  const uint16_t ev = aorta_trace_intern_event("op");
  const uint64_t token =
      aorta_trace_span_begin(scope.handle(), ev, static_cast<uint8_t>(Level::Info), 0);
  EXPECT_NE(token, 0u) << "enabled span_begin returns a live token";
  // Balanced close (leaves the thread-local span stack empty).
  (void)aorta_trace_span_end(token);
  // A zero/unknown token is a harmless no-op.
  EXPECT_EQ(aorta_trace_span_end(0), 0u);

  aorta::trace::set_enabled(false);
  EXPECT_EQ(aorta_trace_span_begin(scope.handle(), ev,
                                   static_cast<uint8_t>(Level::Info), 0),
            0u)
      << "disabled span_begin returns the inert token";
}

TEST_F(AortaTraceTest, DualTierInstantAndCounterRecordAggregate) {
  aorta::trace::set_enabled(true);
  Scope scope("cpp.test.dual");
  scope.set_enabled(true);
  const uint64_t i0 = scope.recorded_count("blip");
  AORTA_TRACE_INSTANT(scope, Level::Info, "blip");
  EXPECT_EQ(scope.recorded_count("blip") - i0, 1u);

  const uint64_t c0 = scope.recorded_count("depth");
  AORTA_TRACE_COUNTER(scope, Level::Info, "depth", 9);
  EXPECT_EQ(scope.recorded_count("depth") - c0, 1u);
}

TEST_F(AortaTraceTest, ContextRoundTrips) {
  uint8_t in[16];
  for (int i = 0; i < 16; ++i) in[i] = static_cast<uint8_t>(0x20 + i);
  aorta::trace::set_context(in, 0xBEEFu);

  uint8_t out[16] = {0};
  uint64_t span = 0;
  EXPECT_EQ(aorta_trace_current_context(out, &span), 1u);
  for (int i = 0; i < 16; ++i) EXPECT_EQ(out[i], in[i]);
  EXPECT_EQ(span, 0xBEEFu);  // no open span -> falls back to the context parent

  aorta::trace::clear_context();
  EXPECT_EQ(aorta_trace_current_context(out, &span), 0u)
      << "cleared context reports no active trace";
}

TEST_F(AortaTraceTest, ContextGuardInstallsForItsScopeAndClears) {
  uint8_t trace_id[16];
  for (int i = 0; i < 16; ++i) trace_id[i] = static_cast<uint8_t>(0x40 + i);

  {
    aorta::trace::ContextGuard guard(trace_id, 0xCAFEu);
    const auto ctx = aorta::trace::current_context();
    ASSERT_TRUE(ctx.valid);
    for (int i = 0; i < 16; ++i) EXPECT_EQ(ctx.trace_id[i], trace_id[i]);
    EXPECT_EQ(ctx.span_id, 0xCAFEu);
  }
  EXPECT_FALSE(aorta::trace::current_context().valid)
      << "guard destruction must clear the thread's inbound context";
}

TEST_F(AortaTraceTest, ContextGuardFromContextStructRoundTrips) {
  aorta::trace::Context captured;
  for (int i = 0; i < 16; ++i) captured.trace_id[i] = static_cast<uint8_t>(0x60 + i);
  captured.span_id = 0xF00Du;
  captured.valid = true;

  {
    aorta::trace::ContextGuard guard(captured);
    const auto ctx = aorta::trace::current_context();
    ASSERT_TRUE(ctx.valid);
    for (int i = 0; i < 16; ++i) EXPECT_EQ(ctx.trace_id[i], captured.trace_id[i]);
    EXPECT_EQ(ctx.span_id, captured.span_id);
  }
  EXPECT_FALSE(aorta::trace::current_context().valid);
}

TEST_F(AortaTraceTest, InvalidContextGuardIsANoOpAndPreservesAnInstalledContext) {
  uint8_t trace_id[16];
  for (int i = 0; i < 16; ++i) trace_id[i] = static_cast<uint8_t>(0x80 + i);
  aorta::trace::set_context(trace_id, 0xD00Du);

  {
    aorta::trace::ContextGuard guard(aorta::trace::Context{});
    // An invalid Context installs nothing — the pre-existing context stays.
    EXPECT_TRUE(aorta::trace::current_context().valid);
  }
  // ...and its destructor must NOT have cleared what it never installed.
  const auto ctx = aorta::trace::current_context();
  ASSERT_TRUE(ctx.valid);
  EXPECT_EQ(ctx.span_id, 0xD00Du);

  aorta::trace::clear_context();
}

}  // namespace
