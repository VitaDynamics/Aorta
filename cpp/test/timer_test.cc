// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Timer class and chrono conversions

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <thread>

namespace aorta {
namespace {

using namespace std::chrono;

// ==================== Timer Duration Conversion Tests ====================

// Test nanoseconds conversion
TEST(TimerDurationTest, Nanoseconds) {
  nanoseconds ns(1000);
  auto count = ns.count();
  EXPECT_EQ(count, 1000);
}

// Test microseconds conversion
TEST(TimerDurationTest, Microseconds) {
  microseconds us(100);
  auto ns = duration_cast<nanoseconds>(us);
  EXPECT_EQ(ns.count(), 100000);
}

// Test milliseconds conversion
TEST(TimerDurationTest, Milliseconds) {
  milliseconds ms(10);
  auto ns = duration_cast<nanoseconds>(ms);
  EXPECT_EQ(ns.count(), 10000000);
}

// Test seconds conversion
TEST(TimerDurationTest, Seconds) {
  seconds s(1);
  auto ns = duration_cast<nanoseconds>(s);
  EXPECT_EQ(ns.count(), 1000000000);
}

// Test minutes conversion
TEST(TimerDurationTest, Minutes) {
  minutes m(1);
  auto ns = duration_cast<nanoseconds>(m);
  EXPECT_EQ(ns.count(), 60000000000LL);
}

// Test various timer periods
TEST(TimerDurationTest, VariousPeriods) {
  // 100 microseconds
  {
    auto period = microseconds(100);
    auto ns = duration_cast<nanoseconds>(period);
    EXPECT_EQ(ns.count(), 100000);
  }

  // 1 millisecond
  {
    auto period = milliseconds(1);
    auto ns = duration_cast<nanoseconds>(period);
    EXPECT_EQ(ns.count(), 1000000);
  }

  // 10 milliseconds
  {
    auto period = milliseconds(10);
    auto ns = duration_cast<nanoseconds>(period);
    EXPECT_EQ(ns.count(), 10000000);
  }

  // 100 milliseconds
  {
    auto period = milliseconds(100);
    auto ns = duration_cast<nanoseconds>(period);
    EXPECT_EQ(ns.count(), 100000000);
  }

  // 1 second
  {
    auto period = seconds(1);
    auto ns = duration_cast<nanoseconds>(period);
    EXPECT_EQ(ns.count(), 1000000000);
  }
}

// Test zero duration
TEST(TimerDurationTest, ZeroDuration) {
  nanoseconds ns(0);
  EXPECT_EQ(ns.count(), 0);
}

// Test negative duration (edge case for comparisons)
TEST(TimerDurationTest, NegativeDuration) {
  nanoseconds ns(-1000);
  EXPECT_EQ(ns.count(), -1000);
}

// Test maximum reasonable timer period
TEST(TimerDurationTest, MaxReasonablePeriod) {
  // 1 hour in nanoseconds
  auto period = hours(1);
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 3600000000000LL);
}

// Test very small period
TEST(TimerDurationTest, VerySmallPeriod) {
  // 1 microsecond
  auto period = microseconds(1);
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 1000);
}

// ==================== Timer Callback Tests ====================

// Test timer callback type
TEST(TimerCallbackTest, CallbackType) {
  // Timer::Callback type: std::function<void()>
  using TimerCallback = std::function<void()>;

  TimerCallback callback = []() {
    // Timer callback implementation
  };

  // Verify callback can be invoked
  callback();
}

// Test timer callback with captures
TEST(TimerCallbackTest, CallbackWithCaptures) {
  int counter = 0;
  std::function<void()> callback = [&counter]() { ++counter; };

  callback();
  EXPECT_EQ(counter, 1);

  callback();
  EXPECT_EQ(counter, 2);
}

// Test timer callback with mutable captures
TEST(TimerCallbackTest, CallbackWithMutableCaptures) {
  int counter = 0;
  std::string message = "test";

  std::function<void()> callback = [&counter, &message]() mutable {
    ++counter;
    message += "_called";
  };

  callback();
  EXPECT_EQ(counter, 1);
  EXPECT_EQ(message, "test_called");
}

// Test empty timer callback
TEST(TimerCallbackTest, EmptyCallback) {
  std::function<void()> callback = []() {
    // Empty callback
  };

  // Should not throw
  EXPECT_NO_THROW(callback());
}

// ==================== Chrono Integration Tests ====================

// Test steady_clock with timer periods
TEST(TimerChronoTest, SteadyClock) {
  auto start = steady_clock::now();
  auto period = milliseconds(10);
  auto end = start + period;

  auto duration = end - start;
  auto ns = duration_cast<nanoseconds>(duration);
  EXPECT_EQ(ns.count(), 10000000);
}

// Test system_clock with timer periods
TEST(TimerChronoTest, SystemClock) {
  auto start = system_clock::now();
  auto period = milliseconds(100);
  auto end = start + period;

  auto duration = end - start;
  auto ms = duration_cast<milliseconds>(duration);
  EXPECT_EQ(ms.count(), 100);
}

// Test duration arithmetic
TEST(TimerChronoTest, DurationArithmetic) {
  auto period1 = milliseconds(100);
  auto period2 = milliseconds(50);

  auto sum = period1 + period2;
  EXPECT_EQ(duration_cast<milliseconds>(sum).count(), 150);

  auto diff = period1 - period2;
  EXPECT_EQ(duration_cast<milliseconds>(diff).count(), 50);
}

// Test duration comparison
TEST(TimerChronoTest, DurationComparison) {
  auto short_period = milliseconds(10);
  auto long_period = milliseconds(100);

  EXPECT_LT(short_period, long_period);
  EXPECT_GT(long_period, short_period);
  EXPECT_NE(short_period, long_period);
}

// Test duration casting
TEST(TimerChronoTest, DurationCasting) {
  auto ms = milliseconds(1500);

  auto s = duration_cast<seconds>(ms);
  EXPECT_EQ(s.count(), 1);  // Truncated

  auto us = duration_cast<microseconds>(ms);
  EXPECT_EQ(us.count(), 1500000);
}

// ==================== Real-world Timer Periods ====================

// Test high-frequency timer (1000 Hz)
TEST(TimerRealWorldTest, HighFrequency1kHz) {
  auto period = microseconds(1000);  // 1ms = 1000Hz
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 1000000);
}

// Test medium-frequency timer (100 Hz)
TEST(TimerRealWorldTest, MediumFrequency100Hz) {
  auto period = milliseconds(10);  // 10ms = 100Hz
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 10000000);
}

// Test low-frequency timer (10 Hz)
TEST(TimerRealWorldTest, LowFrequency10Hz) {
  auto period = milliseconds(100);  // 100ms = 10Hz
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 100000000);
}

// Test very low-frequency timer (1 Hz)
TEST(TimerRealWorldTest, VeryLowFrequency1Hz) {
  auto period = seconds(1);  // 1s = 1Hz
  auto ns = duration_cast<nanoseconds>(period);
  EXPECT_EQ(ns.count(), 1000000000);
}

// Test control loop timer period (typical robotics)
TEST(TimerRealWorldTest, ControlLoopPeriod) {
  // Common control loop frequencies in robotics:
  // 1000Hz for motor control
  auto motor_period = microseconds(1000);
  EXPECT_EQ(duration_cast<nanoseconds>(motor_period).count(), 1000000);

  // 100Hz for sensor fusion
  auto sensor_period = milliseconds(10);
  EXPECT_EQ(duration_cast<nanoseconds>(sensor_period).count(), 10000000);

  // 10Hz for state estimation
  auto state_period = milliseconds(100);
  EXPECT_EQ(duration_cast<nanoseconds>(state_period).count(), 100000000);
}

// ==================== Edge Cases ====================

// Test minimum timer period
TEST(TimerEdgeCaseTest, MinimumPeriod) {
  // 1 nanosecond (not practical but test the conversion)
  auto period = nanoseconds(1);
  EXPECT_EQ(period.count(), 1);
}

// Test sub-microsecond period
TEST(TimerEdgeCaseTest, SubMicrosecondPeriod) {
  auto period = nanoseconds(500);
  EXPECT_EQ(period.count(), 500);
}

// Test floating point duration (if supported)
TEST(TimerEdgeCaseTest, FloatingPointDuration) {
  // Using duration<double>
  duration<double> seconds_double(1.5);
  auto ns = duration_cast<nanoseconds>(seconds_double);
  EXPECT_EQ(ns.count(), 1500000000);
}

}  // namespace
}  // namespace aorta
