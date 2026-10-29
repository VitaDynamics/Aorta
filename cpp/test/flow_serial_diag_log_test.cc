// SerialDomain — the diagnostics' REAL log
// contract: level, message, and COUNT asserted against a file sink
// (latches alone cannot catch a level regression, a deleted log call
// behind a surviving latch, or a repeat-logging break). Own binary: the
// process-wide logging OnceLock is claimed here with filter="debug" so
// DEBUG/WARN both reach the sink.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"
#include "aorta/flow_readiness.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/flow_subscriber.hpp"
#include "aorta/logging.h"
#include "aorta/node.h"
#include "aorta/subscriber.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

std::string LogFilePath() {
  static std::string path = [] {
    std::string p = ::testing::TempDir() + "flow_serial_diag_log.txt";
    std::remove(p.c_str());
    return p;
  }();
  return path;
}

std::string ReadLogFile() {
  std::ifstream in(LogFilePath());
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

size_t CountOccurrences(const std::string& hay, const std::string& needle) {
  size_t n = 0;
  for (size_t pos = hay.find(needle); pos != std::string::npos;
       pos = hay.find(needle, pos + needle.size())) {
    n++;
  }
  return n;
}

bool WaitForCount(const std::string& needle, size_t want,
                  std::chrono::seconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (CountOccurrences(ReadLogFile(), needle) >= want) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

/// The one line carrying BOTH the level token and the message must
/// exist — a level regression moves the message to a different line
/// prefix and this check fails.
bool LogHasLevelAndMessage(const std::string& level,
                           const std::string& needle) {
  std::istringstream in(ReadLogFile());
  std::string line;
  while (std::getline(in, line)) {
    if (line.find(level) != std::string::npos &&
        line.find(needle) != std::string::npos) {
      return true;
    }
  }
  return false;
}

/// Sentinel-bounded WINDOWS: the file writer is a
/// non-blocking ASYNC appender — without a flush barrier, a delayed
/// line from a previous leg could impersonate the next leg's log. The
/// appender is FIFO, so an ERROR sentinel emitted AFTER a leg and
/// awaited on disk bounds everything the leg wrote. `Close()` returns
/// exactly the window's content.
class LogWindow {
 public:
  explicit LogWindow(std::string tag) : tag_(std::move(tag)) {
    start_ = SyncedSize();
  }
  /// Emit + await the closing sentinel; return the window's content.
  std::string Close() {
    const std::string sentinel = "window-sentinel-" + tag_;
    (void)aorta::WriteLogAt(aorta::LogLevel::kError, "aorta.flow", __FILE__,
                            __LINE__, sentinel);
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
      const std::string all = ReadLogFile();
      const size_t pos = all.find(sentinel, start_);
      if (pos != std::string::npos) {
        return all.substr(start_, pos - start_);
      }
      std::this_thread::sleep_for(10ms);
    }
    ADD_FAILURE() << "window sentinel never flushed: " << sentinel;
    return {};
  }

 private:
  /// The stable start boundary: everything BEFORE this window was
  /// bounded by the previous sentinel (or the suite probe), so the
  /// current size is a safe cut.
  size_t SyncedSize() { return ReadLogFile().size(); }

  std::string tag_;
  size_t start_ = 0;
};

size_t CountLinesWith(const std::string& hay, const std::string& a,
                      const std::string& b) {
  std::istringstream in(hay);
  std::string line;
  size_t n = 0;
  while (std::getline(in, line)) {
    if (line.find(a) != std::string::npos &&
        line.find(b) != std::string::npos) {
      n++;
    }
  }
  return n;
}

class SerialDiagLogHarness : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    aorta::LogConfig cfg;
    cfg.filter = "debug";  // DEBUG + WARN must reach the sink
    cfg.file_path = LogFilePath();
    cfg.stderr_enabled = false;
    ASSERT_TRUE(aorta::ConfigureLogging(cfg).ok());
    AORTA_LOG(ERROR) << "serial-diag-log-probe";
    ASSERT_TRUE(WaitForCount("serial-diag-log-probe", 1))
        << "log file sink did not come up at " << LogFilePath();
  }
};

constexpr const char* kF3Needle =
    "priority is above the domain's declared class";
constexpr const char* kF4Needle =
    "unbound spawn from inside a serial-domain drain slice";
constexpr const char* kF6Needle =
    "blocking TaskHandle::wait_for inside a serial-domain drain";

flow::Task<void> noop_task(std::atomic<bool>* ran) {
  if (ran != nullptr) {
    ran->store(true, std::memory_order_seq_cst);
  }
  co_return;
}

// ---------------------------------------------------------------------------
// F-3: WARN level, once PER DOMAIN, only on real MT strands; the bound
// member provably lands on the DOMAIN lane.
// ---------------------------------------------------------------------------

TEST_F(SerialDiagLogHarness, F3WarnOncePerDomainOnMtStrandsOnly) {
  auto rt = flow::Runtime::MultiThread({0, 0, 0, 1});
  ASSERT_TRUE(rt.ok());
  auto d1 = rt->create_serial(aorta::EventPriority::kLow, "diag-d1");
  ASSERT_TRUE(d1.valid());
  flow::Scope scope = rt->root_scope();

  // WINDOW 1 (sentinel-bounded): two over-urgent spawns
  // into d1 → exactly ONE WARN line NAMING d1.
  const uint64_t lane3_before = rt->lanes().pool().stats(3).enqueued;
  LogWindow w1("f3-d1");
  auto h1 = rt->spawn(scope, d1, noop_task(nullptr));
  auto h2 = rt->spawn(scope, d1, noop_task(nullptr));
  ASSERT_TRUE(h1.ok());
  ASSERT_TRUE(h2.ok());
  ASSERT_TRUE(h1.wait_for(10s));
  ASSERT_TRUE(h2.wait_for(10s));
  const std::string win1 = w1.Close();
  EXPECT_EQ(CountLinesWith(win1, kF3Needle, ""), 1u)
      << "exactly ONE F-3 line total in the d1 window";
  EXPECT_EQ(CountLinesWith(win1, kF3Needle, "diag-d1"), 1u)
      << "F-3 must log exactly once for d1 in its own window";
  EXPECT_EQ(CountLinesWith(win1, kF3Needle, "diag-d2"), 0u);
  EXPECT_EQ(CountLinesWith(win1, "WARN", kF3Needle), 1u)
      << "the d1 line must carry WARN";
  EXPECT_GE(rt->lanes().pool().stats(3).enqueued, lane3_before + 1)
      << "the bound member must land on the DOMAIN lane (kLow = 3) — "
         "the monotone claim 'executes at the domain class'";

  // WINDOW 2: a SECOND domain fires its OWN one-shot, named d2.
  auto d2 = rt->create_serial(aorta::EventPriority::kLow, "diag-d2");
  LogWindow w2("f3-d2");
  auto h3 = rt->spawn(scope, d2, noop_task(nullptr));
  ASSERT_TRUE(h3.ok());
  ASSERT_TRUE(h3.wait_for(10s));
  const std::string win2 = w2.Close();
  EXPECT_EQ(CountLinesWith(win2, kF3Needle, ""), 1u)
      << "exactly ONE F-3 line total in the d2 window";
  EXPECT_EQ(CountLinesWith(win2, kF3Needle, "diag-d2"), 1u)
      << "F-3 is per-DOMAIN — d2 logs its own WARN in its own window";
  EXPECT_EQ(CountLinesWith(win2, "WARN", kF3Needle), 1u);
  ASSERT_TRUE(scope.close().ok());

  // NEGATIVE WINDOW: no-op domains (ST + rollback) must
  // stay SILENT — their tasks genuinely dispatch at their own class.
  LogWindow w3("f3-negative");
  {
    auto st_rt = flow::Runtime::SingleThread();
    ASSERT_TRUE(st_rt.ok());
    auto d = st_rt->create_serial(aorta::EventPriority::kLow, "st-noop");
    flow::Scope s2 = st_rt->root_scope();
    auto h = st_rt->spawn(s2, d, noop_task(nullptr));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    const auto& stt = flow::detail::serial_access::state(d);
    EXPECT_FALSE(stt->monotone_warned.load(std::memory_order_acquire));
    ASSERT_TRUE(s2.close().ok());
  }
  {
    ::setenv("AORTA_CPP_LANES", "0", 1);
    auto rb = flow::Runtime::FromEnv();
    ::unsetenv("AORTA_CPP_LANES");
    ASSERT_TRUE(rb.ok());
    auto d = rb->create_serial(aorta::EventPriority::kLow, "rb-noop");
    flow::Scope s3 = rb->root_scope();
    auto h = rb->spawn(s3, d, noop_task(nullptr));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    const auto& stt = flow::detail::serial_access::state(d);
    EXPECT_FALSE(stt->monotone_warned.load(std::memory_order_acquire));
    ASSERT_TRUE(s3.close().ok());
  }
  const std::string win3 = w3.Close();
  EXPECT_EQ(CountLinesWith(win3, kF3Needle, ""), 0u)
      << "no-op domains must not fire the F-3 WARN";
}

// ---------------------------------------------------------------------------
// F-4: DEBUG level, once per latch window, BOTH spawn_detached
// overloads; silent outside a slice.
// ---------------------------------------------------------------------------

flow::Task<void> lane_escape(const flow::Runtime* rt, flow::Scope scope,
                             std::atomic<bool>* child_ran) {
  auto h = rt->spawn_with(
      scope, [](flow::Scope, std::atomic<bool>* r) -> flow::Task<void> {
        r->store(true, std::memory_order_seq_cst);
        co_return;
      },
      child_ran);
  std::vector<flow::TaskHandle<void>> hv;
  hv.push_back(std::move(h));
  auto joined = co_await flow::when_all(std::move(hv));
  if (!joined.ok()) {
    throw std::runtime_error("lane_escape: child join was not ok");
  }
  co_return;
}

flow::Task<void> executor_escape(uint64_t exec, flow::Scope scope,
                                 std::atomic<bool>* child_ran) {
  // The EXECUTOR-plane overload from inside the slice (required:
  // both overloads must call the aid).
  auto h = flow::spawn_on(exec, scope, noop_task(child_ran));
  std::vector<flow::TaskHandle<void>> hv;
  hv.push_back(std::move(h));
  auto joined = co_await flow::when_all(std::move(hv));
  if (!joined.ok()) {
    throw std::runtime_error("executor_escape: child join was not ok");
  }
  co_return;
}

TEST_F(SerialDiagLogHarness, F4DebugOnceThroughBothOverloads) {
  auto rt = flow::Runtime::MultiThread({0, 0, 2, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "f4-log");
  flow::Scope scope = rt->root_scope();

  // WINDOW 1: the LANE overload — exactly one DEBUG, zero WARN.
  flow::detail::g_serial_f4_logged.store(false, std::memory_order_seq_cst);
  LogWindow w1("f4-lane");
  std::atomic<bool> child1{false};
  auto h1 = rt->spawn(scope, d, lane_escape(&*rt, scope, &child1));
  ASSERT_TRUE(h1.ok());
  ASSERT_TRUE(h1.wait_for(10s));
  auto o1 = h1.take_outcome();
  ASSERT_TRUE(o1.has_value());
  EXPECT_TRUE(o1->ok()) << "lane_escape parent must complete ok";
  // Once-per-latch-window: a second escape in the SAME window is silent.
  std::atomic<bool> child2{false};
  auto h2 = rt->spawn(scope, d, lane_escape(&*rt, scope, &child2));
  ASSERT_TRUE(h2.ok());
  ASSERT_TRUE(h2.wait_for(10s));
  auto o2 = h2.take_outcome();
  ASSERT_TRUE(o2.has_value());
  EXPECT_TRUE(o2->ok()) << "second lane_escape parent must complete ok";
  const std::string win1 = w1.Close();
  EXPECT_EQ(CountLinesWith(win1, kF4Needle, ""), 1u)
      << "exactly ONE F-4 line total in the lane window (no extra "
         "line at any other level)";
  EXPECT_EQ(CountLinesWith(win1, "DEBUG", kF4Needle), 1u)
      << "the lane overload logs F-4 exactly once, at DEBUG";
  EXPECT_EQ(CountLinesWith(win1, "WARN", kF4Needle), 0u)
      << "F-4 must never read as anomalous (WARN)";

  // WINDOW 2: the EXECUTOR overload (fresh latch) — its OWN line must
  // also be DEBUG (the level is asserted per overload).
  flow::detail::g_serial_f4_logged.store(false, std::memory_order_seq_cst);
  LogWindow w2("f4-executor");
  std::atomic<bool> child3{false};
  auto h3 = rt->spawn(scope, d,
                      executor_escape(rt->executor(), scope, &child3));
  ASSERT_TRUE(h3.ok());
  ASSERT_TRUE(h3.wait_for(10s));
  auto o3 = h3.take_outcome();
  ASSERT_TRUE(o3.has_value());
  EXPECT_TRUE(o3->ok()) << "executor_escape parent must complete ok";
  const std::string win2 = w2.Close();
  EXPECT_EQ(CountLinesWith(win2, kF4Needle, ""), 1u)
      << "exactly ONE F-4 line total in the executor window";
  EXPECT_EQ(CountLinesWith(win2, "DEBUG", kF4Needle), 1u)
      << "the EXECUTOR overload must call the F-4 aid, at DEBUG";
  EXPECT_EQ(CountLinesWith(win2, "WARN", kF4Needle), 0u);

  // NEGATIVE WINDOW: an unbound spawn OUTSIDE any slice stays silent.
  flow::detail::g_serial_f4_logged.store(false, std::memory_order_seq_cst);
  LogWindow w3("f4-negative");
  std::atomic<bool> child4{false};
  auto h4 = rt->spawn(scope, noop_task(&child4));
  ASSERT_TRUE(h4.ok());
  ASSERT_TRUE(h4.wait_for(10s));
  const std::string win3 = w3.Close();
  EXPECT_EQ(CountLinesWith(win3, kF4Needle, ""), 0u)
      << "an off-slice unbound spawn must not log F-4";
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// F-6: WARN level, the SAME-DOMAIN done-handle shape; silent off-slice.
// ---------------------------------------------------------------------------

flow::Task<void> wait_on_done_sibling(flow::TaskHandle<void>* sibling) {
  (void)sibling->wait_for(1s);  // inside the slice: fires the aid
  co_return;
}

TEST_F(SerialDiagLogHarness, F6WarnOnceOnSameDomainDoneHandle) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "f6-log");
  flow::Scope scope = rt->root_scope();

  // The SAME-DOMAIN sibling completes FIRST (the strand serializes:
  // spawn it alone and wait), so the in-slice wait_for below returns
  // immediately — the CHECK, not the deadlock, is under test.
  std::atomic<bool> sib_done{false};
  auto sibling = rt->spawn(scope, d, noop_task(&sib_done));
  ASSERT_TRUE(sibling.ok());
  ASSERT_TRUE(sibling.wait_for(10s));

  flow::detail::g_serial_f6_warned.store(false, std::memory_order_seq_cst);
  LogWindow w1("f6-in-slice");
  auto h = rt->spawn(scope, d, wait_on_done_sibling(&sibling));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(10s));
  const std::string win1 = w1.Close();
  EXPECT_EQ(CountLinesWith(win1, kF6Needle, ""), 1u)
      << "exactly ONE F-6 line total in the window";
  EXPECT_EQ(CountLinesWith(win1, "WARN", kF6Needle), 1u)
      << "F-6 logs exactly once, at WARN, in its own window";

  // NEGATIVE WINDOW: wait_for OFF-slice (this thread) stays silent.
  flow::detail::g_serial_f6_warned.store(false, std::memory_order_seq_cst);
  LogWindow w2("f6-negative");
  (void)sibling.wait_for(1ms);
  const std::string win2 = w2.Close();
  EXPECT_EQ(CountLinesWith(win2, kF6Needle, ""), 0u)
      << "an off-slice wait_for must not log F-6";
  ASSERT_TRUE(scope.close().ok());
}


// ---------------------------------------------------------------------------
// The not-ready foreign-arm refusals' IDENTITY — each
// surface must log the F-2 refusal (ERROR, "[serial: foreign runtime
// arm]", INVALID_ARGUMENT), not just deliver some cancellation.
// ---------------------------------------------------------------------------

flow::Task<void> notready_recv(aorta::PullSubscriber& sub,
                               flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::recv(sub, *rtb, std::move(scope));
  co_return;
}

flow::Task<void> notready_aligned(flow::WindowSync* ws,
                                  flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::aligned(*ws, *rtb, std::move(scope));
  co_return;
}

flow::Task<void> notready_wave(flow::ReadinessSet* set,
                               flow::LaneRuntime* rtb, flow::Scope scope) {
  (void)co_await flow::wait_wave(*set, *rtb, std::move(scope));
  co_return;
}

TEST_F(SerialDiagLogHarness, NotReadyForeignArmRefusalsLogTheF2Identity) {
  auto node = aorta::Node::Create("diag_notready");
  ASSERT_TRUE(node.ok());
  auto sub = node.value()->CreatePullSubscriber("test/diag_notready", 4);
  ASSERT_TRUE(sub.ok());
  auto pull2 = node.value()->CreatePullSubscriber("test/diag_notready2", 4);
  ASSERT_TRUE(pull2.ok());
  auto ws = flow::WindowSync::create(1, 1s);
  ASSERT_TRUE(ws.ok());
  ASSERT_EQ(flow::bind_window(pull2.value(), *ws, 0), AORTA_OK);
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());

  auto rta = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rta.ok());
  auto rtb = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rtb.ok());
  auto d = rta->create_serial(aorta::EventPriority::kNormal, "notready");
  ASSERT_TRUE(d.valid());
  flow::Scope scope = rta->root_scope();

  struct Leg {
    const char* tag;
    const char* surface;
    flow::Task<void> (*mk_recv)(aorta::PullSubscriber&, flow::LaneRuntime*,
                                flow::Scope);
  };
  // recv leg
  {
    LogWindow w("f2-recv");
    auto h = rta->spawn(scope, d,
                        notready_recv(sub.value(), &rtb->lanes(), scope));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    auto o = h.take_outcome();
    ASSERT_TRUE(o.has_value());
    EXPECT_FALSE(o->ok());
    const std::string win = w.Close();
    EXPECT_EQ(CountLinesWith(win, "flow::recv [serial: foreign runtime arm]",
                             "INVALID_ARGUMENT"),
              1u)
        << "the recv refusal must be the F-2 identity, logged at the "
           "refusal site";
    EXPECT_EQ(CountLinesWith(win, "ERROR",
                             "flow::recv [serial: foreign runtime arm]"),
              1u)
        << "the refusal line carries ERROR";
  }
  // aligned leg
  {
    LogWindow w("f2-aligned");
    auto h = rta->spawn(scope, d,
                        notready_aligned(&*ws, &rtb->lanes(), scope));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    auto o = h.take_outcome();
    ASSERT_TRUE(o.has_value());
    EXPECT_FALSE(o->ok());
    const std::string win = w.Close();
    EXPECT_EQ(
        CountLinesWith(win, "flow::readiness [serial: foreign runtime arm]",
                       "INVALID_ARGUMENT"),
        1u)
        << "the aligned refusal must be the F-2 identity";
  }
  // wait_wave leg
  {
    LogWindow w("f2-wave");
    auto h = rta->spawn(scope, d,
                        notready_wave(&*set, &rtb->lanes(), scope));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(h.wait_for(10s));
    auto o = h.take_outcome();
    ASSERT_TRUE(o.has_value());
    EXPECT_FALSE(o->ok());
    const std::string win = w.Close();
    EXPECT_EQ(
        CountLinesWith(win, "flow::readiness [serial: foreign runtime arm]",
                       "INVALID_ARGUMENT"),
        1u)
        << "the wait_wave refusal must be the F-2 identity";
  }
  ASSERT_TRUE(scope.close().ok());
}

// ---------------------------------------------------------------------------
// Failure model: EVERY create_serial failure path is loud. The
// already-stopped-executor path WARNs and returns the DEFAULT handle
// like the other two (it used to return a
// constructed, silently-invalid, name-carrying domain).
// ---------------------------------------------------------------------------

TEST_F(SerialDiagLogHarness, CreateSerialAfterStopWarnsAndReturnsDefault) {
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  ASSERT_EQ(aorta_executor_stop(rt->executor()), AORTA_OK);
  LogWindow w("create-after-stop");
  auto d = rt->create_serial(aorta::EventPriority::kNormal, "late");
  const std::string win = w.Close();
  EXPECT_FALSE(d.valid());
  EXPECT_STREQ(d.name(), "")
      << "default invalid handle, not a constructed state";
  EXPECT_EQ(CountLinesWith(win, "WARN", "already-stopped"), 1u)
      << "the stopped-executor failure path must WARN (loud-invalid "
         "failure model)";
}
}  // namespace
