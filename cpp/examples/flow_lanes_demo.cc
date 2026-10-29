// Flow golden-path demo: the C++ plane on
// the AortaLanes pool — combined ST wait via the executor facade,
// in-task combinators over the completion arbiter, structured teardown.
//
// ONE setup object. flow::Runtime owns the executor, the lane
// runtime, the facade attachment and the spin thread; its destructor IS
// the teardown (stop → drain → detach → destroy, in that order, always).
// spawn returns a TaskHandle — the demo waits on the HANDLE
// (wait_for → take_outcome), THEN closes the scope; no done-flags, no
// poll loops. (Closing first would bar the still-running task's own
// combinator branch spawns — the closing bar is subtree-wide.)
// The coroutine BODIES thread zero {rt, exec, scope} — spawn
// stamps the context into the promise and the ambient verbs read it:
//
//   auto [imu, pcl] = co_await flow::join(hub_imu(), hub_lidar());
//   auto winner     = co_await flow::race(primary(), backup());
//   auto grid       = co_await flow::offload(aorta::EventPriority::kBackground, rasterize);
//   auto loc        = co_await flow::timeout(50ms, localize());
//
// Build/run: bazel run //cpp/examples:flow_lanes_demo
#include <memory>
#include <string>

#include "aorta/flow_readiness.hpp"
#include "aorta/flow_runtime.hpp"
#include "aorta/flow_subscriber.hpp"
#include "aorta/node.h"
#include "aorta/publisher.h"
#include <chrono>
#include <cstdio>
#include <thread>

#include <aorta/flow_combinators.hpp>

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

flow::Task<int> read_imu() {
  co_await flow::sleep(5ms);  // sensor latency (arbiter timer, ambient)
  co_return 10;
}

flow::Task<int> read_lidar() {
  co_await flow::sleep(8ms);
  co_return 32;
}

flow::Task<int> primary_source() {
  co_await flow::sleep(3ms);
  co_return 1;
}

flow::Task<int> backup_source() {
  co_await flow::sleep(100ms);  // loses the race, drains structurally
  co_return 2;
}

flow::Task<int> localize() {
  co_await flow::sleep(10ms);  // well inside the 50ms window
  co_return 7;
}

flow::Task<int> golden_path() {
  // 1. join — both sensors, one await, zero extra machinery. {rt, scope}
  //    come from this frame's promise context (stamped at spawn).
  auto [imu, pcl] = co_await flow::join(read_imu(), read_lidar());
  std::printf("[flow] join: imu=%d lidar=%d\n", imu, pcl);

  // 2. race — first source wins; the loser drains under the scope.
  int winner = co_await flow::race(primary_source(), backup_source());
  std::printf("[flow] race winner: source %d\n", winner);

  // 3. offload — hop a CPU-bound closure to the background lane.
  int grid = co_await flow::offload(aorta::EventPriority::kBackground,
                                    [&] { return imu + pcl; });
  std::printf("[flow] offload(background): grid=%d\n", grid);

  // 4. timeout — bounded localization (the timer arms core-side; a
  //    task win revokes it, aborting the sleeper).
  int loc = co_await flow::timeout(50ms, localize());
  std::printf("[flow] timeout(50ms): loc=%d\n", loc);

  std::printf("[flow] fused: %d\n", winner + grid + loc);
  co_return winner + grid + loc;
}

// 5. co_await flow::recv(sub) — the consumption verb,
//    ambient form. A lane frame parks on the subscriber; the publisher
//    wakes it through the RX notify hook; the CONTEXT scope's cancel
//    resolves the park (cancel-safe by default). Deadline variant:
//    flow::recv_for(sub, window) — do NOT wrap the raw timeout()
//    combinator around recv() (compile error: its loser branch would
//    strand with no data and no cancel path).
flow::Task<int> consume_two(aorta::PullSubscriber& sub) {
  int consumed = 0;
  for (int i = 0; i < 2; i++) {
    aorta::PullSample s = co_await flow::recv(sub);
    std::printf("[flow] recv(): sample %d, %zu bytes\n", i + 1, s.Size());
    consumed++;
  }
  co_return consumed;
}

// 6. co_await aligned(window) — two bound subscribers feed a
//    WindowSync on the RX path (zero glue); the lane frame parks until a
//    fresh pair aligns. The VLN person-follow shape: N sensor streams,
//    act on "one fresh sample of each" — no poll loop anywhere.
flow::Task<uint64_t> fuse_once(flow::WindowSync& ws) {
  flow::AlignedSet set = co_await flow::aligned(ws);
  const auto cam = set.at(0);
  const auto det = set.at(1);
  const uint64_t spread = cam.ts_ns > det.ts_ns ? cam.ts_ns - det.ts_ns
                                                : det.ts_ns - cam.ts_ns;
  co_return spread / 1000;
}

}  // namespace

int main() {
  // The WHOLE Flow wiring — executor, lane runtime, facade, spin thread —
  // is one object now. AORTA_CPP_LANES=0 still selects the queue-path
  // rollback transparently.
  auto rt = flow::Runtime::FromEnv();
  if (!rt.ok()) {
    // Runtime::FromEnv()/create() only ever stops with a fault.
    std::fprintf(stderr, "flow runtime init failed: %d (%s)\n",
                 rt.stopped().fault().code(),
                 rt.stopped().fault().message().c_str());
    return 1;
  }
  std::printf("[flow] lanes: %s\n",
              rt->lanes_attached() ? "attached" : "queue-path rollback");

  // Leg 1: the combinator golden path. Wait on the HANDLE (blocking —
  // main is not a coroutine), read the outcome, then close the scope.
  // Order matters: close() bars the still-running task's own combinator
  // branch spawns, so the wait comes first.
  flow::Scope scope = rt->root_scope();
  auto fused = rt->spawn(scope, golden_path());
  if (!fused.ok()) {
    std::fprintf(stderr, "spawn refused: %d\n", fused.status);
    return 1;
  }
  if (!fused.wait_for(10s)) {
    std::fprintf(stderr, "[flow] golden path did not complete\n");
    return 1;
  }
  auto fused_out = fused.take_outcome();
  if (!fused_out.has_value() || !fused_out->ok()) {
    std::fprintf(stderr, "[flow] golden path stopped early\n");
    return 1;
  }
  if (!scope.close().ok()) {
    std::fprintf(stderr, "scope close failed\n");
    return 1;
  }

  // Leg 2+3: watch/recv + readiness over real topics (needs a zenoh
  // session; degrades gracefully without one).
  {
    auto node = aorta::Node::Create("flow_lanes_demo");
    if (node.ok()) {
      auto sub = node.value()->CreatePullSubscriber("demo/flow_recv", 4);
      auto pub = node.value()->CreatePublisher("demo/flow_recv");
      if (sub.ok() && pub.ok()) {
        std::this_thread::sleep_for(300ms);
        flow::Scope recv_scope = rt->root_scope();
        auto consumed = rt->spawn(recv_scope, consume_two(sub.value()));
        if (consumed.ok()) {
          for (int i = 0; i < 2; i++) {
            std::this_thread::sleep_for(50ms);
            const std::string payload = "sample";
            (void)pub.value()->PublishBytes(
                reinterpret_cast<const uint8_t*>(payload.data()),
                payload.size());
          }
          (void)consumed.wait_for(5s);
        }
        // The parked recv is cancel-safe by default (context scope):
        // shutdown() resolves it even if the samples never arrived.
        (void)recv_scope.shutdown();
        if (auto n = consumed.take_outcome(); n.has_value() && n->ok()) {
          std::printf("[flow] recv leg: %d samples consumed\n", **n);
        } else {
          std::printf("[flow] recv leg cancelled at shutdown\n");
        }
      }

      // Readiness leg: bound subscribers → WindowSync → co_await aligned.
      auto ws = flow::WindowSync::create(2, 1s);
      auto cam_sub = node.value()->CreatePullSubscriber("demo/flow_cam", 1);
      auto det_sub = node.value()->CreatePullSubscriber("demo/flow_det", 1);
      auto cam_pub = node.value()->CreatePublisher("demo/flow_cam");
      auto det_pub = node.value()->CreatePublisher("demo/flow_det");
      if (ws.ok() && cam_sub.ok() && det_sub.ok() && cam_pub.ok() &&
          det_pub.ok() &&
          flow::bind_window(cam_sub.value(), *ws, 0) == AORTA_OK &&
          flow::bind_window(det_sub.value(), *ws, 1) == AORTA_OK) {
        flow::Scope fuse_scope = rt->root_scope();
        auto spread = rt->spawn(fuse_scope, fuse_once(*ws));
        if (spread.ok()) {
          const uint8_t frame[] = {1};
          auto d3 = std::chrono::steady_clock::now() + 10s;
          while (!spread.is_finished() &&
                 std::chrono::steady_clock::now() < d3) {
            (void)cam_pub.value()->PublishBytes(frame, 1);
            (void)det_pub.value()->PublishBytes(frame, 1);
            std::this_thread::sleep_for(10ms);
          }
        }
        (void)fuse_scope.shutdown();
        if (auto us = spread.take_outcome(); us.has_value() && us->ok()) {
          std::printf(
              "[flow] aligned(): 2 bound inputs within %llu us — no glue, "
              "no poll\n",
              static_cast<unsigned long long>(**us));
        } else {
          std::printf("[flow] aligned leg cancelled at shutdown\n");
        }
      }
    } else {
      std::printf("[flow] recv() leg skipped (no zenoh session)\n");
    }
  }

  // Teardown: the Runtime destructor — stop, final drain, detach,
  // destroy. Nothing to order by hand.
  std::printf("[flow] demo complete\n");
  return 0;
}
