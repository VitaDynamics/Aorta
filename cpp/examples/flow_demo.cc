// Aorta Flow demo — the C++20 coroutine surface end-to-end, composed with
// the C++ SDK's OWN objects (aorta::Node / aorta::Client) on the ONE setup
// object (flow::Runtime owns executor + lanes + facade + spin
// thread; its destructor IS the ordered teardown):
//   1. Resume path: coroutines hop onto their own plane via the ambient
//      co_await flow::resume() (the plane comes from the promise
//      context stamped at spawn; resume_on{exec} remains the explicit
//      cross-executor form).
//   2. Structured scopes: budget, RT-safe cancel flag, close() waits.
//      Coroutine bodies read the ambient scope via this_coro::scope —
//      zero scope threading through signatures.
//   3. Awaitable RPC: co_await flow::call(client, payload) — scope and
//      executor from the frame's context; the reply resumes the coroutine
//      on the frame's plane; a cancelled scope refuses calls.
//   4. The latest-value ring via the flow::Watch sugar.
//   5. Readiness binds: subscribers feed a WindowSync on the RX path —
//      the consumer reads aligned sets with zero glue.
//
// Build/run: bazel run //cpp/examples:flow_demo

#include <aorta/aorta.h>
#include <aorta/flow.hpp>
#include <aorta/flow_readiness.hpp>
#include <aorta/flow_runtime.hpp>
#include <aorta/flow_watch.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace flow = aorta::flow;

namespace {

// Coroutine parameters, not captures (see the flow.hpp header warning).
// The coroutine BODY threads no executor/scope handles — the
// promise context (stamped by spawn_on, where the context is ESTABLISHED)
// carries {plane, scope}; the ambient verbs and this_coro read it.
flow::Task<void> demo_request(std::shared_ptr<aorta::Client> client,
                              std::atomic<int>& echoes) {
  if (co_await flow::resume() != AORTA_OK) {
    std::printf("  [coro]  executor gone before start; unwinding\n");
    co_return;
  }
  for (int i = 0; i < 3; ++i) {
    if ((co_await flow::this_coro::scope).is_cancelled()) {
      std::printf("  [coro]  scope cancelled; unwinding cooperatively\n");
      co_return;
    }
    std::string payload = "ping-" + std::to_string(i);
    // The ambient awaitable: your existing Client object and a std::span
    // payload — scope and executor come from the frame's context.
    auto reply = co_await flow::call(
        *client,
        std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));
    if (!reply.ok()) {
      // reply.stopped() carries the exact reason: a fault has
      // code()+message(), a cancel has cancel_reason()/raw_reason() — no
      // ToStatus() flattening (that would re-lose the reason vocabulary).
      const auto& stopped = reply.stopped();
      std::printf(
          "  [coro]  call refused (%s); unwinding\n",
          stopped.is_fault() ? stopped.fault().message().c_str() : "cancelled");
      co_return;
    }
    std::printf("  [rpc]   echo #%d: %.*s\n", i,
                static_cast<int>(reply->size()),
                reinterpret_cast<const char*>(reply->data()));
    echoes.fetch_add(1, std::memory_order_seq_cst);
  }
  co_return;
}

}  // namespace

int main() {
  std::printf("=== Aorta Flow demo (C++20 coroutines) ===\n");

  // SDK objects only: Node + loopback echo service + Client.
  auto node = aorta::Must(aorta::Node::Create("cpp_flow_demo", "default"),
                          "create node");
  auto service = aorta::Must(
      node->CreateService(
          "cpp_flow_demo_echo",
          [](const aorta::MessageView& view, aorta::ServiceRequest&& req) {
            std::vector<uint8_t> payload = view.ToVector();
            (void)std::move(req).Reply(std::move(payload));
          }),
      "create echo service");
  (void)service;

  // The WHOLE Flow wiring is one object: executor, lane runtime,
  // facade attachment and the spin thread — created here, torn down (in
  // the right order, always) by the destructor at the end of main.
  // Node handles ride the FACTORY (dispatch wiring happens before the
  // spin thread starts — the old post-creation add_node was a startup
  // race against the spin thread's dispatch lock).
  auto rt = flow::Runtime::FromEnv({node->handle()});
  if (!rt.ok()) {
    std::fprintf(stderr, "flow runtime init failed\n");
    return 1;
  }

  auto client = aorta::Must(node->CreateClient("cpp_flow_demo_echo"),
                            "create client");
  std::this_thread::sleep_for(std::chrono::milliseconds(300));  // discovery

  // 1+3: a request scope with a budget; the coroutine calls the service.
  flow::Scope root = flow::Scope::root({.name = "cpp-demo-root"});
  flow::Scope request = root.child(
      {.budget = std::chrono::seconds(5), .name = "cpp-demo-request"});
  std::printf("  [scope] budget remaining: %lld ms\n",
              static_cast<long long>(
                  request.remaining().value_or(std::chrono::nanoseconds(0))
                      .count() /
                  1'000'000));

  std::atomic<int> echoes{0};
  auto spawned = rt->spawn(request, demo_request(client, echoes));
  if (!spawned.ok()) {
    std::fprintf(stderr, "spawn failed\n");
    return 1;
  }

  // 2: a cancellable sibling, stopped via the RT-safe flag — spawned with
  // spawn_with: the CAPTURING lambda is safe (co_invoke owns the closure).
  // The BODY threads nothing: the cancel poll reads this_coro::scope and
  // the periodic delay is the ambient flow::sleep (the arbiter timer on
  // this runtime-plane frame).
  flow::Scope worker_scope = root.child({.name = "cpp-demo-worker"});
  std::atomic<bool> worker_observed{false};
  auto worker = rt->spawn_with(
      worker_scope,
      [&worker_observed](flow::Scope) -> flow::Task<void> {
        // Periodic poll every 10ms via ambient sleep — NOT a resume busy-
        // spin (executor teardown unwinds the frame at the await).
        while (!(co_await flow::this_coro::scope).is_cancelled()) {
          co_await flow::sleep(std::chrono::milliseconds(10));
        }
        worker_observed.store(true, std::memory_order_seq_cst);
        co_return;
      });
  if (!worker.ok()) {
    std::fprintf(stderr, "worker spawn failed\n");
    return 1;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  if (!worker_scope.shutdown().ok()) {  // = cancel + close, one call
    std::fprintf(stderr, "worker shutdown failed\n");
    return 1;
  }
  std::printf("  [scope] worker observed cancel via the RT-safe flag: %s\n",
              worker_observed.load(std::memory_order_seq_cst) ? "yes" : "no");

  // Wait for the request coroutine (close() waits for the scope's tasks).
  if (!request.close().ok()) {
    std::fprintf(stderr, "request close failed\n");
    return 1;
  }
  std::printf("  [rpc]   %d echoes completed inside the scope\n",
              echoes.load(std::memory_order_seq_cst));

  // A cancelled scope refuses new work immediately (checkpoint
  // short-circuit) — cancel()/close() stay PAIRED even on the childless
  // path (lifecycle hygiene).
  flow::Scope doomed = root.child({});
  doomed.cancel(flow::CancelReason::kExplicit);
  std::printf("  [rpc]   cancelled-scope checkpoint: %s\n",
              doomed.checkpoint().code() == AORTA_ERR_CANCELLED ? "refused (correct)"
                                                         : "UNEXPECTED");
  (void)doomed.close();

  // 4: the latest-value ring — foreign-producer store + load with the
  // snapshot-paired sequence (no executor, no callback).
  auto watch = flow::Watch::create(node->handle(), "cpp_flow_demo_state");
  if (!watch.ok()) {
    std::fprintf(stderr, "watch create failed\n");
    return 1;
  }
  const uint8_t battery[] = {88};
  (void)watch->store(battery, sizeof(battery));
  if (auto v = watch->load()) {
    std::printf("  [ring]  latest value: len=%zu value=%u seq=%llu\n",
                v->payload.size(), v->payload.empty() ? 0 : v->payload[0],
                static_cast<unsigned long long>(v->seq));
  }

  // 5: readiness binds — two subscribers DECLARE their WindowSync inputs
  // (bind_window: the RX wire feeds the window; no hand-written
  // glue). The VLN person-follow shape: N sensor streams, act on "one
  // fresh sample of each". The awaited form (`co_await aligned`) lives in
  // flow_lanes_demo — this executor runs the queue path, so we poll.
  auto ws = flow::WindowSync::create(2, std::chrono::seconds(1));
  auto cam_sub = node->CreatePullSubscriber("cpp_flow_demo_cam", 1);
  auto det_sub = node->CreatePullSubscriber("cpp_flow_demo_det", 1);
  auto cam_pub = node->CreatePublisher("cpp_flow_demo_cam");
  auto det_pub = node->CreatePublisher("cpp_flow_demo_det");
  if (ws.ok() && cam_sub.ok() && det_sub.ok() && cam_pub.ok() &&
      det_pub.ok() &&
      flow::bind_window(cam_sub.value(), *ws, 0) == AORTA_OK &&
      flow::bind_window(det_sub.value(), *ws, 1) == AORTA_OK) {
    const uint8_t frame[] = {1};
    std::optional<flow::AlignedSet> set;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      (void)cam_pub.value()->PublishBytes(frame, 1);
      (void)det_pub.value()->PublishBytes(frame, 1);
      set = ws->try_aligned();
      if (set.has_value()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (set.has_value()) {
      const auto s0 = set->at(0);
      const auto s1 = set->at(1);
      const uint64_t spread =
          s0.ts_ns > s1.ts_ns ? s0.ts_ns - s1.ts_ns : s1.ts_ns - s0.ts_ns;
      std::printf(
          "  [sync]  aligned 2 bound inputs within %lluus — no feed glue\n",
          static_cast<unsigned long long>(spread / 1000));
    } else {
      std::fprintf(stderr, "readiness leg never aligned\n");
      return 1;
    }
  } else {
    std::fprintf(stderr, "readiness leg setup failed\n");
    return 1;
  }

  // Teardown: root shutdown (cancel + close); the Runtime destructor then
  // runs the ordered stop → drain → detach → destroy by itself.
  (void)root.shutdown();
  std::printf("=== done ===\n");
  return 0;
}
