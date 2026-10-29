// flow::SerialDomain demo — the
// three-sentence model, live:
//
//   1. Tasks bound to one domain never run concurrently with each other
//      (as long as they suspend only through Flow's own awaitables).
//   2. co_await — including join — keeps you in the domain; explicit
//      spawn + when_all is how you go parallel.
//   3. A bound frame lives on its domain's runtime; cross-plane arms
//      are refused loudly.
//
// Four legs:
//   (a) two bound tasks hammer an UNLOCKED shared counter under MT —
//       consistency asserted (the callback-group successor, no mutex);
//   (b) join inheritance (serialized) vs explicit spawn + when_all
//       (parallel), with a timing printout showing the difference;
//   (c) offload out-and-back — the closure runs OFF the domain, the
//       continuation re-enters it;
//   (d) the AORTA_CPP_LANES=0 rollback leg — the same binary re-run
//       with the env var set exercises the no-op domain.
//
// Build/run: bazel run //cpp/examples:flow_serial_demo
//            AORTA_CPP_LANES=0 bazel run //cpp/examples:flow_serial_demo
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "aorta/flow_combinators.hpp"
#include "aorta/flow_runtime.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

uint64_t now_us() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// --- leg (a): the unlocked counter -----------------------------------------

struct Shared {
  uint64_t counter = 0;               // NO lock — the domain serializes
  std::atomic<bool> busy{false};      // overlap detector (proof, not lock)
  std::atomic<uint32_t> overlaps{0};
};

flow::Task<void> hammer(Shared* sh, int iters) {
  for (int i = 0; i < iters; ++i) {
    if (sh->busy.exchange(true)) {
      sh->overlaps.fetch_add(1);
    }
    sh->counter++;  // plain increment: rule 1 above makes this safe
    sh->busy.store(false);
    co_await flow::resume();  // an external re-entry between segments
  }
  co_return;
}

// --- leg (b): join (inherits, serialized) vs spawn+when_all (parallel) -----

flow::Task<int> busy_ms(int ms, int v) {
  // Simulated compute: hold the strand (or a parallel worker) busy.
  const auto until = std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(ms);
  while (std::chrono::steady_clock::now() < until) {
  }
  co_return v;
}

flow::Task<uint64_t> join_leg(bool* branches_ok) {
  const uint64_t t0 = now_us();
  // Branches INHERIT the domain: correct by default, serialized — the
  // two 30ms computes run back to back (~60ms).
  auto [a, b] = co_await flow::join(busy_ms(30, 1), busy_ms(30, 2));
  *branches_ok = (a == 1 && b == 2);
  co_return now_us() - t0;
}

flow::Task<uint64_t> when_all_leg(const flow::Runtime* rt,
                                  flow::Scope scope, bool* branches_ok) {
  const uint64_t t0 = now_us();
  // The PARALLEL spelling: explicit UNBOUND spawns + when_all — visible
  // at the call site; the two computes overlap on the MT workers
  // (~30ms wall).
  std::vector<flow::TaskHandle<int>> hs;
  hs.push_back(rt->spawn(scope, busy_ms(30, 1)));
  hs.push_back(rt->spawn(scope, busy_ms(30, 2)));
  for (auto& h : hs) {
    if (!h.ok()) {
      *branches_ok = false;
      co_return 0;
    }
  }
  auto all = co_await flow::when_all(std::move(hs));
  *branches_ok =
      all.ok() && all->size() == 2 && (*all)[0] == 1 && (*all)[1] == 2;
  co_return now_us() - t0;
}

// --- leg (c): offload out-and-back -----------------------------------------

flow::Task<void> offload_leg(const void* group, bool* closure_off,
                             bool* continuation_in, int* value_out) {
  const void* closure_mark = group;  // sentinel: must be OVERWRITTEN
  int v = co_await flow::offload(aorta::EventPriority::kLow,
                                 [&closure_mark] {
                                   closure_mark =
                                       aorta::lanes::g_serial_slice_group;
                                   return 42;
                                 });
  *value_out = v;  // asserted on BOTH legs — the offload must complete
  *closure_off = (closure_mark != group);  // ran OUTSIDE the domain
  *continuation_in =
      (aorta::lanes::g_serial_slice_group == group) && v == 42;
  co_return;
}

}  // namespace

int main() {
  const char* lanes_env = std::getenv("AORTA_CPP_LANES");
  const bool rollback = lanes_env != nullptr && lanes_env[0] == '0';
  std::printf("flow::SerialDomain demo (%s)\n",
              rollback ? "AORTA_CPP_LANES=0 rollback: no-op domain — the "
                         "single consumer already serializes"
                       : "MT lanes: 2 Normal workers + 1 Low worker");

  auto rt_or = flow::Runtime::MultiThread({0, 0, 2, 1});
  if (!rt_or.ok()) {
    std::fprintf(stderr, "Runtime creation failed\n");
    return 1;
  }
  auto rt = std::move(*rt_or);
  auto d = rt.create_serial(aorta::EventPriority::kNormal, "demo");
  if (!d.valid()) {
    std::fprintf(stderr, "create_serial failed\n");
    return 1;
  }
  // Mode verification: the env string alone
  // is not proof — assert the runtime actually took the claimed shape.
  if (rt.lanes().enabled() == rollback) {
    std::fprintf(stderr, "lanes mode does not match AORTA_CPP_LANES\n");
    return 1;
  }
  // The rollback domain is a NO-OP (null group — the single
  // consumer already serializes); the MT domain owns a real strand.
  const bool has_group =
      flow::detail::serial_access::state(d)->group != nullptr;
  if (has_group == rollback) {
    std::fprintf(stderr, "domain shape does not match the lanes mode\n");
    return 1;
  }
  flow::Scope scope = rt.root_scope();

  // (a) The unlocked counter: 4 bound tasks x 500 increments, no mutex.
  {
    Shared sh;
    std::vector<flow::TaskHandle<void>> hs;
    for (int i = 0; i < 4; ++i) {
      hs.push_back(rt.spawn(scope, d, hammer(&sh, 500)));
    }
    for (auto& h : hs) {
      if (!h.ok() || !h.wait_for(30s)) {
        std::fprintf(stderr, "(a) hammer failed\n");
        return 1;
      }
    }
    std::printf("(a) unlocked counter: %llu (expected 2000), overlaps: %u\n",
                static_cast<unsigned long long>(sh.counter),
                sh.overlaps.load());
    if (sh.counter != 2000 || sh.overlaps.load() != 0) {
      std::fprintf(stderr, "(a) CONSISTENCY VIOLATION\n");
      return 1;
    }
  }

  // (b) join (inherits → serialized) vs spawn + when_all (parallel).
  {
    bool join_branches_ok = false;
    bool when_all_branches_ok = false;
    auto hj = rt.spawn(scope, d, join_leg(&join_branches_ok));
    if (!hj.ok() || !hj.wait_for(30s)) {
      std::fprintf(stderr, "(b) join leg failed\n");
      return 1;
    }
    auto oj = hj.take_outcome();
    auto hw =
        rt.spawn(scope, d, when_all_leg(&rt, scope, &when_all_branches_ok));
    if (!hw.ok() || !hw.wait_for(30s)) {
      std::fprintf(stderr, "(b) when_all leg failed\n");
      return 1;
    }
    auto ow = hw.take_outcome();
    if (!oj.has_value() || !oj->ok() || !ow.has_value() || !ow->ok()) {
      std::fprintf(stderr, "(b) legs did not deliver\n");
      return 1;
    }
    if (!join_branches_ok || !when_all_branches_ok) {
      std::fprintf(stderr, "(b) branch results wrong or refused\n");
      return 1;
    }
    const uint64_t join_us = **oj;
    const uint64_t all_us = **ow;
    std::printf(
        "(b) join (inherited, serialized): %llu us; spawn+when_all "
        "(parallel): %llu us%s\n",
        static_cast<unsigned long long>(join_us),
        static_cast<unsigned long long>(all_us),
        rollback ? "  (rollback: both serialized on the single consumer)"
                 : "  (the parallel spelling overlaps the computes)");
    // Timing RELATION asserts: two 30ms computes — serialized
    // means >= ~60ms, overlapped means ~30ms. Margins are generous to
    // stay CI-safe while still distinguishing the two shapes.
    if (join_us < 55000) {
      std::fprintf(stderr, "(b) join branches were NOT serialized\n");
      return 1;
    }
    if (rollback) {
      if (all_us < 55000) {
        std::fprintf(stderr,
                     "(b) rollback when_all was NOT serialized\n");
        return 1;
      }
    } else {
      if (all_us * 10 > join_us * 8) {  // parallel must be < 0.8x join
        std::fprintf(stderr,
                     "(b) spawn+when_all did NOT overlap the computes\n");
        return 1;
      }
    }
  }

  // (c) offload out-and-back.
  {
    const void* group =
        flow::detail::serial_access::state(d)->group.get();
    bool closure_off = false;
    bool continuation_in = false;
    int value = 0;
    auto h = rt.spawn(scope, d, offload_leg(group, &closure_off,
                                            &continuation_in, &value));
    if (!h.ok() || !h.wait_for(30s)) {
      std::fprintf(stderr, "(c) offload leg failed\n");
      return 1;
    }
    auto oc = h.take_outcome();
    if (!oc.has_value() || !oc->ok() || value != 42) {
      std::fprintf(stderr, "(c) offload task did not deliver 42\n");
      return 1;
    }
    if (rollback) {
      // No strand exists on the rollback leg — the slice-mark probes
      // are vacuous (nullptr == nullptr); the offload round-trip and
      // its value are still asserted above.
      std::printf("(c) offload: completed on the rollback leg, value 42 "
                  "(no strand — the slice-mark probes do not apply)\n");
    } else {
      std::printf("(c) offload: closure ran %s the domain; continuation "
                  "re-entered: %s\n",
                  closure_off ? "OUTSIDE" : "INSIDE(!)",
                  continuation_in ? "yes" : "NO(!)");
      if (!closure_off || !continuation_in) {
        std::fprintf(stderr, "(c) OFFLOAD CONTRACT VIOLATION\n");
        return 1;
      }
    }
  }

  if (!scope.close().ok()) {
    std::fprintf(stderr, "scope close failed\n");
    return 1;
  }
  std::printf("done.\n");
  return 0;
}
