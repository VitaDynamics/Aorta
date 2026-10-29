// D1 (the choke-gate completion sweep) — the
// Flow surface under -fno-exceptions.
//
// This TU is the ONE in-tree translation unit compiled with
// -fno-exceptions. Under D1, C++ Flow (aorta/flow*.hpp) is NOT on a
// -fno-exceptions consumer's surface AT ALL: every await surface —
// default AND `.nothrow()` alike — is now a COMPILE-TIME rejection
// there. Task's promise_type static_asserts on instantiation (the choke
// gate: defining or awaiting any flow::Task/EndpointTask coroutine), and
// every nothrow_awaiter plus the explicit `sleep()`'s historical
// "silent status return" carry their own dependent static_assert (the
// belt: catches foreign, non-flow frames that never instantiate the
// choke-gated promise). There is therefore no more GREEN Flow-await
// runtime coverage to pin here — see the earlier revision of this file
// (git history) for the pre-D1 all-.nothrow() coverage this replaced.
//
// This file now pins two narrower, still-real contracts instead:
//
//   1. HEADER INCLUSION stays legal: every flow*.hpp header (including
//      flow_action.hpp — action.h's try/catch is conditional) must compile cleanly
//      in a -fno-exceptions TU as long as nothing in it is INSTANTIATED.
//      A -fno-exceptions consumer that pulls these headers in
//      transitively (e.g. via a shared internal header) must not have
//      that inclusion alone fail, even though every actual Flow await —
//      or Flow coroutine definition — is rejected the moment it is used.
//
//   2. The C++17 BASELINE SDK (Status/StatusOr, the plain C-ABI
//      executor lifecycle — NOT Flow) keeps working under
//      -fno-exceptions exactly as it always has; -fno-exceptions
//      consumers are pointed at this surface (callbacks/executors/pull
//      subscribers), not Flow.
//
// Real negative coverage for "every Flow await surface is rejected"
// lives in cpp/test/expectfail/ (run_expectfail.sh Set B: each
// noexc_*.cc TU is compiled and asserted to FAIL with the choke-gate
// teaching text, "C++ Flow requires exception support"). This file does
// NOT duplicate that RED coverage — dead compile-fail probe code (kept
// commented out) was deleted once real negative TUs existed to replace
// it.
#if defined(__cpp_exceptions)
#error "this test must be compiled with -fno-exceptions (see BUILD.bazel)"
#endif

#include <cstdint>
#include <cstdio>
#include <thread>

// Contract 1 — header-inclusion gate: every flow*.hpp header must be
// #include-able here without instantiating any Flow coroutine.
// flow_action.hpp is included too: action.h's inline
// try/catch dispatchers are now conditionally compiled (#if
// defined(__cpp_exceptions), #else running the same body directly), so
// the action-verb TYPES compile clean; nothing here constructs a
// send_goal<Action>/wait_result<Action>/cancel_goal<Action> — those are
// class TEMPLATES, never instantiated by mere inclusion.
#include "aorta/flow.hpp"
#include "aorta/flow_action.hpp"
#include "aorta/flow_combinators.hpp"
#include "aorta/flow_readiness.hpp"
#include "aorta/flow_subscriber.hpp"

// Contract 2 — baseline C++17 SDK smoke: Status/StatusOr and the plain
// C-ABI executor lifecycle. None of this is Flow.
#include "aorta/status.h"

namespace {

int g_failures = 0;

void check(bool ok, const char* what) {
  if (!ok) {
    std::fprintf(stderr, "FAILED: %s\n", what);
    ++g_failures;
  }
}

// Status/StatusOr — the baseline SDK's value-or-error vocabulary, used
// throughout the C++17 surface (client.h, action.h, ...) independent of
// Flow. FatalStatusOrAccess (the wrong-side-access trap) is std::abort(),
// not throw, so this type is fully usable under -fno-exceptions.
void status_or_smoke() {
  aorta::Status ok_status = aorta::Status::Ok();
  check(ok_status.ok(), "Status::Ok().ok()");

  aorta::Status err_status(AORTA_ERR_INVALID_ARGUMENT, "boom");
  check(!err_status.ok(), "error Status is not ok()");
  check(err_status.code() == AORTA_ERR_INVALID_ARGUMENT, "Status::code()");
  check(err_status.message() == "boom", "Status::message()");

  aorta::StatusOr<int> good(42);
  check(good.ok(), "StatusOr(value).ok()");
  check(good.value() == 42, "StatusOr::value()");
  check(*good == 42, "StatusOr::operator*()");

  aorta::StatusOr<int> bad(aorta::Status(AORTA_ERR_NOT_FOUND, "missing"));
  check(!bad.ok(), "StatusOr(Status).ok() is false");
  check(bad.value_or(-1) == -1, "StatusOr::value_or() on error");
}

}  // namespace

int main() {
  status_or_smoke();

  // Plain C-ABI executor lifecycle (not Flow — no coroutine ever touches
  // this handle in this TU; a live executor is still meaningful to smoke
  // under -fno-exceptions since the C-ABI itself carries no exception
  // dependency).
  uint64_t exec = 0;
  check(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec) == AORTA_OK,
        "executor_new");
  std::thread spin([exec] { aorta_executor_spin(exec); });
  check(aorta_executor_stop(exec) == AORTA_OK, "executor_stop");
  spin.join();
  check(aorta_executor_free(exec) == AORTA_OK, "executor_free");

  if (g_failures == 0) {
    std::printf("flow_noexcept_test: OK\n");
    return 0;
  }
  std::fprintf(stderr, "flow_noexcept_test: %d failure(s)\n", g_failures);
  return 1;
}
