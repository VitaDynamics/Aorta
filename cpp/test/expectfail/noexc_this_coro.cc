// EXPECTED: rejected via the D1 choke gate (graduated from a former RED
// gap — see git history for the pre-D1 "EXPECTED TODAY: compiles" text).
//
// Covers: `this_coro::context` and every `this_coro::*` reader, which
// previously compiled under -fno-exceptions without being gated.
//
// `this_coro::detail_tags::reader_awaiter` itself still has no
// `#if defined(__cpp_exceptions)` guard (it never throws) — but that no
// longer matters: `probe_this_coro` below is a `flow::Task<void>`
// coroutine, so D1's CHOKE POINT — `Task<T,K>::promise_type`
// static_asserting on instantiation under -fno-exceptions — fires on
// probe_this_coro's OWN promise_type before its body (the
// `co_await flow::this_coro::context`) is ever reached. The contract is
// not "this path happens to need exceptions", it is "Flow is not part of
// the -fno-exceptions consumer's surface, full stop" — a reader that
// never throws is still rejected, because the ENCLOSING flow coroutine
// can never be defined there at all. The rejection carries the teaching
// text "C++ Flow requires exception support" (run_expectfail.sh's
// needle).
#if defined(__cpp_exceptions)
#error "this TU must be compiled with -fno-exceptions (see run_expectfail.sh Set B)"
#endif

#include <aorta/flow.hpp>

namespace flow = aorta::flow;

flow::Task<void> probe_this_coro() {
  auto ctx = co_await flow::this_coro::context;  // rejected: the choke
                                                  // gate fires on this
                                                  // coroutine's own
                                                  // promise_type first
  (void)ctx;
}
